"""
Postmortem Agent — generates structured postmortems from incident timeline.

Receives: incidents.resolved
Produces: postmortems.generated
"""
from __future__ import annotations
import logging
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, "/app")
from shared.models import IncidentResolvedEvent, PostmortemGeneratedEvent, now_ms
from shared.kafka_client import make_producer, make_consumer, publish, consume_loop
from shared.db_client import init_pool, update_incident, log_agent_event, fetch_one, fetch_all
from shared.llm_client import chat, SONNET

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("postmortem-agent")

producer = None

POSTMORTEM_SYSTEM = """You are an expert SRE writing a production incident postmortem.
Write a comprehensive, factual postmortem document in Markdown format.

The postmortem should follow this structure:
1. # Postmortem: [descriptive title]
2. ## Summary (2-3 sentences, non-technical, impact-focused)
3. ## Timeline (bullet list with timestamps, factual)
4. ## Root Cause (clear technical explanation)
5. ## Contributing Factors (systemic issues that allowed this to happen)
6. ## Impact (users affected, revenue impact estimate, SLA breach)
7. ## What Went Well (positives from the incident response)
8. ## What Could Be Improved (process/technical gaps)
9. ## Action Items (table: | Action | Owner | Priority | Due Date |)

Keep it factual, blameless, and actionable. Focus on systemic improvements, not individual blame.
Write in a professional tone suitable for sharing with engineering leadership."""


def build_timeline_text(incident_id: str, detection_time: int, resolved_at: int) -> str:
    """Build timeline from agent_events in Postgres."""
    try:
        events = fetch_all(
            "SELECT agent_name, event_type, payload, created_at FROM agent_events "
            "WHERE incident_id = %s ORDER BY created_at ASC",
            (incident_id,),
        )
        if not events:
            return "Timeline data not available."

        lines = []
        for ev in events:
            ts = ev["created_at"].strftime("%H:%M:%S UTC") if ev["created_at"] else "?"
            lines.append(f"- {ts}: [{ev['agent_name']}] {ev['event_type']}")
        return "\n".join(lines)
    except Exception as exc:
        return f"Timeline retrieval error: {exc}"


def generate_postmortem(resolved: IncidentResolvedEvent) -> str:
    incident_id = resolved.incident_id

    # Fetch full incident data
    incident = fetch_one("SELECT * FROM incidents WHERE id = %s", (incident_id,))
    if not incident:
        logger.warning("Incident %s not found in DB", incident_id)
        incident = {}

    timeline_text = build_timeline_text(incident_id, resolved.detection_time, resolved.resolved_at)

    detection_dt  = datetime.fromtimestamp(resolved.detection_time / 1000, tz=timezone.utc)
    resolution_dt = datetime.fromtimestamp(resolved.resolved_at / 1000, tz=timezone.utc)

    rca_candidates = incident.get("rca_candidates") or []
    top_cause      = incident.get("top_root_cause") or resolved.top_root_cause
    blast_radius   = incident.get("blast_radius") or {}
    remediation    = incident.get("remediation_plan") or {}

    steps_text = ""
    if remediation.get("action_steps"):
        steps_text = "\n".join(
            f"- Step {s.get('step_id',i+1)}: {s.get('action','')}"
            for i, s in enumerate(remediation["action_steps"][:6])
        )

    user_prompt = f"""## Incident Data

**Incident ID:** INC-{incident_id[:8].upper()}
**Severity:** {resolved.severity}
**Anomaly Type:** {resolved.anomaly_type.replace('_',' ')}
**Affected Services:** {', '.join(resolved.affected_services)}
**Detection Time:** {detection_dt.strftime('%Y-%m-%d %H:%M:%S UTC')}
**Resolution Time:** {resolution_dt.strftime('%Y-%m-%d %H:%M:%S UTC')}
**MTTR:** {resolved.mttr_minutes:.0f} minutes
**Resolution Method:** {resolved.resolution_method.replace('_',' ')}

## Root Cause

{top_cause}

**RCA Confidence:** {incident.get('rca_confidence', 0):.0%}

## Impact

User Impact: {blast_radius.get('estimated_user_impact', 'Unknown')}
Services Affected: {', '.join(blast_radius.get('affected_services', resolved.affected_services))}

## Agent Timeline

{timeline_text}

## Remediation Steps Applied

{steps_text or 'Automatic recovery — metrics normalized without manual intervention'}

Please write a comprehensive postmortem document following the standard structure.
Use the data above to populate specific details. For action items, propose 3-5 preventative measures with realistic due dates (use relative dates like '2 weeks', '1 month').
"""

    try:
        postmortem_text = chat(
            system=POSTMORTEM_SYSTEM,
            user=user_prompt,
            model=SONNET,
            max_tokens=3000,
            temperature=0.2,
        )
        return postmortem_text
    except Exception as exc:
        logger.exception("Postmortem generation failed: %s", exc)
        return f"""# Postmortem: {resolved.anomaly_type.replace('_',' ')} on {', '.join(resolved.affected_services)}

## Summary
A {resolved.severity} severity {resolved.anomaly_type.replace('_',' ')} incident occurred on {', '.join(resolved.affected_services)}.
The incident was resolved in {resolved.mttr_minutes:.0f} minutes.

## Root Cause
{top_cause}

## Timeline
{timeline_text}

## Action Items
| Action | Owner | Priority | Due Date |
|--------|-------|----------|----------|
| Review incident timeline | On-call team | P1 | 1 week |
| Add monitoring for similar pattern | SRE team | P2 | 2 weeks |

*Note: Full postmortem generation was unavailable. Please update this document manually.*
"""


def handle_resolved(raw: dict) -> None:
    try:
        resolved = IncidentResolvedEvent(**raw)
    except Exception as exc:
        logger.error("Invalid resolved event: %s", exc)
        return

    logger.info("Generating postmortem for incident %s", resolved.incident_id)

    postmortem_text = generate_postmortem(resolved)

    evt = PostmortemGeneratedEvent(
        incident_id=resolved.incident_id,
        postmortem=postmortem_text,
        mttr_minutes=resolved.mttr_minutes,
        severity=resolved.severity,
        anomaly_type=resolved.anomaly_type,
        affected_services=resolved.affected_services,
    )

    # Persist to Postgres
    try:
        update_incident(resolved.incident_id, {"postmortem": postmortem_text})
        log_agent_event(
            resolved.incident_id, "postmortem-agent", "POSTMORTEM_GENERATED",
            {"length_chars": len(postmortem_text)},
        )
    except Exception as exc:
        logger.error("DB update failed: %s", exc)

    publish(producer, "postmortems.generated", evt.model_dump())
    logger.info(
        "Published postmortem for %s (%d chars)",
        resolved.incident_id, len(postmortem_text),
    )


if __name__ == "__main__":
    logger.info("Postmortem Agent starting...")
    init_pool()
    producer = make_producer()
    consumer = make_consumer(topics=["incidents.resolved"], group_id="postmortem-agent")
    logger.info("Postmortem Agent ready.")
    consume_loop(consumer, handle_resolved)
