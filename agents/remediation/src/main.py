"""
Remediation Agent — generates concrete action plans grounded in runbooks.

Receives: rca.completed
Produces: remediation.plans
"""
from __future__ import annotations
import json
import logging
import os
import sys
import time

from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

sys.path.insert(0, "/app")
from shared.models import RCACompletedEvent, RemediationPlanEvent, RemediationStep, now_ms
from shared.kafka_client import make_producer, make_consumer, publish, consume_loop
from shared.db_client import init_pool, update_incident, log_agent_event
from shared.llm_client import chat, SONNET

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("remediation-agent")

QDRANT_HOST = os.getenv("QDRANT_HOST", "qdrant")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))

qdrant: QdrantClient | None = None
embedder: SentenceTransformer | None = None
producer = None


def init_qdrant(max_retries: int = 20) -> None:
    global qdrant
    for attempt in range(1, max_retries + 1):
        try:
            qdrant = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
            qdrant.get_collections()
            return
        except Exception as exc:
            logger.warning("Qdrant not ready (%d/%d): %s", attempt, max_retries, exc)
            time.sleep(3)
    raise RuntimeError("Qdrant never available.")


def fetch_runbook_content(anomaly_type: str, top_root_cause: str) -> str:
    try:
        query = f"{anomaly_type} {top_root_cause} remediation steps"
        vec = embedder.encode([query], show_progress_bar=False)[0].tolist()
        results = qdrant.search(
            collection_name="runbooks",
            query_vector=vec,
            limit=6,
            with_payload=True,
        )
        if not results:
            return "No specific runbook found."
        chunks = []
        for r in results[:4]:
            p = r.payload
            chunks.append(f"[{p.get('runbook_id')} — {p.get('section_title')}]\n{p.get('text','')[:600]}")
        return "\n\n".join(chunks)
    except Exception as exc:
        return f"Runbook retrieval error: {exc}"


def generate_plan_with_llm(rca: RCACompletedEvent, runbook_content: str) -> dict:
    system = (
        "You are a senior SRE generating an incident remediation plan. "
        "Based on the root cause analysis and relevant runbook content, produce a JSON response with:\n"
        "  'action_steps': array of step objects each with:\n"
        "    - 'step_id': integer starting at 1\n"
        "    - 'priority': 'IMMEDIATE' (do in <5min) | 'WITHIN_15MIN' | 'WITHIN_1HOUR'\n"
        "    - 'action': clear actionable instruction (what to do)\n"
        "    - 'rationale': why this step helps\n"
        "    - 'risk_level': 'LOW' | 'MEDIUM' | 'HIGH'\n"
        "    - 'rollback': how to undo this step if it makes things worse\n"
        "    - 'owner': team or role responsible (e.g., 'on-call engineer', 'database team')\n"
        "    - 'expected_outcome': what should happen after this step\n"
        "  'escalation_path': list of escalation contacts/teams if steps don't resolve\n"
        "  'estimated_resolution_time': e.g., '15-30 minutes'\n"
        "  'runbook_references': list of runbook names referenced\n"
        "Output ONLY valid JSON. Be specific and actionable — not generic advice."
    )

    top = rca.root_cause_candidates[0] if rca.root_cause_candidates else None
    top_confidence = top.confidence if top else rca.top_confidence
    top_hypothesis = top.hypothesis if top else rca.top_root_cause

    user = (
        f"## Incident Information\n"
        f"Incident ID: {rca.incident_id}\n"
        f"Anomaly Type: {rca.anomaly_type}\n"
        f"Severity: {rca.severity}\n"
        f"Affected Services: {', '.join(rca.affected_services)}\n"
        f"User Impact: {rca.blast_radius.get('estimated_user_impact','Unknown')}\n\n"
        f"## Root Cause Analysis\n"
        f"Top Root Cause ({top_confidence:.0%} confidence): {top_hypothesis}\n\n"
        f"Supporting evidence:\n" +
        "\n".join(f"  - {e}" for e in (top.evidence if top else [])) +
        f"\n\n## Relevant Runbook Content\n{runbook_content}\n\n"
        f"Generate a complete remediation plan with specific, actionable steps."
    )
    try:
        raw = chat(system=system, user=user, model=SONNET, max_tokens=2000, temperature=0.1)
        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw.strip())
    except Exception as exc:
        logger.error("LLM remediation generation failed: %s", exc)
        return {
            "action_steps": [{
                "step_id": 1,
                "priority": "IMMEDIATE",
                "action": f"Investigate {rca.anomaly_type} on {', '.join(rca.affected_services)}",
                "rationale": "Primary anomaly requires immediate attention",
                "risk_level": "LOW",
                "rollback": "No rollback needed for investigation",
                "owner": "on-call engineer",
                "expected_outcome": "Root cause confirmed or ruled out",
            }],
            "escalation_path": ["on-call lead", "service owner"],
            "estimated_resolution_time": "30-60 minutes",
            "runbook_references": [rca.anomaly_type.lower().replace("_", "-")],
        }


def handle_rca(raw: dict) -> None:
    try:
        rca = RCACompletedEvent(**raw)
    except Exception as exc:
        logger.error("Invalid RCA event: %s", exc)
        return

    logger.info("Generating remediation plan for incident %s", rca.incident_id)

    runbook_content = fetch_runbook_content(rca.anomaly_type, rca.top_root_cause)
    plan_data = generate_plan_with_llm(rca, runbook_content)

    steps = [
        RemediationStep(
            step_id=s.get("step_id", i + 1),
            priority=s.get("priority", "WITHIN_15MIN"),
            action=s.get("action", ""),
            rationale=s.get("rationale", ""),
            risk_level=s.get("risk_level", "LOW"),
            rollback=s.get("rollback", ""),
            owner=s.get("owner", "on-call engineer"),
            expected_outcome=s.get("expected_outcome", ""),
        )
        for i, s in enumerate(plan_data.get("action_steps", []))
    ]

    plan = RemediationPlanEvent(
        incident_id=rca.incident_id,
        root_cause=rca.top_root_cause,
        confidence=rca.top_confidence,
        action_steps=steps,
        escalation_path=plan_data.get("escalation_path", []),
        runbook_references=plan_data.get("runbook_references", []),
        estimated_resolution_time=plan_data.get("estimated_resolution_time", "Unknown"),
        anomaly_type=rca.anomaly_type,
        affected_services=rca.affected_services,
        severity=rca.severity,
    )

    try:
        update_incident(rca.incident_id, {
            "status": "REMEDIATING",
            "remediation_plan": plan_data,
        })
        log_agent_event(
            rca.incident_id, "remediation-agent", "PLAN_GENERATED",
            {"steps": len(steps), "estimated_time": plan.estimated_resolution_time},
        )
    except Exception as exc:
        logger.error("DB update failed: %s", exc)

    publish(producer, "remediation.plans", plan.model_dump())
    logger.info(
        "Published remediation plan for %s — %d steps, ETA: %s",
        rca.incident_id, len(steps), plan.estimated_resolution_time,
    )


if __name__ == "__main__":
    logger.info("Remediation Agent starting...")
    init_pool()
    init_qdrant()
    logger.info("Loading embedding model...")
    embedder = SentenceTransformer("all-MiniLM-L6-v2")
    logger.info("Embedder ready.")
    producer = make_producer()
    consumer = make_consumer(topics=["rca.completed"], group_id="remediation-agent")
    logger.info("Remediation Agent ready.")
    consume_loop(consumer, handle_rca)
