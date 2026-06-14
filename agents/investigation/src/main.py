"""
Investigation Agent — orchestrates root cause analysis using Claude tool use.

Receives: incidents.opened
Produces: rca.completed, incidents.resolved

Flow:
  1. Receive incident context from incidents.opened
  2. Run Claude claude-sonnet-4-6 with tools for evidence gathering
  3. Tools call Qdrant for similar incidents, runbooks, architecture
  4. Claude reasons over evidence and submits RCA
  5. Publish rca.completed
  6. Background thread monitors anomaly_score → publish incidents.resolved when metrics normalize
"""
from __future__ import annotations
import json
import logging
import os
import sys
import threading
import time

from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

sys.path.insert(0, "/app")
from shared.models import (
    IncidentOpenedEvent, RCACompletedEvent, RootCauseCandidate,
    IncidentResolvedEvent, now_ms
)
from shared.kafka_client import make_producer, make_consumer, publish, consume_loop
from shared.redis_client import get_client as get_redis, get_metric_history
from shared.db_client import init_pool, update_incident, log_agent_event, fetch_one
from shared.llm_client import run_tool_use_agent, SONNET

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("investigation-agent")

QDRANT_HOST = os.getenv("QDRANT_HOST", "qdrant")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))
RESOLUTION_INTERVAL = int(os.getenv("RESOLUTION_CHECK_INTERVAL_SECONDS", "30"))
NORMAL_READINGS_REQUIRED = int(os.getenv("RESOLUTION_CONSECUTIVE_NORMAL_READINGS", "3"))

qdrant: QdrantClient | None = None
embedder: SentenceTransformer | None = None
producer = None

# Track active incidents for resolution monitoring
active_incidents: dict[str, dict] = {}
active_incidents_lock = threading.Lock()


def init_qdrant(max_retries: int = 20) -> None:
    global qdrant
    for attempt in range(1, max_retries + 1):
        try:
            qdrant = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
            qdrant.get_collections()
            logger.info("Qdrant connected.")
            return
        except Exception as exc:
            logger.warning("Qdrant not ready (%d/%d): %s", attempt, max_retries, exc)
            time.sleep(3)
    raise RuntimeError("Qdrant never became available.")


def embed_text(text: str) -> list[float]:
    return embedder.encode([text], show_progress_bar=False)[0].tolist()


# ─── Knowledge retrieval tools ─────────────────────────────────────────────────

def search_similar_incidents(query: str, anomaly_type: str = "", top_k: int = 5) -> str:
    """Search Qdrant for similar historical incidents."""
    try:
        vec = embed_text(query)
        filter_cond = None
        if anomaly_type:
            from qdrant_client.http.models import Filter, FieldCondition, MatchValue
            filter_cond = Filter(
                must=[FieldCondition(key="anomaly_type", match=MatchValue(value=anomaly_type))]
            )

        results = qdrant.search(
            collection_name="incidents",
            query_vector=vec,
            query_filter=filter_cond,
            limit=top_k * 2,
            with_payload=True,
        )

        # Group by incident_id, keep best score
        seen: dict[str, tuple] = {}
        for r in results:
            iid = r.payload.get("incident_id", "")
            if iid not in seen or r.score > seen[iid][0]:
                seen[iid] = (r.score, r.payload)

        top = sorted(seen.values(), key=lambda x: x[0], reverse=True)[:top_k]

        if not top:
            return "No similar incidents found in knowledge base."

        lines = [f"Found {len(top)} similar historical incidents:\n"]
        for i, (score, payload) in enumerate(top, 1):
            lines.append(
                f"{i}. [{payload.get('incident_id')}] {payload.get('title','')} "
                f"(similarity={score:.2f}, MTTR={payload.get('mttr_minutes',0)}min)\n"
                f"   Root cause: {payload.get('text','')[:200]}\n"
            )
        return "\n".join(lines)
    except Exception as exc:
        return f"Error searching incidents: {exc}"


def get_runbooks(anomaly_type: str, service: str = "") -> str:
    """Retrieve relevant runbooks for the anomaly type."""
    try:
        query = f"{anomaly_type} {service} troubleshooting remediation steps"
        vec = embed_text(query)

        from qdrant_client.http.models import Filter, FieldCondition, MatchAny
        results = qdrant.search(
            collection_name="runbooks",
            query_vector=vec,
            limit=8,
            with_payload=True,
        )

        if not results:
            return f"No runbooks found for {anomaly_type}."

        lines = [f"Relevant runbook sections for {anomaly_type}:\n"]
        for r in results[:5]:
            payload = r.payload
            lines.append(
                f"[{payload.get('runbook_id','')} — {payload.get('section_title','')}] "
                f"(score={r.score:.2f})\n"
                f"{payload.get('text','')[:400]}\n"
            )
        return "\n".join(lines)
    except Exception as exc:
        return f"Error retrieving runbooks: {exc}"


def get_service_architecture(service_name: str) -> str:
    """Get service architecture, dependencies, and ownership info."""
    try:
        vec = embed_text(f"{service_name} service architecture dependencies")

        from qdrant_client.http.models import Filter, FieldCondition, MatchValue
        results = qdrant.search(
            collection_name="architecture",
            query_vector=vec,
            query_filter=Filter(must=[
                FieldCondition(key="service_name", match=MatchValue(value=service_name))
            ]),
            limit=1,
            with_payload=True,
        )

        if not results:
            # Fallback: search without filter
            results = qdrant.search(
                collection_name="architecture",
                query_vector=vec,
                limit=1,
                with_payload=True,
            )

        if not results:
            return f"No architecture documentation found for {service_name}."

        p = results[0].payload
        return (
            f"Service: {p.get('service_name')}\n"
            f"Description: {p.get('description','')}\n"
            f"Team: {p.get('team','')}\n"
            f"Criticality: {p.get('criticality','')}\n"
            f"SLA: {json.dumps(p.get('sla',{}))}\n"
            f"Depends on: {', '.join(p.get('dependencies_downstream',[]))}\n"
            f"Called by: {', '.join(p.get('dependencies_upstream',[]))}\n"
            f"Common failures: {', '.join(p.get('common_failure_modes',[]))}\n"
            f"On-call: {p.get('on_call','')}\n"
            f"Runbooks: {', '.join(p.get('runbooks',[]))}\n"
        )
    except Exception as exc:
        return f"Error fetching architecture for {service_name}: {exc}"


def submit_rca(
    top_hypothesis: str,
    confidence: float,
    evidence: list,
    all_candidates: list,
    estimated_user_impact: str = "",
) -> str:
    """Tool for Claude to submit the final RCA — this terminates the tool loop."""
    return json.dumps({
        "submitted": True,
        "top_hypothesis": top_hypothesis,
        "confidence": confidence,
        "evidence": evidence,
        "all_candidates": all_candidates,
        "estimated_user_impact": estimated_user_impact,
    })


TOOLS = [
    {
        "name": "search_similar_incidents",
        "description": "Search historical incident database for past incidents similar to the current one. Returns ranked similar incidents with root causes and resolution steps.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Description of the current incident symptoms and anomaly",
                },
                "anomaly_type": {
                    "type": "string",
                    "description": "Optional anomaly type filter (e.g., LATENCY_SPIKE, ERROR_RATE_SPIKE)",
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "get_runbooks",
        "description": "Retrieve relevant operational runbooks for the detected anomaly type. Returns step-by-step investigation and remediation procedures.",
        "input_schema": {
            "type": "object",
            "properties": {
                "anomaly_type": {
                    "type": "string",
                    "description": "The type of anomaly (e.g., LATENCY_SPIKE, DB_CONNECTION_EXHAUSTION)",
                },
                "service": {
                    "type": "string",
                    "description": "Optional service name for service-specific runbooks",
                },
            },
            "required": ["anomaly_type"],
        },
    },
    {
        "name": "get_service_architecture",
        "description": "Get architecture documentation, dependencies, SLA, and ownership for a service.",
        "input_schema": {
            "type": "object",
            "properties": {
                "service_name": {
                    "type": "string",
                    "description": "Name of the service (e.g., payment-service)",
                },
            },
            "required": ["service_name"],
        },
    },
    {
        "name": "submit_rca",
        "description": "Submit the completed root cause analysis. Call this when you have gathered sufficient evidence and reached a conclusion. This finalizes the investigation.",
        "input_schema": {
            "type": "object",
            "properties": {
                "top_hypothesis": {
                    "type": "string",
                    "description": "The most likely root cause, stated clearly and concisely",
                },
                "confidence": {
                    "type": "number",
                    "description": "Confidence level 0.0-1.0 for the top hypothesis",
                },
                "evidence": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of evidence items supporting the top hypothesis",
                },
                "all_candidates": {
                    "type": "array",
                    "description": "All root cause candidates ranked by confidence",
                    "items": {
                        "type": "object",
                        "properties": {
                            "rank": {"type": "integer"},
                            "hypothesis": {"type": "string"},
                            "confidence": {"type": "number"},
                            "evidence": {"type": "array", "items": {"type": "string"}},
                        },
                    },
                },
                "estimated_user_impact": {
                    "type": "string",
                    "description": "Brief description of the user-facing impact",
                },
            },
            "required": ["top_hypothesis", "confidence", "evidence", "all_candidates"],
        },
    },
]


def tool_executor(tool_name: str, tool_input: dict) -> str:
    if tool_name == "search_similar_incidents":
        return search_similar_incidents(
            tool_input.get("query", ""),
            tool_input.get("anomaly_type", ""),
        )
    elif tool_name == "get_runbooks":
        return get_runbooks(
            tool_input.get("anomaly_type", ""),
            tool_input.get("service", ""),
        )
    elif tool_name == "get_service_architecture":
        return get_service_architecture(tool_input.get("service_name", ""))
    elif tool_name == "submit_rca":
        return submit_rca(
            tool_input.get("top_hypothesis", ""),
            tool_input.get("confidence", 0.0),
            tool_input.get("evidence", []),
            tool_input.get("all_candidates", []),
            tool_input.get("estimated_user_impact", ""),
        )
    else:
        return f"Unknown tool: {tool_name}"


SYSTEM_PROMPT = """You are an expert Site Reliability Engineer performing root cause analysis for a production incident.

Your goal is to identify the most likely root cause of the incident using the available tools:
1. First, search for similar historical incidents to learn from past patterns
2. Get the relevant runbook for the anomaly type
3. Get architecture info for the affected service
4. Based on all evidence gathered, formulate root cause hypotheses
5. Submit your final RCA with confidence scores

Be systematic and evidence-driven. Consider:
- Deployment correlation (was there a recent deployment?)
- Cascade failures (is this a symptom of an upstream issue?)
- Historical patterns (have we seen this before?)
- Service-specific common failure modes

When you have gathered sufficient evidence, call submit_rca with your conclusions.
Rank candidates by confidence (1.0 = certain, 0.5 = plausible, 0.3 = possible)."""


def investigate(incident: IncidentOpenedEvent) -> dict:
    """Run the Claude investigation loop. Returns the extracted RCA data."""
    service = incident.affected_services[0] if incident.affected_services else "unknown"

    # Build context for Claude
    signals_text = "\n".join(
        f"  - {s.signal_type} (strength={s.strength:.2f}): {s.description}"
        for s in incident.correlation_signals
    ) or "  No correlation signals"

    deployment_text = (
        f"Deployment {incident.deployment_context.get('version')} occurred "
        f"{incident.deployment_context.get('delta_minutes')} minutes before anomaly"
        if incident.deployment_context else "No recent deployment detected"
    )

    metrics_summary = "\n".join(
        f"  {m}: {vals[:5]}"
        for m, vals in incident.recent_metrics.items()
    )

    prompt = (
        f"## Production Incident Investigation Required\n\n"
        f"**Incident ID:** {incident.incident_id}\n"
        f"**Anomaly Type:** {incident.anomaly_type}\n"
        f"**Severity:** {incident.severity}\n"
        f"**Affected Service:** {service}\n"
        f"**Description:** {incident.description}\n\n"
        f"**Correlation Signals:**\n{signals_text}\n\n"
        f"**Deployment Context:** {deployment_text}\n\n"
        f"**Recent Metrics (last 5 readings):**\n{metrics_summary}\n\n"
        f"Please investigate this incident and identify the root cause. "
        f"Use the available tools to search historical incidents and runbooks. "
        f"Submit your final RCA when ready."
    )

    rca_result = None
    raw_result = {}

    def capturing_tool_executor(tool_name: str, tool_input: dict) -> str:
        nonlocal rca_result, raw_result
        result = tool_executor(tool_name, tool_input)
        if tool_name == "submit_rca":
            try:
                raw_result = json.loads(result)
                rca_result = raw_result
            except Exception:
                pass
        return result

    try:
        _final_text = run_tool_use_agent(
            system=SYSTEM_PROMPT,
            initial_prompt=prompt,
            tools=TOOLS,
            tool_executor=capturing_tool_executor,
            model=SONNET,
            max_tokens=4096,
            max_rounds=12,
        )
    except Exception as exc:
        logger.exception("Investigation agent failed: %s", exc)

    return raw_result or {
        "top_hypothesis": f"Unable to determine root cause — {incident.anomaly_type} on {service}",
        "confidence": 0.3,
        "evidence": [incident.description],
        "all_candidates": [{
            "rank": 1,
            "hypothesis": f"{incident.anomaly_type} detected on {service}",
            "confidence": 0.3,
            "evidence": [incident.description],
        }],
        "estimated_user_impact": incident.blast_radius.get("estimated_user_impact", "Unknown"),
    }


def handle_incident_opened(raw: dict) -> None:
    try:
        incident = IncidentOpenedEvent(**raw)
    except Exception as exc:
        logger.error("Invalid incident event: %s", exc)
        return

    logger.info("Investigating incident %s (type=%s)", incident.incident_id, incident.anomaly_type)

    rca_data = investigate(incident)

    # Build RCACompletedEvent
    candidates = []
    for i, c in enumerate(rca_data.get("all_candidates", []), 1):
        candidates.append(RootCauseCandidate(
            rank=i,
            hypothesis=c.get("hypothesis", ""),
            confidence=float(c.get("confidence", 0.3)),
            evidence=c.get("evidence", []),
        ))

    if not candidates:
        candidates = [RootCauseCandidate(
            rank=1,
            hypothesis=rca_data.get("top_hypothesis", "Unknown"),
            confidence=float(rca_data.get("confidence", 0.3)),
            evidence=rca_data.get("evidence", []),
        )]

    rca_event = RCACompletedEvent(
        incident_id=incident.incident_id,
        root_cause_candidates=candidates,
        top_root_cause=rca_data.get("top_hypothesis", ""),
        top_confidence=float(rca_data.get("confidence", 0.3)),
        blast_radius=incident.blast_radius,
        anomaly_type=incident.anomaly_type,
        affected_services=incident.affected_services,
        severity=incident.severity,
    )

    # Update Postgres
    try:
        update_incident(incident.incident_id, {
            "status": "RCA_COMPLETE",
            "rca_candidates": [c.model_dump() for c in candidates],
            "top_root_cause": rca_event.top_root_cause,
            "rca_confidence": rca_event.top_confidence,
        })
        log_agent_event(
            incident.incident_id, "investigation-agent", "RCA_COMPLETE",
            {"top_cause": rca_event.top_root_cause, "confidence": rca_event.top_confidence},
        )
    except Exception as exc:
        logger.error("DB update failed: %s", exc)

    publish(producer, "rca.completed", rca_event.model_dump())
    logger.info(
        "Published rca.completed for %s — top cause: %.0f%% confidence: %s",
        incident.incident_id,
        rca_event.top_confidence * 100,
        rca_event.top_root_cause[:80],
    )

    # Start resolution monitoring in background
    service = incident.affected_services[0] if incident.affected_services else ""
    trigger_metric = "service_latency_p99_ms"
    if incident.anomaly_type == "ERROR_RATE_SPIKE":
        trigger_metric = "service_error_rate_percent"
    elif incident.anomaly_type == "CPU_SATURATION":
        trigger_metric = "service_cpu_percent"
    elif incident.anomaly_type == "MEMORY_LEAK":
        trigger_metric = "service_memory_percent"
    elif incident.anomaly_type == "DB_CONNECTION_EXHAUSTION":
        trigger_metric = "service_db_connections"
    elif incident.anomaly_type == "KAFKA_CONSUMER_LAG":
        trigger_metric = "kafka_consumer_lag"

    with active_incidents_lock:
        active_incidents[incident.incident_id] = {
            "service": service,
            "trigger_metric": trigger_metric,
            "anomaly_type": incident.anomaly_type,
            "severity": incident.severity,
            "detection_time": incident.detection_time,
            "top_root_cause": rca_event.top_root_cause,
            "normal_count": 0,
            "rca_event": rca_event.model_dump(),
        }


# ─── Resolution monitoring ─────────────────────────────────────────────────────

NORMAL_THRESHOLDS = {
    "service_latency_p99_ms":     1500,
    "service_error_rate_percent": 2.0,
    "service_cpu_percent":        75.0,
    "service_memory_percent":     80.0,
    "service_db_connections":     80.0,
    "kafka_consumer_lag":         5000,
}


def resolution_monitor_loop() -> None:
    """Background thread: check if active incidents have recovered."""
    while True:
        time.sleep(RESOLUTION_INTERVAL)
        with active_incidents_lock:
            resolved_ids = []
            for incident_id, ctx in active_incidents.items():
                service        = ctx["service"]
                trigger_metric = ctx["trigger_metric"]
                threshold      = NORMAL_THRESHOLDS.get(trigger_metric, float("inf"))

                history = get_metric_history(service, trigger_metric, count=5)
                if not history:
                    continue

                # Check if recent readings are consistently below threshold
                recent = history[:3]
                if all(v < threshold for v in recent):
                    ctx["normal_count"] += 1
                else:
                    ctx["normal_count"] = 0

                if ctx["normal_count"] >= NORMAL_READINGS_REQUIRED:
                    logger.info("Incident %s appears resolved — metrics normalizing.", incident_id)
                    resolved_ids.append(incident_id)
                    _publish_resolution(incident_id, ctx)

            for rid in resolved_ids:
                del active_incidents[rid]


def _publish_resolution(incident_id: str, ctx: dict) -> None:
    now = now_ms()
    mttr = (now - ctx["detection_time"]) / 60_000

    resolved = IncidentResolvedEvent(
        incident_id=incident_id,
        detection_time=ctx["detection_time"],
        mttr_minutes=round(mttr, 1),
        resolution_method="AUTOMATIC_RECOVERY",
        top_root_cause=ctx.get("top_root_cause", ""),
        anomaly_type=ctx.get("anomaly_type", ""),
        affected_services=[ctx.get("service", "")],
        severity=ctx.get("severity", "HIGH"),
    )

    try:
        update_incident(incident_id, {
            "status": "RESOLVED",
            "resolution_time": now,
        })
        log_agent_event(
            incident_id, "investigation-agent", "INCIDENT_RESOLVED",
            {"mttr_minutes": mttr, "resolution_method": "AUTOMATIC_RECOVERY"},
        )
    except Exception as exc:
        logger.error("DB resolution update failed: %s", exc)

    publish(producer, "incidents.resolved", resolved.model_dump())
    logger.info(
        "Incident %s RESOLVED — MTTR: %.1f minutes",
        incident_id, mttr,
    )


if __name__ == "__main__":
    logger.info("Investigation Agent starting...")
    init_pool()
    get_redis()
    init_qdrant()

    logger.info("Loading embedding model...")
    embedder = SentenceTransformer("all-MiniLM-L6-v2")
    logger.info("Embedder ready.")

    producer = make_producer()

    # Start resolution monitor background thread
    monitor_thread = threading.Thread(target=resolution_monitor_loop, daemon=True)
    monitor_thread.start()
    logger.info("Resolution monitor started.")

    consumer = make_consumer(
        topics=["incidents.opened"],
        group_id="investigation-agent",
    )
    logger.info("Investigation Agent ready. Consuming incidents.opened...")
    consume_loop(consumer, handle_incident_opened)
