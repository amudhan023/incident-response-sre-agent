"""
Correlation Agent — builds incident context from an anomaly signal.

Receives: anomalies.detected
Produces: incidents.opened

Responsibilities:
  - Fetch recent metric history for affected service from Redis
  - Check if any deployment happened in the last 60 minutes (from raw.deployments cache)
  - Identify related service degradation (cascade check)
  - Use Claude to analyze correlation and estimate blast radius
  - Publish enriched incidents.opened event
"""
from __future__ import annotations
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, "/app")
from shared.models import (
    AnomalyDetectedEvent, IncidentOpenedEvent, CorrelationSignal, now_ms
)
from shared.kafka_client import make_producer, make_consumer, publish, consume_loop
from shared.redis_client import get_client as get_redis, get_metric_history, get_json, set_json
from shared.db_client import init_pool, update_incident, log_agent_event, fetch_all
from shared.llm_client import chat, SONNET

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("correlation-agent")

# Dependency map: which services depend on which
DEPENDENCIES: dict[str, list[str]] = {
    "api-gateway":          ["payment-service", "order-service", "user-service"],
    "order-service":        ["payment-service", "inventory-service"],
    "payment-service":      [],
    "user-service":         [],
    "notification-service": [],
    "inventory-service":    [],
}

UPSTREAM: dict[str, list[str]] = {
    "payment-service":     ["api-gateway", "order-service"],
    "order-service":       ["api-gateway"],
    "user-service":        ["api-gateway"],
    "inventory-service":   ["order-service"],
    "notification-service":[],
    "api-gateway":         [],
}

producer = None


def get_recent_metrics(service: str) -> dict[str, list[float]]:
    metrics = [
        "service_latency_p99_ms",
        "service_error_rate_percent",
        "service_cpu_percent",
        "service_memory_percent",
        "service_db_connections",
        "kafka_consumer_lag",
    ]
    result = {}
    for m in metrics:
        history = get_metric_history(service, m, count=20)
        if history:
            result[m] = history
    return result


def get_recent_deployments() -> list[dict]:
    """Get deployments from Redis cache (populated by event simulator)."""
    deps = get_json("recent_deployments") or []
    cutoff = now_ms() - 3600_000  # last 1 hour
    return [d for d in deps if d.get("timestamp", 0) > cutoff]


def detect_cascade(service: str, recent_metrics: dict) -> list[CorrelationSignal]:
    """Check if downstream services also show degradation."""
    signals = []
    downstream = DEPENDENCIES.get(service, [])

    for dep_svc in downstream:
        dep_metrics = get_recent_metrics(dep_svc)
        error_hist = dep_metrics.get("service_error_rate_percent", [])
        if error_hist and len(error_hist) >= 5:
            recent_avg = sum(error_hist[:5]) / 5
            if recent_avg > 5.0:
                signals.append(CorrelationSignal(
                    signal_type="CASCADE_FAILURE",
                    strength=min(recent_avg / 50.0, 1.0),
                    description=f"Downstream {dep_svc} also showing elevated error rate: {recent_avg:.1f}%",
                    evidence=[f"{dep_svc} error_rate={recent_avg:.1f}%"],
                ))

    return signals


def check_deployment_correlation(service: str, detection_time: int) -> dict | None:
    """Check if a deployment happened within 60 minutes before the anomaly."""
    deployments = get_recent_deployments()
    cutoff_ms = detection_time - 3_600_000  # 1 hour ago

    for dep in deployments:
        if dep.get("source_service") != service:
            continue
        dep_time = dep.get("timestamp", 0)
        if cutoff_ms <= dep_time <= detection_time:
            delta_minutes = (detection_time - dep_time) / 60_000
            return {
                "service":        service,
                "version":        dep.get("version", "unknown"),
                "deployed_at":    dep_time,
                "delta_minutes":  round(delta_minutes, 1),
                "change_type":    dep.get("change_type", "CODE"),
                "correlation_confidence": round(max(0.1, 1.0 - delta_minutes / 60.0), 2),
            }
    return None


def analyze_with_llm(
    anomaly: AnomalyDetectedEvent,
    metrics: dict[str, list[float]],
    cascade_signals: list[CorrelationSignal],
    deployment: dict | None,
) -> dict:
    """Ask Claude Sonnet to produce correlation analysis and blast radius estimate."""

    metrics_summary = "\n".join(
        f"  {m}: recent=[{', '.join(f'{v:.1f}' for v in vals[:5])}]"
        for m, vals in metrics.items()
    )

    cascade_text = "\n".join(
        f"  - {s.description}" for s in cascade_signals
    ) or "  None detected"

    deployment_text = (
        f"Deployment of {deployment['version']} on {deployment['service']} "
        f"{deployment['delta_minutes']} minutes before anomaly (confidence: {deployment['correlation_confidence']:.0%})"
        if deployment else "No recent deployment found"
    )

    system = (
        "You are an expert SRE performing incident correlation analysis. "
        "Analyze the provided telemetry signals and produce a correlation summary as JSON with these keys:\n"
        "  'correlation_summary': str (2-3 sentences describing what the signals indicate together)\n"
        "  'blast_radius_description': str (which users/operations are affected)\n"
        "  'estimated_user_impact': str (e.g., '15-20% of payment requests failing')\n"
        "  'primary_signal': str (which signal is the clearest indicator)\n"
        "  'deployment_likely_cause': bool\n"
        "Output ONLY valid JSON."
    )
    user = (
        f"Incident ID: {anomaly.incident_id}\n"
        f"Anomaly Type: {anomaly.anomaly_type}\n"
        f"Severity: {anomaly.severity}\n"
        f"Primary Service: {', '.join(anomaly.affected_services)}\n"
        f"Trigger Metric: {anomaly.trigger_metric} = {anomaly.observed_value:.1f} "
        f"(baseline: {anomaly.baseline_value:.1f}, deviation: {anomaly.deviation_sigma:.1f}σ)\n\n"
        f"Recent metric history:\n{metrics_summary}\n\n"
        f"Cascade signals:\n{cascade_text}\n\n"
        f"Deployment context: {deployment_text}\n"
    )
    try:
        raw = chat(system=system, user=user, model=SONNET, max_tokens=600, temperature=0.1)
        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw.strip())
    except Exception as exc:
        logger.warning("LLM correlation failed: %s", exc)
        return {
            "correlation_summary": f"{anomaly.anomaly_type} detected on {', '.join(anomaly.affected_services)}.",
            "blast_radius_description": f"Service {', '.join(anomaly.affected_services)} and its consumers",
            "estimated_user_impact": "Unknown",
            "primary_signal": anomaly.trigger_metric,
            "deployment_likely_cause": deployment is not None,
        }


def handle_anomaly(raw: dict) -> None:
    try:
        anomaly = AnomalyDetectedEvent(**raw)
    except Exception as exc:
        logger.error("Invalid anomaly event: %s", exc)
        return

    service = anomaly.affected_services[0] if anomaly.affected_services else "unknown"
    logger.info("Correlating anomaly %s on %s", anomaly.incident_id, service)

    # Update incident status
    try:
        update_incident(anomaly.incident_id, {"status": "CORRELATING"})
    except Exception as exc:
        logger.warning("Could not update incident status: %s", exc)

    # Gather context
    metrics = get_recent_metrics(service)
    cascade_signals = detect_cascade(service, metrics)
    deployment = check_deployment_correlation(service, anomaly.detection_time)

    # Build correlation signals list
    correlation_signals = []

    if deployment:
        correlation_signals.append(CorrelationSignal(
            signal_type="DEPLOYMENT_CORRELATED",
            strength=deployment["correlation_confidence"],
            description=f"Deployment {deployment['version']} occurred {deployment['delta_minutes']:.0f} min before anomaly",
            evidence=[f"version={deployment['version']}", f"delta={deployment['delta_minutes']}min"],
        ))

    correlation_signals.extend(cascade_signals)

    # High error rate signal
    err_hist = metrics.get("service_error_rate_percent", [])
    if err_hist and err_hist[0] > 5.0:
        correlation_signals.append(CorrelationSignal(
            signal_type="ERROR_AMPLIFICATION",
            strength=min(err_hist[0] / 50.0, 1.0),
            description=f"Error rate at {err_hist[0]:.1f}% — significantly above normal",
            evidence=[f"error_rate={err_hist[0]:.1f}%"],
        ))

    # Upstream dependency signal
    upstream = UPSTREAM.get(service, [])
    if upstream:
        for up_svc in upstream:
            up_err = get_metric_history(up_svc, "service_error_rate_percent", count=5)
            if up_err and up_err[0] > 3.0:
                correlation_signals.append(CorrelationSignal(
                    signal_type="UPSTREAM_DEGRADATION",
                    strength=0.6,
                    description=f"Upstream {up_svc} showing {up_err[0]:.1f}% error rate",
                    evidence=[f"{up_svc}_error_rate={up_err[0]:.1f}%"],
                ))

    # LLM correlation analysis
    llm_result = analyze_with_llm(anomaly, metrics, cascade_signals, deployment)

    # Build blast radius
    affected = list(anomaly.affected_services)
    cascade_svcs = [s.evidence[0].split("_error_rate=")[0] if "error_rate" in s.evidence[0] else ""
                    for s in cascade_signals if s.signal_type == "CASCADE_FAILURE"]
    affected.extend([s for s in cascade_svcs if s and s not in affected])

    blast_radius = {
        "affected_services":     affected,
        "estimated_user_impact": llm_result.get("estimated_user_impact", "Unknown"),
        "description":           llm_result.get("blast_radius_description", ""),
    }

    # Publish incidents.opened
    recent_errors = [
        f"error_rate={metrics['service_error_rate_percent'][0]:.1f}%"
        if "service_error_rate_percent" in metrics and metrics["service_error_rate_percent"] else ""
    ]
    recent_errors = [e for e in recent_errors if e]

    opened = IncidentOpenedEvent(
        incident_id=anomaly.incident_id,
        anomaly_type=anomaly.anomaly_type,
        severity=anomaly.severity,
        affected_services=affected,
        detection_time=anomaly.detection_time,
        correlation_signals=correlation_signals,
        blast_radius=blast_radius,
        deployment_context=deployment,
        recent_metrics={k: v[:10] for k, v in metrics.items()},
        recent_errors=recent_errors,
        description=llm_result.get("correlation_summary", anomaly.description),
    )

    # Update Postgres with correlation context
    try:
        update_incident(anomaly.incident_id, {
            "status": "INVESTIGATING",
            "correlation_context": {
                "signals": [s.model_dump() for s in correlation_signals],
                "llm_analysis": llm_result,
            },
            "blast_radius": blast_radius,
        })
        log_agent_event(
            anomaly.incident_id, "correlation-agent", "CORRELATION_COMPLETE",
            {"signals_count": len(correlation_signals), "deployment_correlated": deployment is not None},
        )
    except Exception as exc:
        logger.error("DB update failed: %s", exc)

    publish(producer, "incidents.opened", opened.model_dump())
    logger.info(
        "Published incidents.opened for %s (signals=%d, deployment=%s)",
        anomaly.incident_id, len(correlation_signals), deployment is not None,
    )


# Cache deployment events from the Kafka stream
_deployment_cache: list[dict] = []

def handle_deployment(raw: dict) -> None:
    """Cache deployment events in Redis for correlation lookups."""
    _deployment_cache.append(raw)
    # Keep last 20 deployments
    cache = _deployment_cache[-20:]
    set_json("recent_deployments", cache, ttl=7200)


def dispatch(raw: dict) -> None:
    event_type = raw.get("event_type", "")
    if event_type == "DEPLOYMENT":
        handle_deployment(raw)
    else:
        # All other events treated as anomaly events (from anomalies.detected topic)
        # Distinguish by presence of incident_id
        if "incident_id" in raw and "anomaly_type" in raw:
            handle_anomaly(raw)


if __name__ == "__main__":
    logger.info("Correlation Agent starting...")
    init_pool()
    get_redis()
    producer = make_producer()

    # Subscribe to both anomaly events AND deployment events for caching
    consumer = make_consumer(
        topics=["anomalies.detected", "raw.deployments"],
        group_id="correlation-agent",
    )
    logger.info("Correlation Agent ready.")
    consume_loop(consumer, dispatch)
