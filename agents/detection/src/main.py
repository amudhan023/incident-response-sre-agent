"""
Detection Agent — first-line anomaly detection over raw telemetry streams.

Algorithm:
  1. Consume raw.metrics and raw.logs from Kafka
  2. Maintain rolling baseline (last N values) per (service, metric) in Redis
  3. Compute z-score; if > threshold → candidate anomaly
  4. Redis dedup to suppress alert storms
  5. Call Claude Haiku to classify anomaly type and severity
  6. Insert incident into Postgres
  7. Publish AnomalyDetectedEvent to Kafka
"""
from __future__ import annotations
import logging
import math
import os
import sys
import json
import uuid

sys.path.insert(0, "/app")
from shared.models import AnomalyDetectedEvent, now_ms
from shared.kafka_client import make_producer, make_consumer, publish, consume_loop
from shared.redis_client import get_client as get_redis, push_metric, get_metric_history, set_dedup
from shared.db_client import init_pool, insert_incident, log_agent_event
from shared.llm_client import chat, HAIKU

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("detection-agent")

SIGMA_THRESHOLD     = float(os.getenv("SIGMA_THRESHOLD", "2.5"))
DEDUP_TTL           = int(os.getenv("DEDUP_TTL_SECONDS", "300"))
BASELINE_WINDOW     = int(os.getenv("BASELINE_WINDOW_SIZE", "50"))
MIN_SAMPLES         = 10  # Need at least this many samples before detecting

# Only detect anomalies on these metric names (ignore request_rate for raw detection)
MONITORED_METRICS = {
    "service_latency_p99_ms":    ("LATENCY_SPIKE",           "HIGH"),
    "service_error_rate_percent":("ERROR_RATE_SPIKE",        "HIGH"),
    "service_cpu_percent":       ("CPU_SATURATION",          "HIGH"),
    "service_memory_percent":    ("MEMORY_LEAK",             "HIGH"),
    "service_db_connections":    ("DB_CONNECTION_EXHAUSTION", "HIGH"),
    "kafka_consumer_lag":        ("KAFKA_CONSUMER_LAG",      "MEDIUM"),
}

SEVERITY_THRESHOLDS = {
    "service_latency_p99_ms":     {"CRITICAL": 5.0, "HIGH": 3.0},
    "service_error_rate_percent": {"CRITICAL": 6.0, "HIGH": 3.5},
    "service_cpu_percent":        {"CRITICAL": 5.5, "HIGH": 3.0},
    "service_memory_percent":     {"CRITICAL": 5.0, "HIGH": 3.0},
    "service_db_connections":     {"CRITICAL": 4.0, "HIGH": 2.8},
    "kafka_consumer_lag":         {"CRITICAL": 5.0, "HIGH": 3.0},
}

producer = None


def compute_zscore(values: list[float], new_value: float) -> tuple[float, float, float]:
    """Returns (zscore, mean, std)."""
    if len(values) < MIN_SAMPLES:
        return 0.0, new_value, 0.0
    n = len(values)
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / n
    std = math.sqrt(variance) if variance > 0 else 0.0
    if std < 1e-9:
        return 0.0, mean, std
    return (new_value - mean) / std, mean, std


def determine_severity(metric_name: str, zscore: float) -> str:
    thresholds = SEVERITY_THRESHOLDS.get(metric_name, {"CRITICAL": 6.0, "HIGH": 3.0})
    if zscore >= thresholds["CRITICAL"]:
        return "CRITICAL"
    if zscore >= thresholds["HIGH"]:
        return "HIGH"
    return "MEDIUM"


def classify_with_llm(service: str, metric: str, observed: float, baseline: float, zscore: float, default_type: str) -> dict:
    """Quick Claude Haiku call to classify and describe the anomaly."""
    try:
        system = (
            "You are an SRE anomaly classifier. Given a metric anomaly, output ONLY valid JSON with keys: "
            "'anomaly_type' (one of: LATENCY_SPIKE, ERROR_RATE_SPIKE, CPU_SATURATION, MEMORY_LEAK, "
            "DB_CONNECTION_EXHAUSTION, KAFKA_CONSUMER_LAG, DEPLOYMENT_FAILURE, DEPENDENCY_OUTAGE, UNKNOWN), "
            "'description' (one sentence describing the anomaly in human-readable terms). "
            "Output ONLY the JSON object, nothing else."
        )
        user = (
            f"Service: {service}\n"
            f"Metric: {metric}\n"
            f"Observed value: {observed:.1f}\n"
            f"Baseline (mean): {baseline:.1f}\n"
            f"Deviation: {zscore:.1f} standard deviations above normal\n"
            f"Default classification: {default_type}"
        )
        raw = chat(system=system, user=user, model=HAIKU, max_tokens=200, temperature=0.1)
        # Extract JSON from response
        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw.strip())
    except Exception as exc:
        logger.warning("LLM classification failed, using default: %s", exc)
        return {
            "anomaly_type": default_type,
            "description": f"{service} {metric} deviated {zscore:.1f}σ above baseline (observed: {observed:.1f}, baseline: {baseline:.1f})",
        }


def handle_metric_event(event: dict) -> None:
    service     = event.get("source_service", "")
    metric_name = event.get("metric_name", "")
    metric_val  = float(event.get("metric_value", 0.0))

    if metric_name not in MONITORED_METRICS:
        return

    # Update rolling baseline in Redis
    push_metric(service, metric_name, metric_val, max_len=BASELINE_WINDOW)
    history = get_metric_history(service, metric_name, count=BASELINE_WINDOW)

    if len(history) < MIN_SAMPLES:
        return

    # The most recent value (history[0]) is the one just pushed
    baseline_values = history[1:]  # exclude current for fair comparison
    zscore, mean, std = compute_zscore(baseline_values, metric_val)

    if zscore < SIGMA_THRESHOLD:
        return  # Normal — no anomaly

    # Dedup: suppress identical anomaly on same service+metric within TTL
    dedup_key = f"dedup:{service}:{metric_name}"
    if not set_dedup(dedup_key, DEDUP_TTL):
        return  # Duplicate within window

    logger.info(
        "Anomaly detected: service=%s metric=%s value=%.2f mean=%.2f zscore=%.2f",
        service, metric_name, metric_val, mean, zscore,
    )

    default_type, default_severity = MONITORED_METRICS[metric_name]
    severity = determine_severity(metric_name, zscore)

    # LLM classification (fast — Haiku model)
    classification = classify_with_llm(service, metric_name, metric_val, mean, zscore, default_type)

    incident_id = str(uuid.uuid4())
    anomaly_score = min(1.0, (zscore - SIGMA_THRESHOLD) / 5.0 + 0.5)

    evt = AnomalyDetectedEvent(
        incident_id=incident_id,
        anomaly_type=classification.get("anomaly_type", default_type),
        severity=severity,
        affected_services=[service],
        trigger_metric=metric_name,
        observed_value=metric_val,
        baseline_value=round(mean, 2),
        deviation_sigma=round(zscore, 2),
        anomaly_score=round(anomaly_score, 3),
        description=classification.get("description", ""),
        raw_events=[event.get("event_id", "")],
    )

    # Persist to Postgres
    try:
        insert_incident(incident_id, evt.model_dump())
        log_agent_event(
            incident_id, "detection-agent", "ANOMALY_DETECTED",
            {"metric": metric_name, "zscore": zscore, "value": metric_val},
        )
    except Exception as exc:
        logger.error("Failed to insert incident: %s", exc)

    # Publish to Kafka
    publish(producer, "anomalies.detected", evt.model_dump())
    logger.info("Published anomaly %s (type=%s severity=%s)", incident_id, evt.anomaly_type, severity)


def handle_log_event(event: dict) -> None:
    level   = event.get("level", "INFO")
    message = event.get("message", "")
    service = event.get("source_service", "")

    if level not in ("ERROR", "FATAL", "CRITICAL"):
        return

    # Detect log-based anomalies (error keyword patterns)
    error_keywords = ["OOM", "OutOfMemoryError", "connection refused", "FATAL",
                      "panic", "CrashLoopBackoff", "pool exhausted"]
    if not any(kw.lower() in message.lower() for kw in error_keywords):
        return

    # Only detect if we haven't seen an error log anomaly for this service recently
    dedup_key = f"dedup:log:{service}:error"
    if not set_dedup(dedup_key, 120):  # 2-minute dedup for log anomalies
        return

    # Determine anomaly type from log keywords
    anomaly_type = "UNKNOWN"
    if "OOM" in message or "OutOfMemory" in message or "heap" in message.lower():
        anomaly_type = "MEMORY_LEAK"
    elif "connection" in message.lower() and "pool" in message.lower():
        anomaly_type = "DB_CONNECTION_EXHAUSTION"
    elif "kafka" in message.lower() or "consumer" in message.lower():
        anomaly_type = "KAFKA_CONSUMER_LAG"
    elif "connection refused" in message.lower() or "CrashLoop" in message:
        anomaly_type = "DEPENDENCY_OUTAGE"

    incident_id = str(uuid.uuid4())
    evt = AnomalyDetectedEvent(
        incident_id=incident_id,
        anomaly_type=anomaly_type,
        severity="HIGH",
        affected_services=[service],
        trigger_metric="log_error",
        observed_value=1.0,
        baseline_value=0.0,
        deviation_sigma=0.0,
        anomaly_score=0.7,
        description=f"Critical log detected: {message[:200]}",
        raw_events=[event.get("event_id", "")],
    )

    try:
        insert_incident(incident_id, evt.model_dump())
    except Exception as exc:
        logger.error("Failed to insert log-based incident: %s", exc)

    publish(producer, "anomalies.detected", evt.model_dump())
    logger.warning("Log-based anomaly %s: %s", incident_id, message[:100])


def dispatch(event: dict) -> None:
    event_type = event.get("event_type", "")
    if event_type == "METRIC":
        handle_metric_event(event)
    elif event_type == "LOG":
        handle_log_event(event)


if __name__ == "__main__":
    logger.info("Detection Agent starting...")
    init_pool()
    get_redis()  # warm up connection
    producer = make_producer()
    consumer = make_consumer(
        topics=["raw.metrics", "raw.logs"],
        group_id="detection-agent",
    )
    logger.info("Detection Agent ready. Consuming raw.metrics + raw.logs...")
    consume_loop(consumer, dispatch)
