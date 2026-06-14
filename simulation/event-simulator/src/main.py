"""
Event Simulator — generates realistic production telemetry and injects failures.

Runs two concurrent threads:
  1. Normal traffic: continuous baseline metric + log events → Kafka
  2. Failure injector: periodic anomaly scenarios → Kafka + Prometheus
  3. HTTP server: /metrics endpoint for Prometheus scraping
"""
from __future__ import annotations
import json
import logging
import math
import os
import random
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Callable

sys.path.insert(0, "/app")
from shared.models import RawMetricEvent, RawLogEvent, RawDeploymentEvent, now_ms
from shared.kafka_client import make_producer, publish, flush

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("event-simulator")

# ─── Configuration ─────────────────────────────────────────────────────────────

METRICS_PORT         = int(os.getenv("METRICS_PORT", "8100"))
FAILURE_MIN          = int(os.getenv("FAILURE_INTERVAL_MIN_SECONDS", "300"))
FAILURE_MAX          = int(os.getenv("FAILURE_INTERVAL_MAX_SECONDS", "600"))
INITIAL_QUIET        = int(os.getenv("INITIAL_QUIET_PERIOD_SECONDS", "60"))
PUBLISH_INTERVAL     = 10   # publish metric snapshot every 10 seconds
LOG_INTERVAL         = 3    # publish log events every 3 seconds

SERVICES = [
    "api-gateway",
    "payment-service",
    "order-service",
    "user-service",
    "notification-service",
    "inventory-service",
]

# ─── Live state (modified by failure injector) ─────────────────────────────────

@dataclass
class ServiceState:
    name: str
    # Baseline normals
    base_request_rate:  float = 50.0
    base_error_rate:    float = 0.5
    base_latency_p99:   float = 200.0
    base_cpu:           float = 25.0
    base_memory:        float = 45.0
    base_db_conn:       float = 20.0
    base_kafka_lag:     float = 0.0
    # Live values (injected by failure scenarios)
    request_rate:  float = field(init=False)
    error_rate:    float = field(init=False)
    latency_p99:   float = field(init=False)
    cpu:           float = field(init=False)
    memory:        float = field(init=False)
    db_conn:       float = field(init=False)
    kafka_lag:     float = field(init=False)
    is_degraded:   bool  = False
    failure_type:  str   = ""

    def __post_init__(self):
        self.request_rate = self.base_request_rate
        self.error_rate   = self.base_error_rate
        self.latency_p99  = self.base_latency_p99
        self.cpu          = self.base_cpu
        self.memory       = self.base_memory
        self.db_conn      = self.base_db_conn
        self.kafka_lag    = self.base_kafka_lag

    def normal_values(self) -> dict[str, float]:
        """Add natural jitter to baseline values."""
        jitter = lambda base, pct=0.08: base * (1 + random.uniform(-pct, pct))
        return {
            "service_request_rate":   jitter(self.request_rate),
            "service_error_rate_percent": jitter(self.error_rate, 0.3),
            "service_latency_p99_ms": jitter(self.latency_p99),
            "service_cpu_percent":    jitter(self.cpu),
            "service_memory_percent": jitter(self.memory, 0.05),
            "service_db_connections": jitter(self.db_conn),
            "kafka_consumer_lag":     max(0, jitter(self.kafka_lag, 0.5)),
        }


# Initialize service states with varied baselines
STATES: dict[str, ServiceState] = {
    "api-gateway":         ServiceState("api-gateway",         base_request_rate=150, base_latency_p99=120, base_cpu=30),
    "payment-service":     ServiceState("payment-service",     base_request_rate=80,  base_latency_p99=380, base_cpu=22, base_db_conn=30),
    "order-service":       ServiceState("order-service",       base_request_rate=60,  base_latency_p99=280, base_cpu=20, base_db_conn=15, base_kafka_lag=100),
    "user-service":        ServiceState("user-service",        base_request_rate=200, base_latency_p99=150, base_cpu=18),
    "notification-service":ServiceState("notification-service",base_request_rate=40,  base_latency_p99=500, base_memory=55, base_kafka_lag=200),
    "inventory-service":   ServiceState("inventory-service",   base_request_rate=30,  base_latency_p99=220, base_cpu=15, base_db_conn=10),
}

# Prometheus-format metric registry (key → value)
_prom_metrics: dict[str, float] = {}
_prom_lock = threading.Lock()
_failure_count = 0

# ─── Failure scenarios ─────────────────────────────────────────────────────────

def _gradual(current: float, target: float, step_pct: float = 0.3) -> float:
    """Move current toward target by step_pct of the distance."""
    return current + (target - current) * step_pct


@dataclass
class FailureScenario:
    name:        str
    description: str
    service:     str
    duration_s:  int
    injector:    Callable[[ServiceState, float], None]  # (state, progress 0→1) → None
    recoverer:   Callable[[ServiceState], None]

    def run(self, producer) -> None:
        global _failure_count
        state = STATES[self.service]
        state.is_degraded  = True
        state.failure_type = self.name
        _failure_count += 1

        logger.warning("💥 INJECTING FAILURE: %s on %s (duration=%ds)", self.name, self.service, self.duration_s)

        # Publish deployment event first for DEPLOYMENT_FAILURE type
        if "DEPLOYMENT" in self.name:
            _publish_deployment(producer, self.service, self.name)

        start = time.time()
        while time.time() - start < self.duration_s:
            progress = (time.time() - start) / self.duration_s
            self.injector(state, progress)
            time.sleep(5)

        # Recover
        logger.info("✅ RECOVERING from %s on %s", self.name, self.service)
        self.recoverer(state)
        state.is_degraded  = False
        state.failure_type = ""


def _publish_deployment(producer, service: str, reason: str) -> None:
    evt = RawDeploymentEvent(
        source_service=service,
        version=f"v{random.randint(1,9)}.{random.randint(0,99)}.{random.randint(0,9)}",
        deployed_by="ci-cd-pipeline",
        change_type="CODE",
        description=f"Automated deployment triggered by {reason}",
        known_risks=["Increased latency during rollout", "Database migration"],
    )
    publish(producer, "raw.deployments", evt.model_dump())
    logger.info("Published deployment event for %s", service)


SCENARIOS: list[FailureScenario] = [
    FailureScenario(
        name="LATENCY_SPIKE",
        description="Payment service p99 latency spikes 20x normal",
        service="payment-service",
        duration_s=240,
        injector=lambda s, p: setattr(s, "latency_p99", s.base_latency_p99 + (7000 * min(p * 2, 1.0))),
        recoverer=lambda s: setattr(s, "latency_p99", s.base_latency_p99),
    ),
    FailureScenario(
        name="ERROR_RATE_SPIKE",
        description="Order service error rate spikes to 45%",
        service="order-service",
        duration_s=180,
        injector=lambda s, p: setattr(s, "error_rate", 0.5 + 44.5 * min(p * 3, 1.0)),
        recoverer=lambda s: setattr(s, "error_rate", s.base_error_rate),
    ),
    FailureScenario(
        name="CPU_SATURATION",
        description="API gateway CPU saturates to 97%",
        service="api-gateway",
        duration_s=300,
        injector=lambda s, p: setattr(s, "cpu", 30 + 67 * min(p, 1.0)),
        recoverer=lambda s: setattr(s, "cpu", s.base_cpu),
    ),
    FailureScenario(
        name="MEMORY_LEAK",
        description="Notification service memory grows to OOM",
        service="notification-service",
        duration_s=360,
        injector=lambda s, p: setattr(s, "memory", min(98, 55 + 43 * p)),
        recoverer=lambda s: setattr(s, "memory", s.base_memory),
    ),
    FailureScenario(
        name="DB_CONNECTION_EXHAUSTION",
        description="Payment service database connection pool exhausted",
        service="payment-service",
        duration_s=200,
        injector=lambda s, p: (
            setattr(s, "db_conn", min(100, 30 + 70 * min(p * 2, 1.0))),
            setattr(s, "error_rate", min(30, 0.5 + 29.5 * max(0, p - 0.4) * 5)),
        ),
        recoverer=lambda s: (
            setattr(s, "db_conn", s.base_db_conn),
            setattr(s, "error_rate", s.base_error_rate),
        ),
    ),
    FailureScenario(
        name="KAFKA_CONSUMER_LAG",
        description="Order service Kafka consumer lag grows to 50k messages",
        service="order-service",
        duration_s=300,
        injector=lambda s, p: setattr(s, "kafka_lag", 100 + 49900 * min(p, 1.0)),
        recoverer=lambda s: setattr(s, "kafka_lag", s.base_kafka_lag),
    ),
    FailureScenario(
        name="DEPLOYMENT_FAILURE",
        description="Bad deployment on user-service triggers error spike",
        service="user-service",
        duration_s=240,
        injector=lambda s, p: (
            setattr(s, "error_rate", min(40, 0.5 + 39.5 * min(p * 4, 1.0))),
            setattr(s, "latency_p99", 150 + 2000 * min(p, 1.0)),
        ),
        recoverer=lambda s: (
            setattr(s, "error_rate", s.base_error_rate),
            setattr(s, "latency_p99", s.base_latency_p99),
        ),
    ),
]


# ─── Prometheus HTTP handler ────────────────────────────────────────────────────

class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return
        lines = []
        with _prom_lock:
            for key, val in sorted(_prom_metrics.items()):
                lines.append(f"{key} {val:.4f}")
        body = "\n".join(lines) + "\n"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.end_headers()
        self.wfile.write(body.encode())

    def log_message(self, fmt, *args):
        pass  # suppress access logs


def _update_prom(service: str, values: dict[str, float]) -> None:
    with _prom_lock:
        for metric, val in values.items():
            _prom_metrics[f'{metric}{{service="{service}"}}'] = val
        _prom_metrics["failure_injections_total"] = _failure_count


# ─── Error log messages ────────────────────────────────────────────────────────

ERROR_MESSAGES = {
    "LATENCY_SPIKE": [
        "Database query exceeded timeout threshold: 30000ms",
        "Connection pool wait time exceeded 15s — request queuing",
        "Slow query detected: SELECT * FROM transactions WHERE user_id=? took 8234ms",
        "HTTP upstream timeout after 10s — payment gateway unresponsive",
    ],
    "ERROR_RATE_SPIKE": [
        "Internal server error: NullPointerException in OrderProcessor.process()",
        "Failed to deserialize request body: unexpected token at position 42",
        "Circuit breaker OPEN for downstream payment-service",
        "Unhandled exception in request pipeline — returning 500",
    ],
    "CPU_SATURATION": [
        "GC pause duration exceeded 2000ms — STW collection triggered",
        "Thread pool exhausted: 500/500 active threads, 1200 queued",
        "CPU throttling detected — request processing degraded",
        "Infinite loop detected in RequestRouter — watchdog triggered",
    ],
    "MEMORY_LEAK": [
        "Heap usage at 87% — approaching OOM threshold",
        "Memory pressure: evicting 10000 cache entries to free space",
        "Suspected connection leak: 450 unclosed connections detected",
        "OutOfMemoryError: Java heap space — GC overhead limit exceeded",
    ],
    "DB_CONNECTION_EXHAUSTION": [
        "Connection pool exhausted: timeout waiting for available connection",
        "FATAL: remaining connection slots reserved for replication",
        "PG::ConnectionBad: SSL SYSCALL error: connection reset by peer",
        "Database connection timeout after 30s — pool size: 100/100",
    ],
    "KAFKA_CONSUMER_LAG": [
        "Consumer group lag growing: order-processor lag=45231 on partition 0",
        "Rebalance triggered for consumer group order-processor",
        "Poll interval exceeded max.poll.interval.ms — consumer kicked from group",
        "Offset commit failed: leader not available",
    ],
    "DEPLOYMENT_FAILURE": [
        "Health check failed: /health returned 503 after deployment",
        "Startup probe failing: timeout waiting for service to become ready",
        "Configuration error: DATABASE_URL not set in new deployment",
        "Rollout paused: error threshold 10% exceeded during canary",
    ],
}

NORMAL_LOGS = [
    "Request completed successfully in {latency}ms — POST /api/v1/payments",
    "Cache hit for key user:{uid} — returning cached profile",
    "Database connection acquired from pool ({conn}/{max} active)",
    "Background job completed: sending {n} notifications",
    "Health check passed — all dependencies healthy",
    "Rate limit check passed for client {ip}",
    "Successfully published {n} events to Kafka topic orders",
    "JWT token validated for user {uid}",
]


# ─── Main loops ───────────────────────────────────────────────────────────────

def traffic_loop(producer) -> None:
    """Continuously publish metric and log events for all services."""
    tick = 0
    while True:
        tick += 1
        for svc_name, state in STATES.items():
            values = state.normal_values()
            _update_prom(svc_name, values)

            # Publish metric events
            for metric_name, metric_value in values.items():
                evt = RawMetricEvent(
                    source_service=svc_name,
                    metric_name=metric_name,
                    metric_value=metric_value,
                )
                publish(producer, "raw.metrics", evt.model_dump())

            # Publish log events every LOG_INTERVAL ticks
            if tick % (LOG_INTERVAL // PUBLISH_INTERVAL + 1) == 0 or True:
                if state.is_degraded and state.failure_type in ERROR_MESSAGES:
                    msgs = ERROR_MESSAGES[state.failure_type]
                    msg = random.choice(msgs)
                    level = "ERROR"
                else:
                    tmpl = random.choice(NORMAL_LOGS)
                    msg = tmpl.format(
                        latency=int(state.latency_p99 * random.uniform(0.3, 0.8)),
                        uid=random.randint(10000, 99999),
                        ip=f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
                        n=random.randint(1, 500),
                        conn=int(state.db_conn),
                        max=100,
                    )
                    level = "INFO"

                log_evt = RawLogEvent(
                    source_service=svc_name,
                    level=level,
                    message=msg,
                )
                publish(producer, "raw.logs", log_evt.model_dump())

        producer.poll(0)
        time.sleep(PUBLISH_INTERVAL)


def failure_loop(producer) -> None:
    """Periodically inject failure scenarios."""
    logger.info("Failure injector quiet period: %ds", INITIAL_QUIET)
    time.sleep(INITIAL_QUIET)

    while True:
        wait = random.randint(FAILURE_MIN, FAILURE_MAX)
        logger.info("Next failure injection in %ds", wait)
        time.sleep(wait)

        scenario = random.choice(SCENARIOS)
        # Don't inject on an already-degraded service
        if STATES[scenario.service].is_degraded:
            logger.info("Service %s already degraded — skipping", scenario.service)
            continue

        t = threading.Thread(target=scenario.run, args=(producer,), daemon=True)
        t.start()


def metrics_server() -> None:
    server = HTTPServer(("0.0.0.0", METRICS_PORT), MetricsHandler)
    logger.info("Prometheus metrics server on :%d", METRICS_PORT)
    server.serve_forever()


if __name__ == "__main__":
    logger.info("Starting Event Simulator")

    producer = make_producer()
    logger.info("Kafka producer connected")

    # Start Prometheus metrics server
    t_metrics = threading.Thread(target=metrics_server, daemon=True)
    t_metrics.start()

    # Start failure injector
    t_failures = threading.Thread(target=failure_loop, args=(producer,), daemon=True)
    t_failures.start()

    # Run traffic generator (blocking)
    logger.info("Starting traffic generation for %d services", len(SERVICES))
    traffic_loop(producer)
