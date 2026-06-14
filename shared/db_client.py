"""PostgreSQL client with retry and connection pooling."""
from __future__ import annotations
import json
import logging
import os
import time
from contextlib import contextmanager
from typing import Any

import psycopg2
import psycopg2.pool
import psycopg2.extras

logger = logging.getLogger(__name__)

_pool: psycopg2.pool.ThreadedConnectionPool | None = None


def _dsn() -> str:
    return (
        f"host={os.getenv('POSTGRES_HOST','postgres')} "
        f"port={os.getenv('POSTGRES_PORT','5432')} "
        f"dbname={os.getenv('POSTGRES_DB','sre_agent')} "
        f"user={os.getenv('POSTGRES_USER','sre_user')} "
        f"password={os.getenv('POSTGRES_PASSWORD','sre_password')}"
    )


def init_pool(min_conn: int = 1, max_conn: int = 10, max_retries: int = 20) -> None:
    global _pool
    for attempt in range(1, max_retries + 1):
        try:
            _pool = psycopg2.pool.ThreadedConnectionPool(min_conn, max_conn, _dsn())
            logger.info("Postgres pool initialized.")
            return
        except Exception as exc:
            logger.warning("Postgres not ready (attempt %d/%d): %s", attempt, max_retries, exc)
            time.sleep(3)
    raise RuntimeError("Postgres never became available.")


@contextmanager
def get_conn():
    if _pool is None:
        init_pool()
    conn = _pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _pool.putconn(conn)


def execute(sql: str, params: tuple = ()) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)


def fetch_one(sql: str, params: tuple = ()) -> dict | None:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return dict(row) if row else None


def fetch_all(sql: str, params: tuple = ()) -> list[dict]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]


def insert_incident(incident_id: str, data: dict) -> None:
    execute(
        """
        INSERT INTO incidents (id, severity, anomaly_type, affected_services, status,
            detection_time, anomaly_score, trigger_metric, observed_value, baseline_value,
            deviation_sigma, description)
        VALUES (%s, %s, %s, %s, %s, to_timestamp(%s / 1000.0), %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING
        """,
        (
            incident_id,
            data.get("severity", "HIGH"),
            data.get("anomaly_type", "UNKNOWN"),
            data.get("affected_services", []),
            "DETECTING",
            data.get("detection_time", 0),
            data.get("anomaly_score", 0.0),
            data.get("trigger_metric", ""),
            data.get("observed_value", 0.0),
            data.get("baseline_value", 0.0),
            data.get("deviation_sigma", 0.0),
            data.get("description", ""),
        ),
    )


def update_incident(incident_id: str, updates: dict) -> None:
    if not updates:
        return
    allowed = {
        "status", "resolution_time", "correlation_context", "blast_radius",
        "rca_candidates", "top_root_cause", "rca_confidence",
        "remediation_plan", "postmortem",
    }
    set_parts = []
    clean_vals = []
    for k, v in updates.items():
        if k not in allowed:
            continue
        if k == "resolution_time":
            set_parts.append("resolution_time = to_timestamp(%s / 1000.0)")
        elif isinstance(v, (dict, list)):
            set_parts.append(f"{k} = %s::jsonb")
        else:
            set_parts.append(f"{k} = %s")
        clean_vals.append(json.dumps(v) if isinstance(v, (dict, list)) else v)

    if not set_parts:
        return

    clean_vals.append(incident_id)
    execute(
        f"UPDATE incidents SET {', '.join(set_parts)} WHERE id = %s",
        tuple(clean_vals),
    )


def log_agent_event(incident_id: str | None, agent_name: str, event_type: str, payload: dict) -> None:
    execute(
        """
        INSERT INTO agent_events (incident_id, agent_name, event_type, payload)
        VALUES (%s, %s, %s, %s::jsonb)
        """,
        (incident_id, agent_name, event_type, json.dumps(payload)),
    )


def log_email(incident_id: str, notification_type: str, recipients: list[str],
              subject: str, body_html: str, status: str) -> None:
    execute(
        """
        INSERT INTO email_notifications
            (incident_id, notification_type, recipients, subject, body_html, status, sent_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        """,
        (incident_id, notification_type, recipients, subject, body_html, status),
    )
