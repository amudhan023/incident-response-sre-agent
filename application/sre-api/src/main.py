"""SRE Agent API — REST + WebSocket + HTML Dashboard."""
from __future__ import annotations
import asyncio
import json
import logging
import os
import sys
import threading
from datetime import datetime, timezone
from typing import Any

sys.path.insert(0, "/app")

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader, select_autoescape

sys.path.insert(0, "/app")
from shared.db_client import init_pool, fetch_all, fetch_one
from shared.kafka_client import make_consumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("sre-api")

app = FastAPI(title="SRE Agent API", version="1.0.0")

jinja = Environment(
    loader=FileSystemLoader("/app/src/templates"),
    autoescape=select_autoescape(["html"]),
)

# WebSocket connection manager
_ws_clients: set[WebSocket] = set()
_ws_lock = asyncio.Lock()
_live_events: list[dict] = []  # ring buffer of recent events


@app.on_event("startup")
async def startup():
    init_pool()
    # Start background Kafka listener for live events
    t = threading.Thread(target=_kafka_event_listener, daemon=True)
    t.start()


async def _broadcast(event: dict) -> None:
    _live_events.append(event)
    if len(_live_events) > 200:
        _live_events.pop(0)

    dead = set()
    for ws in _ws_clients.copy():
        try:
            await ws.send_text(json.dumps(event))
        except Exception:
            dead.add(ws)
    for ws in dead:
        _ws_clients.discard(ws)


def _kafka_event_listener() -> None:
    """Background thread: relay lifecycle events to WebSocket clients."""
    TOPICS = ["anomalies.detected", "incidents.opened", "rca.completed",
              "remediation.plans", "incidents.resolved", "postmortems.generated"]
    try:
        consumer = make_consumer(topics=TOPICS, group_id="sre-api-ws")
        while True:
            msg = consumer.poll(timeout=0.5)
            if msg is None:
                continue
            if msg.error():
                continue
            try:
                raw = json.loads(msg.value().decode("utf-8"))
                event_type = _classify_event(raw, msg.topic())
                live_event = {
                    "topic": msg.topic(),
                    "event_type": event_type,
                    "incident_id": raw.get("incident_id", ""),
                    "data": raw,
                    "received_at": int(datetime.now(timezone.utc).timestamp() * 1000),
                }
                asyncio.run_coroutine_threadsafe(_broadcast(live_event), asyncio.get_event_loop())
            except Exception as exc:
                logger.debug("WS relay error: %s", exc)
    except Exception as exc:
        logger.warning("Kafka WS listener failed: %s", exc)


def _classify_event(raw: dict, topic: str) -> str:
    if topic == "anomalies.detected":
        return "ANOMALY_DETECTED"
    if topic == "incidents.opened":
        return "INCIDENT_OPENED"
    if topic == "rca.completed":
        return "RCA_COMPLETED"
    if topic == "remediation.plans":
        return "REMEDIATION_PLAN"
    if topic == "incidents.resolved":
        return "INCIDENT_RESOLVED"
    if topic == "postmortems.generated":
        return "POSTMORTEM_GENERATED"
    return "UNKNOWN"


def _fmt_ts(ts) -> str:
    if ts is None:
        return "—"
    if isinstance(ts, datetime):
        return ts.strftime("%Y-%m-%d %H:%M:%S UTC")
    return str(ts)


# ─── REST Endpoints ────────────────────────────────────────────────────────────

@app.get("/api/incidents")
def list_incidents(limit: int = 50) -> list[dict]:
    rows = fetch_all(
        "SELECT id, severity, anomaly_type, affected_services, status, "
        "detection_time, resolution_time, mttr_minutes, top_root_cause, rca_confidence, "
        "anomaly_score, description, created_at "
        "FROM incidents ORDER BY detection_time DESC LIMIT %s",
        (limit,),
    )
    for r in rows:
        r["detection_time"] = _fmt_ts(r.get("detection_time"))
        r["resolution_time"] = _fmt_ts(r.get("resolution_time"))
        r["created_at"] = _fmt_ts(r.get("created_at"))
    return rows


@app.get("/api/incidents/{incident_id}")
def get_incident(incident_id: str) -> dict:
    row = fetch_one("SELECT * FROM incidents WHERE id = %s", (incident_id,))
    if not row:
        return {"error": "Not found"}
    events = fetch_all(
        "SELECT agent_name, event_type, payload, created_at FROM agent_events "
        "WHERE incident_id = %s ORDER BY created_at ASC",
        (incident_id,),
    )
    emails = fetch_all(
        "SELECT notification_type, recipients, subject, status, sent_at FROM email_notifications "
        "WHERE incident_id = %s ORDER BY created_at ASC",
        (incident_id,),
    )
    row["agent_events"] = [
        {**e, "created_at": _fmt_ts(e.get("created_at"))} for e in events
    ]
    row["emails"] = [
        {**e, "sent_at": _fmt_ts(e.get("sent_at"))} for e in emails
    ]
    for k in ("detection_time", "resolution_time", "created_at", "updated_at"):
        row[k] = _fmt_ts(row.get(k))
    return row


@app.get("/api/agent-events")
def list_agent_events(limit: int = 100) -> list[dict]:
    rows = fetch_all(
        "SELECT incident_id, agent_name, event_type, created_at FROM agent_events "
        "ORDER BY created_at DESC LIMIT %s",
        (limit,),
    )
    for r in rows:
        r["created_at"] = _fmt_ts(r.get("created_at"))
    return rows


@app.get("/api/emails")
def list_emails(limit: int = 50) -> list[dict]:
    rows = fetch_all(
        "SELECT incident_id, notification_type, recipients, subject, status, sent_at, created_at "
        "FROM email_notifications ORDER BY created_at DESC LIMIT %s",
        (limit,),
    )
    for r in rows:
        r["sent_at"] = _fmt_ts(r.get("sent_at"))
        r["created_at"] = _fmt_ts(r.get("created_at"))
    return rows


@app.get("/api/live-events")
def get_live_events() -> list[dict]:
    return list(reversed(_live_events[-50:]))


@app.get("/api/stats")
def get_stats() -> dict:
    rows = fetch_all(
        "SELECT status, severity, COUNT(*) as count FROM incidents "
        "GROUP BY status, severity"
    )
    total = fetch_one("SELECT COUNT(*) as total FROM incidents")
    resolved = fetch_one("SELECT AVG(mttr_minutes) as avg_mttr FROM incidents WHERE status='RESOLVED'")
    return {
        "total_incidents": total["total"] if total else 0,
        "avg_mttr_minutes": round(resolved["avg_mttr"] or 0, 1) if resolved else 0,
        "by_status": rows,
    }


@app.websocket("/ws/live")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    _ws_clients.add(websocket)
    # Send recent events on connect
    for evt in list(reversed(_live_events[-20:])):
        try:
            await websocket.send_text(json.dumps(evt))
        except Exception:
            break
    try:
        while True:
            await websocket.receive_text()  # keep alive
    except WebSocketDisconnect:
        _ws_clients.discard(websocket)


# ─── HTML Dashboard ───────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
def dashboard():
    tmpl = jinja.get_template("dashboard.html")
    return HTMLResponse(tmpl.render())


@app.get("/health")
def health():
    return {"status": "healthy", "service": "sre-api"}
