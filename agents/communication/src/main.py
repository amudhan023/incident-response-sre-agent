"""
Communication Agent — sends email notifications at every incident lifecycle stage.

Subscribes to: anomalies.detected, rca.completed, remediation.plans,
               incidents.resolved, postmortems.generated

Sends structured HTML emails via SMTP (Mailhog in demo).
"""
from __future__ import annotations
import json
import logging
import os
import smtplib
import sys
import time
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

sys.path.insert(0, "/app")
from shared.models import (
    AnomalyDetectedEvent, RCACompletedEvent,
    RemediationPlanEvent, IncidentResolvedEvent, PostmortemGeneratedEvent,
)
from shared.kafka_client import make_consumer, consume_loop
from shared.db_client import init_pool, log_email, log_agent_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("communication-agent")

SMTP_HOST   = os.getenv("SMTP_HOST", "mailhog")
SMTP_PORT   = int(os.getenv("SMTP_PORT", "1025"))
EMAIL_FROM  = os.getenv("EMAIL_FROM", "sre-agent@company.com")
ONCALL      = os.getenv("EMAIL_ONCALL", "oncall@company.com")
TEAM        = os.getenv("EMAIL_TEAM", "engineering@company.com")
MANAGEMENT  = os.getenv("EMAIL_MANAGEMENT", "management@company.com")

TEMPLATES_DIR = Path("/app/templates")

jinja = Environment(
    loader=FileSystemLoader(str(TEMPLATES_DIR)),
    autoescape=select_autoescape(["html"]),
)


def ts_to_str(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


def send_email(to: list[str], subject: str, html: str, incident_id: str, notification_type: str) -> None:
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = EMAIL_FROM
    msg["To"]      = ", ".join(to)

    text_part = MIMEText(html.replace("<br>", "\n").replace("</p>", "\n"), "plain")
    html_part = MIMEText(html, "html")
    msg.attach(text_part)
    msg.attach(html_part)

    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10) as server:
                server.sendmail(EMAIL_FROM, to, msg.as_string())
            logger.info("📧 Sent %s email for %s to %s", notification_type, incident_id, to)

            try:
                log_email(incident_id, notification_type, to, subject, html, "SENT")
                log_agent_event(
                    incident_id, "communication-agent", f"EMAIL_SENT_{notification_type}",
                    {"recipients": to, "subject": subject[:80]},
                )
            except Exception as exc:
                logger.warning("Could not log email to DB: %s", exc)
            return
        except Exception as exc:
            logger.warning("Email send failed (attempt %d): %s", attempt, exc)
            if attempt < max_retries:
                time.sleep(2)

    logger.error("Failed to send %s email after %d attempts", notification_type, max_retries)
    try:
        log_email(incident_id, notification_type, to, subject, html, "FAILED")
    except Exception:
        pass


# ─── Handlers per event type ───────────────────────────────────────────────────

def handle_anomaly_detected(raw: dict) -> None:
    try:
        evt = AnomalyDetectedEvent(**raw)
    except Exception as exc:
        logger.error("Invalid anomaly event: %s", exc)
        return

    if evt.severity not in ("CRITICAL", "HIGH"):
        return  # Only notify on high severity

    recipients = [ONCALL, TEAM]
    if evt.severity == "CRITICAL":
        recipients.append(MANAGEMENT)

    try:
        tmpl = jinja.get_template("incident_opened.html")
        html = tmpl.render(
            incident_id=evt.incident_id[:8].upper(),
            severity=evt.severity,
            anomaly_type=evt.anomaly_type.replace("_", " ").title(),
            service=", ".join(evt.affected_services),
            detection_time=ts_to_str(evt.detection_time),
            description=evt.description,
            trigger_metric=evt.trigger_metric,
            observed_value=f"{evt.observed_value:.1f}",
            baseline_value=f"{evt.baseline_value:.1f}",
            deviation=f"{evt.deviation_sigma:.1f}σ",
            anomaly_score=f"{evt.anomaly_score:.0%}",
        )
    except Exception as exc:
        logger.error("Template render failed: %s", exc)
        html = f"<p>Incident {evt.incident_id} — {evt.anomaly_type} on {', '.join(evt.affected_services)}</p>"

    severity_emoji = "🚨" if evt.severity == "CRITICAL" else "⚠️"
    subject = (
        f"{severity_emoji} [{evt.severity}] INC-{evt.incident_id[:8].upper()} — "
        f"{evt.anomaly_type.replace('_',' ')} on {', '.join(evt.affected_services)}"
    )
    send_email(recipients, subject, html, evt.incident_id, "INCIDENT_OPENED")


def handle_rca_completed(raw: dict) -> None:
    try:
        evt = RCACompletedEvent(**raw)
    except Exception as exc:
        logger.error("Invalid RCA event: %s", exc)
        return

    recipients = [ONCALL, TEAM]
    top = evt.root_cause_candidates[0] if evt.root_cause_candidates else None

    try:
        tmpl = jinja.get_template("rca_available.html")
        html = tmpl.render(
            incident_id=evt.incident_id[:8].upper(),
            severity=evt.severity,
            anomaly_type=evt.anomaly_type.replace("_", " ").title(),
            service=", ".join(evt.affected_services),
            top_root_cause=evt.top_root_cause,
            confidence=f"{evt.top_confidence:.0%}",
            evidence=top.evidence if top else [],
            all_candidates=evt.root_cause_candidates[:3],
            user_impact=evt.blast_radius.get("estimated_user_impact", "Unknown"),
        )
    except Exception as exc:
        logger.error("Template render failed: %s", exc)
        html = f"<p>RCA for {evt.incident_id}: {evt.top_root_cause}</p>"

    subject = (
        f"🔍 [INC-{evt.incident_id[:8].upper()}] Root Cause Analysis — "
        f"{evt.top_confidence:.0%} Confidence"
    )
    send_email(recipients, subject, html, evt.incident_id, "RCA_AVAILABLE")


def handle_remediation_plan(raw: dict) -> None:
    try:
        evt = RemediationPlanEvent(**raw)
    except Exception as exc:
        logger.error("Invalid remediation event: %s", exc)
        return

    try:
        tmpl = jinja.get_template("remediation_plan.html")
        immediate_steps  = [s for s in evt.action_steps if s.priority == "IMMEDIATE"]
        shortterm_steps  = [s for s in evt.action_steps if s.priority == "WITHIN_15MIN"]
        longerterm_steps = [s for s in evt.action_steps if s.priority == "WITHIN_1HOUR"]
        html = tmpl.render(
            incident_id=evt.incident_id[:8].upper(),
            root_cause=evt.root_cause,
            confidence=f"{evt.confidence:.0%}",
            estimated_time=evt.estimated_resolution_time,
            immediate_steps=immediate_steps,
            shortterm_steps=shortterm_steps,
            longerterm_steps=longerterm_steps,
            escalation_path=evt.escalation_path,
            runbook_refs=evt.runbook_references,
            service=", ".join(evt.affected_services),
        )
    except Exception as exc:
        logger.error("Template render failed: %s", exc)
        html = f"<p>Remediation plan for {evt.incident_id}: {len(evt.action_steps)} steps</p>"

    subject = f"🛠️ [INC-{evt.incident_id[:8].upper()}] Remediation Plan Ready — {len(evt.action_steps)} steps"
    send_email([ONCALL], subject, html, evt.incident_id, "REMEDIATION_PLAN")


def handle_incident_resolved(raw: dict) -> None:
    try:
        evt = IncidentResolvedEvent(**raw)
    except Exception as exc:
        logger.error("Invalid resolved event: %s", exc)
        return

    try:
        tmpl = jinja.get_template("incident_resolved.html")
        html = tmpl.render(
            incident_id=evt.incident_id[:8].upper(),
            severity=evt.severity,
            anomaly_type=evt.anomaly_type.replace("_", " ").title(),
            service=", ".join(evt.affected_services),
            resolution_time=ts_to_str(evt.resolved_at),
            mttr=f"{evt.mttr_minutes:.0f}",
            root_cause=evt.top_root_cause,
            resolution_method=evt.resolution_method.replace("_", " ").title(),
        )
    except Exception as exc:
        logger.error("Template render failed: %s", exc)
        html = f"<p>Incident {evt.incident_id} resolved in {evt.mttr_minutes:.0f} minutes</p>"

    subject = f"✅ [INC-{evt.incident_id[:8].upper()}] RESOLVED — {evt.anomaly_type.replace('_',' ')} — MTTR {evt.mttr_minutes:.0f}min"
    send_email([ONCALL, TEAM, MANAGEMENT], subject, html, evt.incident_id, "INCIDENT_RESOLVED")


def handle_postmortem(raw: dict) -> None:
    try:
        evt = PostmortemGeneratedEvent(**raw)
    except Exception as exc:
        logger.error("Invalid postmortem event: %s", exc)
        return

    try:
        tmpl = jinja.get_template("postmortem_ready.html")
        html = tmpl.render(
            incident_id=evt.incident_id[:8].upper(),
            severity=evt.severity,
            anomaly_type=evt.anomaly_type.replace("_", " ").title(),
            service=", ".join(evt.affected_services),
            mttr=f"{evt.mttr_minutes:.0f}",
            postmortem_preview=evt.postmortem[:1200],
        )
    except Exception as exc:
        logger.error("Template render failed: %s", exc)
        html = f"<p>Postmortem ready for {evt.incident_id}</p>"

    subject = f"📋 [INC-{evt.incident_id[:8].upper()}] Postmortem Ready — {evt.anomaly_type.replace('_',' ')} on {', '.join(evt.affected_services)}"
    send_email([ONCALL, TEAM, MANAGEMENT], subject, html, evt.incident_id, "POSTMORTEM_READY")


DISPATCH_MAP = {
    # Keyed by distinguishing field combinations
}


def dispatch(raw: dict) -> None:
    """Route incoming Kafka messages to the right handler."""
    # Detect event type by presence of unique fields
    if "top_root_cause" in raw and "root_cause_candidates" in raw:
        handle_rca_completed(raw)
    elif "action_steps" in raw and "root_cause" in raw:
        handle_remediation_plan(raw)
    elif "postmortem" in raw and "mttr_minutes" in raw:
        handle_postmortem(raw)
    elif "resolution_method" in raw:
        handle_incident_resolved(raw)
    elif "anomaly_score" in raw and "trigger_metric" in raw:
        handle_anomaly_detected(raw)


if __name__ == "__main__":
    logger.info("Communication Agent starting...")
    init_pool()

    consumer = make_consumer(
        topics=[
            "anomalies.detected",
            "rca.completed",
            "remediation.plans",
            "incidents.resolved",
            "postmortems.generated",
        ],
        group_id="communication-agent",
    )
    logger.info("Communication Agent ready. Watching all lifecycle topics...")
    consume_loop(consumer, dispatch)
