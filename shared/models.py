"""Shared Pydantic event models for all SRE Agent services."""
from __future__ import annotations
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from pydantic import BaseModel, Field


def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def new_uuid() -> str:
    return str(uuid.uuid4())


# ─── Enums ────────────────────────────────────────────────────────────────────

class EventType(str, Enum):
    METRIC     = "METRIC"
    LOG        = "LOG"
    DEPLOYMENT = "DEPLOYMENT"

class Severity(str, Enum):
    CRITICAL = "CRITICAL"
    HIGH     = "HIGH"
    MEDIUM   = "MEDIUM"
    LOW      = "LOW"

class AnomalyType(str, Enum):
    LATENCY_SPIKE          = "LATENCY_SPIKE"
    ERROR_RATE_SPIKE       = "ERROR_RATE_SPIKE"
    CPU_SATURATION         = "CPU_SATURATION"
    MEMORY_LEAK            = "MEMORY_LEAK"
    DB_CONNECTION_EXHAUSTION = "DB_CONNECTION_EXHAUSTION"
    KAFKA_CONSUMER_LAG     = "KAFKA_CONSUMER_LAG"
    DEPLOYMENT_FAILURE     = "DEPLOYMENT_FAILURE"
    DEPENDENCY_OUTAGE      = "DEPENDENCY_OUTAGE"
    UNKNOWN                = "UNKNOWN"

class IncidentStatus(str, Enum):
    DETECTING    = "DETECTING"
    CORRELATING  = "CORRELATING"
    INVESTIGATING = "INVESTIGATING"
    RCA_COMPLETE = "RCA_COMPLETE"
    REMEDIATING  = "REMEDIATING"
    RESOLVED     = "RESOLVED"


# ─── Raw telemetry events ─────────────────────────────────────────────────────

class RawMetricEvent(BaseModel):
    event_id:       str   = Field(default_factory=new_uuid)
    event_type:     str   = EventType.METRIC
    source_service: str
    environment:    str   = "production"
    timestamp:      int   = Field(default_factory=now_ms)
    metric_name:    str
    metric_value:   float
    labels:         dict[str, str] = {}

class RawLogEvent(BaseModel):
    event_id:       str   = Field(default_factory=new_uuid)
    event_type:     str   = EventType.LOG
    source_service: str
    environment:    str   = "production"
    timestamp:      int   = Field(default_factory=now_ms)
    level:          str   = "INFO"
    message:        str
    trace_id:       str   = Field(default_factory=new_uuid)
    labels:         dict[str, str] = {}

class RawDeploymentEvent(BaseModel):
    event_id:       str   = Field(default_factory=new_uuid)
    event_type:     str   = EventType.DEPLOYMENT
    source_service: str
    environment:    str   = "production"
    timestamp:      int   = Field(default_factory=now_ms)
    version:        str
    deployed_by:    str   = "ci-cd-pipeline"
    change_type:    str   = "CODE"
    git_sha:        str   = ""
    description:    str   = ""
    known_risks:    list[str] = []


# ─── Agent events ─────────────────────────────────────────────────────────────

class AnomalyDetectedEvent(BaseModel):
    event_id:          str         = Field(default_factory=new_uuid)
    incident_id:       str         = Field(default_factory=new_uuid)
    anomaly_type:      str         = AnomalyType.UNKNOWN
    severity:          str         = Severity.HIGH
    affected_services: list[str]   = []
    detection_time:    int         = Field(default_factory=now_ms)
    trigger_metric:    str         = ""
    observed_value:    float       = 0.0
    baseline_value:    float       = 0.0
    deviation_sigma:   float       = 0.0
    anomaly_score:     float       = 0.0
    description:       str         = ""
    raw_events:        list[str]   = []

class CorrelationSignal(BaseModel):
    signal_type:  str
    strength:     float
    description:  str
    evidence:     list[str] = []

class IncidentOpenedEvent(BaseModel):
    event_id:             str                  = Field(default_factory=new_uuid)
    incident_id:          str
    anomaly_type:         str
    severity:             str
    affected_services:    list[str]            = []
    detection_time:       int
    opened_at:            int                  = Field(default_factory=now_ms)
    correlation_signals:  list[CorrelationSignal] = []
    blast_radius:         dict[str, Any]       = {}
    deployment_context:   dict[str, Any] | None = None
    recent_metrics:       dict[str, list[float]] = {}
    recent_errors:        list[str]            = []
    description:          str                  = ""

class RootCauseCandidate(BaseModel):
    rank:              int
    hypothesis:        str
    confidence:        float
    evidence:          list[str]   = []
    similar_incidents: list[str]   = []
    runbook_refs:      list[str]   = []

class RCACompletedEvent(BaseModel):
    event_id:            str                    = Field(default_factory=new_uuid)
    incident_id:         str
    rca_id:              str                    = Field(default_factory=new_uuid)
    generated_at:        int                    = Field(default_factory=now_ms)
    root_cause_candidates: list[RootCauseCandidate] = []
    top_root_cause:      str                    = ""
    top_confidence:      float                  = 0.0
    blast_radius:        dict[str, Any]         = {}
    anomaly_type:        str                    = ""
    affected_services:   list[str]              = []
    severity:            str                    = Severity.HIGH

class RemediationStep(BaseModel):
    step_id:          int
    priority:         str
    action:           str
    rationale:        str
    risk_level:       str  = "LOW"
    rollback:         str  = ""
    owner:            str  = ""
    expected_outcome: str  = ""

class RemediationPlanEvent(BaseModel):
    event_id:                   str                  = Field(default_factory=new_uuid)
    incident_id:                str
    root_cause:                 str
    confidence:                 float
    action_steps:               list[RemediationStep] = []
    escalation_path:            list[str]            = []
    runbook_references:         list[str]            = []
    estimated_resolution_time:  str                  = "Unknown"
    generated_at:               int                  = Field(default_factory=now_ms)
    anomaly_type:               str                  = ""
    affected_services:          list[str]            = []
    severity:                   str                  = Severity.HIGH

class IncidentResolvedEvent(BaseModel):
    event_id:          str   = Field(default_factory=new_uuid)
    incident_id:       str
    resolved_at:       int   = Field(default_factory=now_ms)
    detection_time:    int
    mttr_minutes:      float = 0.0
    resolution_method: str   = "AUTOMATIC_RECOVERY"
    top_root_cause:    str   = ""
    anomaly_type:      str   = ""
    affected_services: list[str] = []
    severity:          str   = Severity.HIGH

class PostmortemGeneratedEvent(BaseModel):
    event_id:      str   = Field(default_factory=new_uuid)
    incident_id:   str
    generated_at:  int   = Field(default_factory=now_ms)
    postmortem:    str   = ""
    mttr_minutes:  float = 0.0
    severity:      str   = Severity.HIGH
    anomaly_type:  str   = ""
    affected_services: list[str] = []
