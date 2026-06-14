-- Incident Response SRE Agent — Database Schema

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- ─── Core incident table ────────────────────────────────────────────────────

CREATE TABLE incidents (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    severity            VARCHAR(20) NOT NULL CHECK (severity IN ('CRITICAL','HIGH','MEDIUM','LOW')),
    anomaly_type        VARCHAR(60) NOT NULL,
    affected_services   TEXT[] NOT NULL DEFAULT '{}',
    status              VARCHAR(30) NOT NULL DEFAULT 'DETECTING'
                        CHECK (status IN ('DETECTING','CORRELATING','INVESTIGATING',
                                          'RCA_COMPLETE','REMEDIATING','RESOLVED')),
    detection_time      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolution_time     TIMESTAMPTZ,
    mttr_minutes        INTEGER GENERATED ALWAYS AS (
                            CASE WHEN resolution_time IS NOT NULL
                            THEN EXTRACT(EPOCH FROM (resolution_time - detection_time))::INTEGER / 60
                            ELSE NULL END
                        ) STORED,
    anomaly_score       FLOAT,
    trigger_metric      TEXT,
    observed_value      FLOAT,
    baseline_value      FLOAT,
    deviation_sigma     FLOAT,
    description         TEXT,
    correlation_context JSONB,
    blast_radius        JSONB,
    rca_candidates      JSONB,
    top_root_cause      TEXT,
    rca_confidence      FLOAT,
    remediation_plan    JSONB,
    postmortem          TEXT,
    raw_event_id        UUID,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_incidents_status ON incidents (status);
CREATE INDEX idx_incidents_severity ON incidents (severity);
CREATE INDEX idx_incidents_detection_time ON incidents (detection_time DESC);
CREATE INDEX idx_incidents_affected_services ON incidents USING GIN (affected_services);

-- ─── Agent audit log ────────────────────────────────────────────────────────

CREATE TABLE agent_events (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    incident_id UUID REFERENCES incidents(id) ON DELETE CASCADE,
    agent_name  VARCHAR(50) NOT NULL,
    event_type  VARCHAR(80) NOT NULL,
    payload     JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_agent_events_incident ON agent_events (incident_id);
CREATE INDEX idx_agent_events_agent ON agent_events (agent_name);
CREATE INDEX idx_agent_events_created ON agent_events (created_at DESC);

-- ─── Email notifications ────────────────────────────────────────────────────

CREATE TABLE email_notifications (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    incident_id         UUID REFERENCES incidents(id) ON DELETE CASCADE,
    notification_type   VARCHAR(50) NOT NULL,
    recipients          TEXT[] NOT NULL DEFAULT '{}',
    subject             TEXT NOT NULL,
    body_html           TEXT,
    body_text           TEXT,
    status              VARCHAR(20) NOT NULL DEFAULT 'PENDING'
                        CHECK (status IN ('PENDING','SENT','FAILED')),
    sent_at             TIMESTAMPTZ,
    error_message       TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_emails_incident ON email_notifications (incident_id);
CREATE INDEX idx_emails_created ON email_notifications (created_at DESC);

-- ─── Metric snapshots (for resolution monitoring) ─────────────────────────

CREATE TABLE metric_snapshots (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    incident_id     UUID REFERENCES incidents(id) ON DELETE CASCADE,
    service_name    TEXT NOT NULL,
    metric_name     TEXT NOT NULL,
    metric_value    FLOAT NOT NULL,
    anomaly_score   FLOAT,
    snapshot_time   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_snapshots_incident ON metric_snapshots (incident_id, snapshot_time DESC);

-- ─── Updated-at trigger ────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER incidents_updated_at
    BEFORE UPDATE ON incidents
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- ─── Service registry (static seed data) ──────────────────────────────────

CREATE TABLE service_registry (
    service_name        TEXT PRIMARY KEY,
    team_owner          TEXT NOT NULL,
    criticality         VARCHAR(5) NOT NULL DEFAULT 'P1',
    sla_p99_latency_ms  INTEGER DEFAULT 500,
    sla_error_rate_pct  FLOAT DEFAULT 1.0,
    on_call_rotation    TEXT,
    slack_channel       TEXT,
    upstream_services   TEXT[] DEFAULT '{}',
    downstream_services TEXT[] DEFAULT '{}',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO service_registry (service_name, team_owner, criticality, sla_p99_latency_ms, sla_error_rate_pct, on_call_rotation, slack_channel, downstream_services) VALUES
('api-gateway',           'platform-team',  'P0', 200,  0.5, 'platform-oncall',  '#platform-alerts',  ARRAY['payment-service','order-service','user-service']),
('payment-service',       'payments-team',  'P0', 500,  0.1, 'payments-oncall',  '#payments-alerts',  ARRAY['postgres','redis']),
('order-service',         'commerce-team',  'P0', 400,  0.5, 'commerce-oncall',  '#commerce-alerts',  ARRAY['payment-service','inventory-service','kafka']),
('user-service',          'identity-team',  'P1', 300,  0.5, 'identity-oncall',  '#identity-alerts',  ARRAY['postgres','redis']),
('notification-service',  'platform-team',  'P1', 1000, 1.0, 'platform-oncall',  '#platform-alerts',  ARRAY['kafka']),
('inventory-service',     'commerce-team',  'P1', 400,  1.0, 'commerce-oncall',  '#commerce-alerts',  ARRAY['postgres']);
