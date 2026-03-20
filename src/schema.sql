-- Core event store table
CREATE TABLE IF NOT EXISTS events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    stream_id TEXT NOT NULL,
    stream_position BIGINT NOT NULL,
    global_position BIGINT GENERATED ALWAYS AS IDENTITY,
    event_type TEXT NOT NULL,
    event_version SMALLINT NOT NULL DEFAULT 1,
    payload JSONB NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_stream_position UNIQUE (stream_id, stream_position)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_events_stream ON events (stream_id, stream_position);
CREATE INDEX IF NOT EXISTS idx_events_global ON events (global_position);
CREATE INDEX IF NOT EXISTS idx_events_type ON events (event_type);
CREATE INDEX IF NOT EXISTS idx_events_recorded ON events (recorded_at);

-- Stream metadata table
CREATE TABLE IF NOT EXISTS event_streams (
    stream_id TEXT PRIMARY KEY,
    aggregate_type TEXT NOT NULL,
    current_version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    archived_at TIMESTAMPTZ,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- Projection checkpoints table
CREATE TABLE IF NOT EXISTS projection_checkpoints (
    projection_name TEXT PRIMARY KEY,
    last_position BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Outbox table for guaranteed delivery
CREATE TABLE IF NOT EXISTS outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id UUID NOT NULL REFERENCES events(event_id),
    destination TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    attempts SMALLINT NOT NULL DEFAULT 0
);

-- Application summary projection table
CREATE TABLE IF NOT EXISTS application_summary (
    application_id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    applicant_id TEXT,
    requested_amount NUMERIC,
    approved_amount NUMERIC,
    credit_score NUMERIC,
    final_decision TEXT,
    updated_at TIMESTAMPTZ NOT NULL
);

-- Compliance audit view table
CREATE TABLE IF NOT EXISTS compliance_audit (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    application_id TEXT NOT NULL,
    check_type TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    passed BOOLEAN NOT NULL,
    checked_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    as_of_date DATE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_compliance_application ON compliance_audit (application_id, as_of_date);

-- Agent performance ledger table
CREATE TABLE IF NOT EXISTS agent_performance (
    agent_id TEXT NOT NULL,
    model_version TEXT NOT NULL,
    date DATE NOT NULL,
    decisions_count INTEGER DEFAULT 0,
    avg_confidence NUMERIC,
    PRIMARY KEY (agent_id, model_version, date)
);

-- Audit integrity chain table
CREATE TABLE IF NOT EXISTS audit_integrity_checks (
    check_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    previous_hash TEXT,
    current_hash TEXT NOT NULL,
    events_verified INTEGER NOT NULL,
    chain_valid BOOLEAN NOT NULL,
    tamper_detected BOOLEAN NOT NULL,
    checked_at TIMESTAMPTZ NOT NULL
);