-- Production schema migration (psql -f).  Mirrors app/models.py exactly.
-- Run inside one transaction so the deploy is atomic.

BEGIN;

CREATE TABLE IF NOT EXISTS outbox_messages (
    id              UUID PRIMARY KEY,
    aggregate_type  VARCHAR(40)  NOT NULL,
    aggregate_id    VARCHAR(36)  NOT NULL,
    event_type      VARCHAR(80)  NOT NULL,
    payload         JSONB        NOT NULL,             -- JSONB: queryable DLQ triage
    status          VARCHAR(12)  NOT NULL DEFAULT 'pending',
    attempts        INT          NOT NULL DEFAULT 0,
    max_attempts    INT          NOT NULL DEFAULT 10,
    next_attempt_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    claimed_at      TIMESTAMPTZ,
    claimed_by      VARCHAR(64),
    published_at    TIMESTAMPTZ,
    last_error      TEXT,
    traceparent     VARCHAR(64),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT chk_outbox_status CHECK (status IN ('pending','in_flight','published','dead'))
);

-- Partial index: the relay ONLY scans due+pending rows. Published history
-- stays in the table (or gets archived) without bloating the hot scan path.
CREATE INDEX IF NOT EXISTS ix_outbox_claim
    ON outbox_messages (next_attempt_at, created_at)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS ix_outbox_aggregate
    ON outbox_messages (aggregate_type, aggregate_id);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    id                  BIGSERIAL PRIMARY KEY,
    scope               VARCHAR(255) NOT NULL,
    key                 VARCHAR(255) NOT NULL,
    request_fingerprint VARCHAR(64)  NOT NULL,
    response_status     INT,
    response_body       JSONB,
    completed           BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uq_idempotency_scope_key UNIQUE (scope, key)
);

COMMIT;

-- Retention hygiene (schedule via pg_cron or external cron):
-- DELETE FROM outbox_messages WHERE status='published' AND published_at < now() - interval '7 days';
