-- Oraklion Mentor persistence — AuditLog + RateLimiter DDL (BRIEF 2026-08-03).
-- Idempotent. Trygg å kjøre flere ganger.
-- Alle tabeller lever under oraklion-schemaet slik at public/ ikke forurenses.

CREATE SCHEMA IF NOT EXISTS oraklion;

-- 1) mentor_log — audit av hver mentor-interaksjon.
--    user_input og response er PII-maskerte ved skrivetidspunkt.
CREATE TABLE IF NOT EXISTS oraklion.mentor_log (
    id                bigserial   PRIMARY KEY,
    session_id        text        NOT NULL,
    created_at        timestamptz NOT NULL DEFAULT NOW(),
    user_input_masked text        NOT NULL,
    response_masked   text,
    verdict           text        NOT NULL
        CHECK (verdict IN ('ok','blocked_input','blocked_output',
                           'rate_limited','upstream_error')),
    reason            text,
    detail            text,
    latency_ms        integer
);

CREATE INDEX IF NOT EXISTS idx_mentor_log_created_at
    ON oraklion.mentor_log (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_mentor_log_session
    ON oraklion.mentor_log (session_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_mentor_log_verdict
    ON oraklion.mentor_log (verdict);

-- 2) audit_log — generell audit for oraklion-side. (Speiler
--    sniper_killswitch_audit-mønsteret men uten CHECK på action.)
CREATE TABLE IF NOT EXISTS oraklion.audit_log (
    id            bigserial   PRIMARY KEY,
    changed_at    timestamptz NOT NULL DEFAULT NOW(),
    subsystem     text        NOT NULL,
    action        text        NOT NULL,
    actor         text        NOT NULL,
    reason        text        NOT NULL
        CHECK (length(reason) >= 5),
    payload       jsonb       NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_audit_log_changed_at
    ON oraklion.audit_log (changed_at DESC);

CREATE INDEX IF NOT EXISTS idx_audit_log_subsystem
    ON oraklion.audit_log (subsystem, changed_at DESC);

-- 3) rate_limiter — én rad per mentor-kall. Aggregeres per sesjon.
--    Ryddes av admin (scheduled DELETE) eller manuelt.
CREATE TABLE IF NOT EXISTS oraklion.rate_limiter (
    id          bigserial   PRIMARY KEY,
    session_id  text        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rate_limiter_session_time
    ON oraklion.rate_limiter (session_id, created_at DESC);

-- 4) Verifisering
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'oraklion'
ORDER BY table_name;
