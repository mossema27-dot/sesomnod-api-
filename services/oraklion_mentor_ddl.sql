-- Oraklion Mentor persistence — SafetyGate + AuditLog + RateLimiter DDL
-- (BRIEF 2026-08-15, revidert etter SafetyGate-testing).
-- Idempotent. Trygg å kjøre flere ganger.
-- Alle tabeller lever under oraklion-schemaet.

CREATE SCHEMA IF NOT EXISTS oraklion;

-- ─────────────────────────────────────────────────────────────
-- 1) mentor_log — audit av hver mentor-interaksjon.
--    user_input og response er PII-maskerte ved skrivetidspunkt.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS oraklion.mentor_log (
    id                bigserial   PRIMARY KEY,
    session_id        text        NOT NULL,
    created_at        timestamptz NOT NULL DEFAULT NOW(),
    user_input_masked text        NOT NULL,
    response_masked   text,
    verdict           text        NOT NULL,
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

-- Verdict-check: må inkludere SafetyGate-verdiktene og safe_locked.
-- DROP + ADD sikrer idempotens ved oppdatert enum-liste.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'mentor_log_verdict_chk'
    ) THEN
        ALTER TABLE oraklion.mentor_log DROP CONSTRAINT mentor_log_verdict_chk;
    END IF;
    ALTER TABLE oraklion.mentor_log
        ADD CONSTRAINT mentor_log_verdict_chk
            CHECK (verdict IN (
                'ok',
                'blocked_input',
                'blocked_output',
                'rate_limited',
                'upstream_error',
                'crisis',
                'dependency',
                'panic',
                'safe_locked'
            ));
END $$;

-- ─────────────────────────────────────────────────────────────
-- 2) audit_log — generell audit for oraklion-side.
-- ─────────────────────────────────────────────────────────────
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

-- ─────────────────────────────────────────────────────────────
-- 3) rate_limiter — én rad per mentor-kall (per NONE-verdict).
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS oraklion.rate_limiter (
    id          bigserial   PRIMARY KEY,
    session_id  text        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rate_limiter_session_time
    ON oraklion.rate_limiter (session_id, created_at DESC);

-- ─────────────────────────────────────────────────────────────
-- 4) safe_sessions — sticky SAFE-modus per session_id.
--    PRIMARY KEY på session_id gjør at ON CONFLICT DO NOTHING
--    beholder FØRSTE verdikt. Kan ikke reverseres innen sesjonen.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS oraklion.safe_sessions (
    session_id            text        PRIMARY KEY,
    verdict               text        NOT NULL
        CHECK (verdict IN ('CRISIS','DEPENDENCY','PANIC')),
    activated_at          timestamptz NOT NULL DEFAULT NOW(),
    first_message_masked  text        NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_safe_sessions_activated_at
    ON oraklion.safe_sessions (activated_at DESC);

-- ─────────────────────────────────────────────────────────────
-- 5) Verifisering
-- ─────────────────────────────────────────────────────────────
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'oraklion'
ORDER BY table_name;
