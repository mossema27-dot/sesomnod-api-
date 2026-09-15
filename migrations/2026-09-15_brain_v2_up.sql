-- ORAKLION BRAIN v2 — migrering UP (2026-09-15)
-- Idempotent. Legger KUN til nye objekter under oraklion-schemaet.
-- Rører aldri eksisterende tabeller (oraklion.events, mentor_log, audit_log, sniper_bets_v1, picks_v2).
-- Alle nye objekter har prefiks brain_ for å unngå kollisjon med v1-objekter.
-- Prod-kjøring kun etter Don's "godkjent ALTER oraklion v2". Se docs/MIGRATION_HOWTO.md.

CREATE SCHEMA IF NOT EXISTS oraklion;

-- ─────────────────────────────────────────────────────────────
-- 1) brain_events — append-only, hash-kjedet ledger.
--    canonical = eksakt tekst som ble hashet (event_public, §5.4).
--    meta_private = interne felt, bundet via meta_private_hash inne i canonical.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS oraklion.brain_events (
    seq             bigint      PRIMARY KEY,
    event_id        uuid        NOT NULL UNIQUE,
    event_type      text        NOT NULL,
    ts_utc          timestamptz NOT NULL,
    decision_id     uuid,
    campaign_id     uuid,
    schema_version  text        NOT NULL DEFAULT 'v2',
    canonical       text        NOT NULL,
    prev_hash       char(64)    NOT NULL,
    hash            char(64)    NOT NULL UNIQUE,
    meta_private    jsonb,
    created_utc     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT brain_events_seq_positive CHECK (seq >= 1)
);

CREATE INDEX IF NOT EXISTS idx_brain_events_type     ON oraklion.brain_events (event_type);
CREATE INDEX IF NOT EXISTS idx_brain_events_decision ON oraklion.brain_events (decision_id);
CREATE INDEX IF NOT EXISTS idx_brain_events_ts       ON oraklion.brain_events (ts_utc);

-- Immutabilitet: UPDATE/DELETE/TRUNCATE feiler med 42501 (også for eier-rollen).
-- Superuser kan disable trigger; det er dokumentert (§5.4), ikke skjult.
CREATE OR REPLACE FUNCTION oraklion.brain_events_immutable()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'oraklion.brain_events is append-only (%s blocked)', TG_OP
        USING ERRCODE = '42501';
END $$;

DROP TRIGGER IF EXISTS trg_brain_events_no_update ON oraklion.brain_events;
CREATE TRIGGER trg_brain_events_no_update
    BEFORE UPDATE OR DELETE ON oraklion.brain_events
    FOR EACH ROW EXECUTE FUNCTION oraklion.brain_events_immutable();

DROP TRIGGER IF EXISTS trg_brain_events_no_truncate ON oraklion.brain_events;
CREATE TRIGGER trg_brain_events_no_truncate
    BEFORE TRUNCATE ON oraklion.brain_events
    FOR EACH STATEMENT EXECUTE FUNCTION oraklion.brain_events_immutable();

-- ─────────────────────────────────────────────────────────────
-- 2) brain_verify_chain — rekomputerer sha256(prev_hash || canonical) fra
--    LAGRET tekst. Re-serialiserer aldri jsonb. Returnerer avvikende rader.
--    FORVENTET: 0 rader. Bruker innebygd sha256(bytea) (PostgreSQL >= 11).
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION oraklion.brain_verify_chain()
RETURNS TABLE (seq bigint, problem text) LANGUAGE sql STABLE AS $$
    WITH ordered AS (
        SELECT e.seq, e.prev_hash, e.hash, e.canonical,
               lag(e.hash) OVER (ORDER BY e.seq) AS expected_prev,
               lag(e.seq)  OVER (ORDER BY e.seq) AS prev_seq
        FROM oraklion.brain_events e
    )
    SELECT o.seq, 'hash_mismatch'::text
    FROM ordered o
    WHERE o.hash <> encode(sha256(convert_to(o.prev_hash || o.canonical, 'UTF8')), 'hex')
    UNION ALL
    SELECT o.seq, 'prev_hash_mismatch'::text
    FROM ordered o
    WHERE o.expected_prev IS NOT NULL AND o.prev_hash <> o.expected_prev
    UNION ALL
    SELECT o.seq, 'genesis_prev_not_zero'::text
    FROM ordered o
    WHERE o.expected_prev IS NULL AND o.prev_hash <> repeat('0', 64)
    UNION ALL
    SELECT o.seq, 'seq_gap'::text
    FROM ordered o
    WHERE o.prev_seq IS NOT NULL AND o.seq <> o.prev_seq + 1
    ORDER BY 1;
$$;

-- ─────────────────────────────────────────────────────────────
-- 3) brain_decisions — projeksjon (kan oppdateres ved oppgjør).
--    Sannheten er brain_events; denne tabellen kan bygges om fra hendelsene.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS oraklion.brain_decisions (
    decision_id               uuid        PRIMARY KEY,
    seq                       bigint      NOT NULL REFERENCES oraklion.brain_events(seq),
    record_type               text        NOT NULL DEFAULT 'FORWARD'
                              CHECK (record_type IN ('FORWARD','BACKTEST','SHADOW')),
    status                    text        NOT NULL
                              CHECK (status IN ('OPEN','SUSPENDED','SETTLED','VOID')),
    source_table              text        NOT NULL,
    source_id                 bigint      NOT NULL,
    fixture_id                text        NOT NULL,
    league                    text,
    home                      text        NOT NULL,
    away                      text        NOT NULL,
    kickoff_utc               timestamptz NOT NULL,
    market_key                text        NOT NULL,
    p_model                   numeric(8,6) NOT NULL,
    odds_locked               numeric(7,3) NOT NULL,
    odds_ts_utc               timestamptz NOT NULL,
    odds_ts_basis             text        NOT NULL CHECK (odds_ts_basis IN ('column','row_created_verified')),
    lock_age_min              integer     NOT NULL,
    ev                        numeric(8,6) NOT NULL,
    model_version             text        NOT NULL,
    selection_rule_version    text        NOT NULL,
    idempotency_key           text        NOT NULL UNIQUE,
    committed_utc             timestamptz NOT NULL,
    -- oppgjør
    outcome                   text        CHECK (outcome IN ('WIN','LOSS','VOID')),
    settled_utc               timestamptz,
    settlement_rules_version  text,
    observed_result           text,
    observed_utc              timestamptz,
    -- closing-referanse (fra kildens capture; Brain gjør ingen egne API-kall)
    odds_close                numeric(7,3),
    close_ts_utc              timestamptz,
    close_minutes_before      integer,
    closing_missing_reason    text,
    p_fair_close              numeric(8,6),
    clv_odds_pct              numeric(9,4),
    clv_fair_pct              numeric(9,4),
    brier_model               numeric(9,6),
    brier_market              numeric(9,6),
    -- ekstern publisering
    published_utc             timestamptz,
    publication_ref           text,
    created_utc               timestamptz NOT NULL DEFAULT now(),
    updated_utc               timestamptz NOT NULL DEFAULT now()
);

-- Én åpen beslutning globalt (SUSPENDED blokkerer ikke).
CREATE UNIQUE INDEX IF NOT EXISTS ux_brain_decisions_one_open
    ON oraklion.brain_decisions ((true)) WHERE status = 'OPEN';
-- Samme kamp + marked kan aldri registreres to ganger.
CREATE UNIQUE INDEX IF NOT EXISTS ux_brain_decisions_fixture_market
    ON oraklion.brain_decisions (fixture_id, market_key);
CREATE INDEX IF NOT EXISTS idx_brain_decisions_status  ON oraklion.brain_decisions (status);
CREATE INDEX IF NOT EXISTS idx_brain_decisions_settled ON oraklion.brain_decisions (settled_utc);

-- ─────────────────────────────────────────────────────────────
-- 4) brain_config — alle terskler. Endring skjer via CONFIG_CHANGE-hendelse.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS oraklion.brain_config (
    key         text        PRIMARY KEY,
    value       text        NOT NULL,
    updated_utc timestamptz NOT NULL DEFAULT now()
);

INSERT INTO oraklion.brain_config (key, value) VALUES
    ('METHODOLOGY_VERSION',      'v2'),
    ('SELECTION_RULE_VERSION',   'v1'),
    ('SETTLEMENT_RULES_VERSION', 'v1'),
    ('DEVIG_METHOD',             'multiplicative'),
    ('CANDIDATE_SOURCE',         'sniper_bets_v1'),
    ('CANDIDATE_TIERS',          'PRIMARY'),
    ('MARKETS',                  'OU|2.5|OVER'),
    ('P_MIN',                    '0.55'),
    ('ODDS_MIN',                 '1.50'),
    ('ODDS_MAX',                 '2.60'),
    ('EV_MIN',                   '0.05'),
    ('MIN_MINUTES_TO_KICKOFF',   '60'),
    ('MAX_HOURS_TO_KICKOFF',     '48'),
    ('MAX_LOCK_AGE_MIN',         '90'),
    ('FLAT_STAKE',               '1000'),
    ('SOURCE_SLA_SEC',           '108000'),
    ('TICK_INTERVAL_SEC',        '900'),
    ('SETTLE_STABLE_MIN',        '30'),
    ('BRAIN_API_DAILY_CAP',      '0')
ON CONFLICT (key) DO NOTHING;

-- ─────────────────────────────────────────────────────────────
-- 5) brain_heartbeat — tre tilstander avledes server-side (§5.6).
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS oraklion.brain_heartbeat (
    job_name               text        PRIMARY KEY,
    expected_interval_sec  integer     NOT NULL,
    last_start_utc         timestamptz,
    last_ok_utc            timestamptz,
    last_error             text,
    last_source_fetch_utc  timestamptz,
    api_calls_today        integer     NOT NULL DEFAULT 0,
    api_day                date
);

-- ─────────────────────────────────────────────────────────────
-- 5b) brain_ticks — hver skann logges (grunnlag for hold_ratio_30d).
--     HOLD-hendelser i ledgeren skrives maks én per UTC-dag per grunn.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS oraklion.brain_ticks (
    id           bigserial   PRIMARY KEY,
    tick_utc     timestamptz NOT NULL,
    reason       text,               -- NULL = COMMIT skjedde
    scanned_n    integer     NOT NULL DEFAULT 0,
    passed_n     integer     NOT NULL DEFAULT 0,
    skip_counts  jsonb,
    decision_id  uuid
);
CREATE INDEX IF NOT EXISTS idx_brain_ticks_utc ON oraklion.brain_ticks (tick_utc DESC);

-- ─────────────────────────────────────────────────────────────
-- 6) brain_stats_snapshot — beregnes ved oppgjør, leses av endepunktet.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS oraklion.brain_stats_snapshot (
    id                   bigserial   PRIMARY KEY,
    methodology_version  text        NOT NULL,
    computed_utc         timestamptz NOT NULL DEFAULT now(),
    payload              jsonb       NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_brain_stats_snapshot_computed
    ON oraklion.brain_stats_snapshot (computed_utc DESC);
