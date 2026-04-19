-- ============================================================
-- pick_intelligence_log — SesomNod MOAT Foundation
-- KJØRES IKKE AUTOMATISK — kun manuelt etter godkjenning
-- Kjør mot Railway PostgreSQL via: psql $DATABASE_URL -f dette-scriptet
-- ============================================================

-- Forutsetning: picks_v2-tabellen eksisterer allerede
-- Ingen DROP TABLE, TRUNCATE eller ALTER på eksisterende tabeller

CREATE TABLE IF NOT EXISTS pick_intelligence_log (
    id                          SERIAL PRIMARY KEY,
    pick_id                     TEXT NOT NULL,
    dossier_id                  TEXT,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Dossier-kvalitet
    dossier_completeness_pct    NUMERIC(5,2),

    -- Intelligence scores (0-100)
    edge_integrity_score        NUMERIC(5,2),
    confidence_fragility_score  NUMERIC(5,2),
    narrative_distortion_index  NUMERIC(5,2),
    sharp_alignment_score       NUMERIC(5,2),
    scenario_survival_rate      NUMERIC(5,2),
    no_bet_pressure_index       NUMERIC(5,2),
    contradiction_density       NUMERIC(5,2),

    -- Verdict
    verdict                     TEXT NOT NULL,
    verdict_reason              TEXT,
    confidence_downgraded       BOOLEAN DEFAULT FALSE,
    original_confidence         TEXT,
    effective_confidence        TEXT,

    -- Triggers og contradictions (JSON arrays)
    false_confidence_triggers   JSONB DEFAULT '[]'::jsonb,
    contradictions_weak         JSONB DEFAULT '[]'::jsonb,
    contradictions_medium       JSONB DEFAULT '[]'::jsonb,
    contradictions_critical     JSONB DEFAULT '[]'::jsonb,

    -- Operator output
    operator_why                TEXT,
    operator_warn               JSONB DEFAULT '[]'::jsonb,
    operator_stress             TEXT,

    -- Receipt
    receipt_sha256              TEXT,
    pick_number                 INTEGER,
    phase                       TEXT DEFAULT 'Phase-1',

    -- Post-match (fylles inn etter kamp)
    actual_outcome              TEXT,
    closing_clv                 NUMERIC(7,4),
    flags_correct               JSONB DEFAULT '[]'::jsonb,
    flags_noise                 JSONB DEFAULT '[]'::jsonb,
    mirofish_improved_decision  BOOLEAN,

    -- Full JSON-snapshot for reanalyse
    full_brief_json             JSONB
);

-- Indekser
CREATE INDEX IF NOT EXISTS idx_pil_pick_id
    ON pick_intelligence_log (pick_id);

CREATE INDEX IF NOT EXISTS idx_pil_verdict
    ON pick_intelligence_log (verdict);

CREATE INDEX IF NOT EXISTS idx_pil_created_at
    ON pick_intelligence_log (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_pil_phase
    ON pick_intelligence_log (phase);

-- Partial index: hent alle som trenger post-match fylling
CREATE INDEX IF NOT EXISTS idx_pil_needs_postmatch
    ON pick_intelligence_log (created_at DESC)
    WHERE actual_outcome IS NULL;

-- ============================================================
-- SLUTT PÅ SCRIPT
-- Kjøres aldri automatisk — alltid manuell godkjenning
-- ============================================================
