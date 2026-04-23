-- SmartPick LIGHT v1 — JSONB payload cache for picks_v2
-- Additive only. Idempotent. Auto-applied at app startup (main.py).
-- Reference file: actual execution path is main.py ALTER TABLE block.

ALTER TABLE picks_v2
    ADD COLUMN IF NOT EXISTS smartpick_payload JSONB;

CREATE INDEX IF NOT EXISTS idx_picks_v2_smartpick
    ON picks_v2 USING GIN (smartpick_payload);
