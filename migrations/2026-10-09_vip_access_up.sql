-- VIP allowlist: free Inner Circle access granted manually by Don. No Stripe involvement.
-- Run manually (not auto-applied). Idempotent.
CREATE TABLE IF NOT EXISTS vip_access (
    email        TEXT PRIMARY KEY CHECK (email = lower(email)),
    tier         TEXT NOT NULL DEFAULT 'inner_circle' CHECK (tier IN ('inner_circle')),
    source       TEXT NOT NULL DEFAULT 'VIP — gratis, satt av Don',
    granted_by   TEXT NOT NULL DEFAULT 'don',
    granted_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at   TIMESTAMPTZ,
    note         TEXT
);
