-- Personal VIP access code: only a salted PBKDF2-SHA256 hash is stored. Idempotent.
ALTER TABLE vip_access ADD COLUMN IF NOT EXISTS code_hash TEXT;
ALTER TABLE vip_access ADD COLUMN IF NOT EXISTS code_set_at TIMESTAMPTZ;
ALTER TABLE vip_access ADD COLUMN IF NOT EXISTS code_revoked_at TIMESTAMPTZ;
