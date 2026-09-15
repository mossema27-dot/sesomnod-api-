# Oraklion Brain v2 — migrering (manuell, aldri automatisk)

Migreringen kjøres KUN etter Don's ord **"godkjent ALTER oraklion v2"**. Den kjøres aldri ved oppstart av appen.

## Hva den gjør
`migrations/2026-09-15_brain_v2_up.sql` legger til nye objekter under schemaet `oraklion`, alle med prefiks `brain_`:
`brain_events` (append-only ledger med immutabilitets-trigger), `brain_verify_chain()`, `brain_decisions`,
`brain_config`, `brain_heartbeat`, `brain_ticks`, `brain_stats_snapshot`.
Den rører ikke `oraklion.events` (v1), `mentor_log`, `audit_log`, `sniper_bets_v1` eller `picks_v2`.
Idempotent: trygg å kjøre to ganger.

## Lokalt (test)
```bash
createdb brain_test
psql brain_test -f migrations/2026-09-15_brain_v2_up.sql
psql brain_test -c "SELECT * FROM oraklion.brain_verify_chain();"   # FORVENTET: 0 rader
BRAIN_TEST_DSN=postgresql://localhost/brain_test python3 -m unittest tests/test_brain_db.py -v
```

## Prod (Railway) — etter "godkjent ALTER oraklion v2"
```bash
cd /Users/don/sesomnod-api && railway connect
```
Velg **Postgres**, deretter i `railway=#`:
```sql
\i migrations/2026-09-15_brain_v2_up.sql
SELECT table_name FROM information_schema.tables WHERE table_schema='oraklion' AND table_name LIKE 'brain_%';
SELECT * FROM oraklion.brain_verify_chain();
```
Forventet: 6 tabeller listet, 0 rader fra verify.

## Rollback
```sql
\i migrations/2026-09-15_brain_v2_down.sql
```
Fjerner kun `brain_`-objektene. **Ledger-hendelser i `brain_events` går tapt** — derfor kun etter Don's ord.
Kode-rollback: `git revert` av C1-commiten (sha i commit-meldingen).

## Aktivering av jobben (separat ord: "OK job")
Railway → Variables → `BRAIN_V2_JOBS=on`. Default er `off`: jobben registreres ikke, endepunktene svarer `schema_missing`/tom tilstand.
