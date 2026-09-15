-- ORAKLION BRAIN v2 — migrering DOWN (2026-09-15)
-- Fjerner KUN objekter opprettet av 2026-09-15_brain_v2_up.sql.
-- Rører aldri oraklion.events, mentor_log, audit_log, sniper_bets_v1, picks_v2.
-- Rører aldri schemaet oraklion selv (deles med v1/mentor).
-- MERK: brain_events har en immutabilitets-trigger som blokkerer TRUNCATE;
-- DROP TABLE er tillatt (trigger følger med tabellen). Rollback av ledger =
-- tap av v2-hendelser. Kjøres kun etter Don's eksplisitte ord.

DROP TABLE IF EXISTS oraklion.brain_stats_snapshot;
DROP TABLE IF EXISTS oraklion.brain_ticks;
DROP TABLE IF EXISTS oraklion.brain_heartbeat;
DROP TABLE IF EXISTS oraklion.brain_config;
DROP TABLE IF EXISTS oraklion.brain_decisions;
DROP FUNCTION IF EXISTS oraklion.brain_verify_chain();
DROP TABLE IF EXISTS oraklion.brain_events;
DROP FUNCTION IF EXISTS oraklion.brain_events_immutable();
