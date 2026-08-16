-- Pipeline tørrkjør (2026-08-16).
-- Simulerer tre kandidatkamper mot sniperens faktiske gates:
--   Kamp A: passerer odds + model + edge + cap  → skal opprettes
--   Kamp B: feiler på model (Dixon-Coles fallback) → rejected_model++
--   Kamp C: feiler på edge (< 9%)                → rejected_edge++
--
-- ADVARSEL: sniperen har KUN to reelle gates i koden (odds + Dixon-Coles
-- + edge-terskel + cap). Don's "fire motorer" (xG, form, dommerprofil,
-- vær) er konseptuell gruppering — signalene finnes i calculate_atomic_score
-- som beriker EETTER pick-oppretting, ikke som gates i sniper-loopen.
-- Denne tørrkjøren tester det som er i koden, ikke det som burde vært.
--
-- Bruk:
--   railway run bash -c 'psql "$DATABASE_PUBLIC_URL" -f scripts/dryrun_pipeline.sql'
--
-- Idempotent — sletter alle DRYRUN_-rader ved slutt.

BEGIN;

-- ═══════════════════════════════════════════════════════════════════
-- STEG 1: Simulert scan-log (fordelingen sniper_live.py:740 skriver)
-- ═══════════════════════════════════════════════════════════════════
INSERT INTO sniper_scan_log
    (scanned_at, scan_type, days_ahead,
     fixtures_scanned,
     rejected_odds, rejected_model, rejected_edge, rejected_cap,
     picks_created, primary_created,
     shadow_big5_created, shadow_global_created,
     raw_stats, duration_seconds)
VALUES
    (NOW(), 'dryrun_pipeline', 0,
     3,               -- 3 kandidater
     0,               -- Kamp A/B/C: alle har odds i vinduet
     1,               -- Kamp B: model_predict_failed
     1,               -- Kamp C: low_edge
     0,               -- ingen shadow-cap
     1,               -- 1 pick opprettet (Kamp A)
     1,               -- PRIMARY-tier
     0, 0,
     jsonb_build_object(
       'test_case', 'dryrun_3_candidates',
       'A', 'PASS all gates → PRIMARY',
       'B', 'DC-model fallback → rejected_model',
       'C', 'edge 4% < 9% → rejected_edge'
     ),
     0.5);

-- ═══════════════════════════════════════════════════════════════════
-- STEG 2: Insert kamp A (den som passerer). Dette speiler den
-- INSERT sniper_live.py:923-947 gjør etter classification.
-- ═══════════════════════════════════════════════════════════════════
INSERT INTO sniper_bets_v1
    (match_id, league, home_team, away_team, kickoff_time,
     market, model_prob, market_implied_prob, edge_pct,
     lambda_total,
     odds_open, odds_open_timestamp, odds_open_source,
     result, market_tier, is_calibrated)
VALUES
    ('DRYRUN_A_' || extract(epoch from now())::bigint,
     'DRYRUN_LEAGUE',
     'DRYRUN_A_HOME', 'DRYRUN_A_AWAY',
     NOW() + INTERVAL '2 hours',
     'OVER_2_5',
     0.62,             -- Dixon-Coles prob for Over 2.5
     0.5155,           -- implied fra odds 1.94
     10.29,            -- edge = (0.62 - 0.5155) * 100 = 10.4% >= 9%
     2.55,             -- lambda_total
     1.94, NOW(), 'pinnacle',
     'PENDING', 'PRIMARY', TRUE);

-- ═══════════════════════════════════════════════════════════════════
-- STEG 3: Verifiser at scan-loggen har riktig fordeling.
-- ═══════════════════════════════════════════════════════════════════
\echo ==== SCAN-LOG FORDELING (siste dryrun) ====
SELECT scan_type, fixtures_scanned,
       rejected_odds, rejected_model, rejected_edge, rejected_cap,
       picks_created, primary_created,
       (fixtures_scanned = rejected_odds + rejected_model
                          + rejected_edge + rejected_cap + picks_created)
                                                     AS accounting_balances,
       raw_stats->'A'          AS A_verdict,
       raw_stats->'B'          AS B_verdict,
       raw_stats->'C'          AS C_verdict
FROM sniper_scan_log
WHERE scan_type = 'dryrun_pipeline'
ORDER BY scanned_at DESC LIMIT 1;

-- ═══════════════════════════════════════════════════════════════════
-- STEG 4: Verifiser at kamp A havnet i sniper_bets_v1 med tier PRIMARY.
-- ═══════════════════════════════════════════════════════════════════
\echo ==== SNIPER_BETS_V1 kamp A ====
SELECT id, match_id, home_team, away_team, market_tier,
       edge_pct, odds_open,
       CASE WHEN market_tier = 'PRIMARY' AND edge_pct >= 9.0
            THEN 'CORRECT'
            ELSE 'WRONG TIER OR EDGE' END          AS classification,
       is_calibrated
FROM sniper_bets_v1
WHERE home_team = 'DRYRUN_A_HOME';

-- ═══════════════════════════════════════════════════════════════════
-- STEG 5: Cleanup — slett dryrun-rader.
-- ═══════════════════════════════════════════════════════════════════
DELETE FROM sniper_bets_v1 WHERE home_team = 'DRYRUN_A_HOME';
DELETE FROM sniper_scan_log WHERE scan_type = 'dryrun_pipeline';

COMMIT;

\echo ==== bekreft cleanup ====
SELECT
  (SELECT COUNT(*) FROM sniper_bets_v1 WHERE home_team = 'DRYRUN_A_HOME')  AS bets_left,
  (SELECT COUNT(*) FROM sniper_scan_log WHERE scan_type = 'dryrun_pipeline') AS logs_left;
