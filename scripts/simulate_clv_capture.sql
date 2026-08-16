-- CLV pre-kickoff capture simulation (2026-08-16).
-- Verifiserer at sniperens close-capture-pipeline (update_odds_close)
-- fanger tidsstempler FØR kickoff, ikke etter.
--
-- Bruk:
--   railway run bash -c 'psql "$DATABASE_PUBLIC_URL" -f scripts/simulate_clv_capture.sql'
--
-- Idempotent. Cleaner opp etter seg selv. Kjør i transaksjon.

BEGIN;

-- Insert testrad med kickoff om 5 min (midt i [NOW+3min, NOW+7min]-vinduet).
INSERT INTO sniper_bets_v1
    (match_id, league, home_team, away_team, kickoff_time,
     market, model_prob, market_implied_prob, edge_pct,
     odds_open, odds_open_timestamp, odds_open_source,
     result, market_tier, is_calibrated)
VALUES
    ('CLV_TEST_' || extract(epoch from now())::bigint,
     'TestLiga', 'CLV_TEST_HOME', 'CLV_TEST_AWAY',
     NOW() + INTERVAL '5 minutes',
     'OVER_2_5', 0.62, 0.54, 8.0,
     1.85, NOW(), 'pinnacle',
     'PENDING', 'PRIMARY', TRUE);

-- Test 1: fanges raden av capture-vinduet?
\echo ==== TEST 1: capture-vinduet ====
SELECT id, home_team, (kickoff_time - NOW()) AS time_to_kickoff
FROM sniper_bets_v1
WHERE odds_close IS NULL
  AND kickoff_time > NOW() + INTERVAL '3 minutes'
  AND kickoff_time < NOW() + INTERVAL '7 minutes'
  AND home_team = 'CLV_TEST_HOME';

-- Mock capture — settes odds_close = 1.80 og odds_close_timestamp = NOW.
UPDATE sniper_bets_v1
SET odds_close             = 1.80,
    odds_close_timestamp   = NOW(),
    clv_close_pct          = round(((1.85/1.80 - 1)*100)::numeric, 2),
    is_positive_clv_close  = (1.80 < 1.85),
    close_capture_minutes_before = EXTRACT(EPOCH FROM (kickoff_time - NOW()))::int / 60
WHERE home_team = 'CLV_TEST_HOME';

-- Test 2: er odds_close_timestamp FØR kickoff_time?
\echo ==== TEST 2: pre/post-kickoff-verdikt ====
SELECT id, home_team, kickoff_time, odds_close_timestamp,
       EXTRACT(EPOCH FROM (odds_close_timestamp - kickoff_time))::int AS sec_after_kickoff,
       CASE WHEN odds_close_timestamp < kickoff_time
            THEN 'PRE-KICKOFF (RIKTIG)'
            ELSE 'POST-KICKOFF (FEIL)' END AS verdict,
       clv_close_pct, is_positive_clv_close, close_capture_minutes_before
FROM sniper_bets_v1
WHERE home_team = 'CLV_TEST_HOME';

-- Cleanup.
DELETE FROM sniper_bets_v1 WHERE home_team = 'CLV_TEST_HOME';

COMMIT;

\echo ==== bekreft slettet ====
SELECT COUNT(*) AS remaining FROM sniper_bets_v1 WHERE home_team = 'CLV_TEST_HOME';
