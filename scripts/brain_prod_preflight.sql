-- Oraklion Brain v2 — PROD PREFLIGHT (kun lesing). Kjøres av tools/brain_prod_migrate.py --preflight.
-- 1) Kolonner motoren leser fra sniper_bets_v1 (engine.py fetch_candidates/settle_open/source_last_fetch)
SELECT 'MISSING_COLUMN' AS check_name, c.col AS detail
FROM unnest(ARRAY['id','match_id','league','home_team','away_team','kickoff_time','pick_timestamp','market',
                  'model_prob','odds_open','odds_open_timestamp','market_tier','result','settled_at',
                  'fixture_status','home_goals','away_goals','total_goals','odds_close','odds_close_timestamp',
                  'close_capture_minutes_before','clv_source']) AS c(col)
WHERE NOT EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema='public' AND table_name='sniper_bets_v1' AND column_name=c.col);
