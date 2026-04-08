# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

SesomNod is a football-betting value-detection engine. A FastAPI app (`main.py`) scans odds from The Odds API across ~18 leagues, computes EV/edge against Pinnacle no-vig probabilities, enriches picks with Dixon-Coles + XGBoost models, sizes stakes via Kelly, and posts picks to Telegram / Notion / a "MiroFish" sync target. APScheduler drives the workflow on cron windows; Postgres (asyncpg) is the system of record.

The repo is primarily Python; a few Node.js helpers (`kelly-integration.js`, `twilio-alerts.js`, `poisson-model.js`, scrapers) are called as side utilities and depend on the vendored `lib/kelly-js` package.

## Run / Deploy

- Local dev: `uvicorn main:app --reload --port 8000`
- Install: `pip install -r requirements.txt` (Python 3.11, see `Dockerfile`); `npm install` for the Node helpers.
- Deploy: Railway via `Dockerfile` / `railway.toml`; healthcheck = `/health`. `Procfile` mirrors the uvicorn command.
- No test suite or linter is configured in this repo. `lib/kelly-js` is a vendored third-party package — do not edit it as part of app changes.

## Architecture

`main.py` (~6k lines) is the monolith and owns:

1. **Config & league lists** — `SCAN_LEAGUES`, `TOP4_LEAGUES`, API budget constants. The Odds API has a hard monthly credit budget enforced by `_check_api_budget` / `_log_api_call` against an `api_calls` table.
2. **DB lifecycle** — `connect_db`, `ensure_tables`, `reconnect_loop`. All schema is created idempotently in `ensure_tables`; there are no migrations. Key tables: `picks`, `picks_v2`, `dagens_kamp`, `clv_records`, `bankroll`, `api_calls`.
3. **Odds ingestion** — `fetch_all_odds` / `fetch_top4_odds` pull h2h + totals, snapshot per-bookmaker prices, and feed `_analyse_snapshot`, which derives Pinnacle no-vig fair odds (`_pinnacle_no_vig`), computes EV/edge, applies the atomic score `EV_pct × log(book_count + 1)` (`calculate_atomic_score`), and Kelly stake (`calculate_kelly_stake`).
4. **Model enrichment** — picks are enriched with Dixon-Coles (`services/dixon_coles_engine.py`) and an XGBoost model (`services/xgboost_model.py`, trained via `services/xgb_training.py`, persisted by `services/model_storage.py`). `services/pick_feature_extractor.py` builds features; `signals/` adds referee/weather adjustments. `services/team_normalizer.py` reconciles team names across data sources.
5. **Scheduler** — APScheduler in `lifespan()` runs the daily windows (early/midday/evening scans, `pre_kickoff_check`, `track_clv`, `post_clv_rapport_telegram`, `post_dagens_kamp_telegram`, `_check_live_results`). Anything that mutates picks should also call `_sync_to_picks_v2` and `_log_pick_to_mirofish` to keep downstream sinks consistent (see recent commits — missing this is the most common bug).
6. **HTTP surface** — read endpoints (`/picks`, `/dagens-kamp`, `/clv`, `/dashboard/stats`, `/backtest/*`, `/bankroll`) and manual triggers (`/fetch-odds`, `/run-analysis`, `/post-telegram`, `/check-results-now`, `/log-results`, `/test-telegram`). These triggers exist so operators can re-run a scheduler step out of band; prefer them over invoking internal functions directly.
7. **Safety shims** — `sys.exit` is monkey-patched to a no-op (`_safe_exit`) so background tasks can never kill the process; imports go through `_safe_import` so optional modules (`bankroll`, `dagens_kamp`, `auto_result`) degrade gracefully. Preserve this behavior when refactoring startup code.

`core/` holds cross-cutting infra: `kelly_engine.py`, `circuit_breaker.py`, `rate_limiter.py`. `services/backtest_engine.py` powers the `/backtest/*` endpoints.

## Domain rules

- **Mandate:** football only, restricted to the leagues listed in `SCAN_LEAGUES` / `SESOMNOD_ACTIVE_PICKS.md`. Do not add NBA/tennis/etc. paths.
- **Pricing baseline:** Pinnacle is treated as the truth. EV/edge must always be computed against Pinnacle no-vig probabilities, never against the median or the offered book.
- **Pick identity in MiroFish:** `pick_id` must include `market_type` to avoid dedup collisions across h2h/totals on the same fixture (see commit 582b107).
- **Edge threshold:** current production gate is 20% edge for "high-conviction" picks (commit 4aef395) — change deliberately.
