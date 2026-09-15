# BRAIN_V2_STATE — gjenopptakbar utførelse (brev v2.1 rev 2, §9)

Ny sesjon leser denne først og fortsetter fra siste grønne commit. Aldri start forfra.

## Ankre
- Backend git: `brain-v1-anchor` = `8c46de7` (lokal `main` HEAD 2026-08-29; **ikke bekreftet lik prod** — `/health` gir versjon `10.2.0-btts` uten sha; `PROD_BACKEND_SHA = UNKNOWN`)
- Frontend git: `web-brain-v1-anchor` = `0e9af88` (lokal `main` HEAD 2026-08-29)
- Frontend prod: `NETLIFY_PUBLISHED_DEPLOY_ID = NO_SOURCE_ACCESS` (hentes i Netlify → Deploys → Published før "deploy frontend")
- Branch i begge repo: `feat/brain-v2`

## Baseline (2026-09-15 17:06 UTC, read-only)
- A Brain-ledger-modul: ABSENT. `scripts/verify_oraklion_brain.sql` refererer `oraklion_brain_schema.sql` som ikke finnes i repo.
- B `oraklion`-schema: DB-svar utestående (Don kjører `railway connect`). `/public/oraklion/chain` → `schema_missing` (tabellen `oraklion_chain` finnes ikke).
- C Job: ingen (FORVENTET 0). 33 jobs totalt.
- D Kandidatvolum: Sniper skanner (70/dag, 18 ligaer), 2 settled siste 30 d. Kun marked `OVER_2_5`.
- H Kandidatkilde: `picks_v2` ubrukelig (ingen fixture_id, ingen odds-tid). `CANDIDATE_SOURCE = sniper_bets_v1`.
- Odds-tid: `ODDS_TS_BASIS = column` (`odds_open_timestamp` settes i samme loop-iterasjon som henting, `services/sniper_live.py` ~l. 914).
- Closing: `sniper_bets_v1.odds_close` (Pinnacle, fanget T-5 av `sniper_odds_close`-jobben). `pinnacle_markets_at_close` lagrer kun markedsnavn+antall, ikke Under-odds → `p_fair_close`/`clv_fair_pct` = null i M1 (kun `clv_odds_pct`).
- G Lokal test-DB: ukjent på Mac (VM har ingen). DB-tester hopper over uten `BRAIN_TEST_DSN`.
- Backend skittent tre ved start (7 filer, bl.a. `M services/market_scanner.py`): ligger urørt, legges aldri til.
- Frontend: `git status` var ødelagt av foreldreløs worktree `.claude/worktrees/friendly-ritchie`; `git worktree prune` kjørt og `.git`-filen i mappen omdøpt til `.git.orphaned-20260915` (reversibelt). 11 skitne filer ligger urørt.

## Commits (M1)
| C | Innhold | sha | Tester |
|---|---|---|---|
| C1 | migrations up/down, docs | — | — |
| C2 | canonical, ledger, rules, stats | — | test_brain_pure |
| C3 | engine (scan/commit/settle/snapshot/heartbeat) | — | test_brain_db (krever DSN) |
| C5 | api + main.py-hook bak flagg | — | py_compile |
| C6 | frontend BrainV2 bak VITE_BRAIN_V2 | — | tsc + build |

## Utestående godkjenninger
"godkjent ALTER oraklion v2" · "OK job" · "push feat/brain-v2" · "deploy frontend" · "M2"
