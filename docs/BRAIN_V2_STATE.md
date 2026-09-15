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

## Commits (M1) — alle på feat/brain-v2, 2026-09-15
| C | Innhold | sha | Tester (kjørt i Linux-VM på Don's Mac) |
|---|---|---|---|
| C1 | migrations up/down, MIGRATION_HOWTO, STATE | `7d41c27` | — |
| C2 | canonical, ledger, rules, stats + test_brain_pure | `839c6cb` | 14/14 grønne (stdlib) |
| C3 | engine + test_brain_db | `99fecac` | 5/5 grønne mot lokal PostgreSQL 16 (pgserver): T1, T2/T15, T10, T11, T19 |
| C5 | api, main.py (+31 linjer, 0 slettet), tools/verify_ledger.py | `9cf178c` | py_compile OK; router-ruter verifisert; engine_state-avledning testet |
| C6 | frontend (sesomnod repo): BrainV2.tsx, brainQueries.ts, brainTypes.ts, App.tsx, nav.ts | `cc7b769` | `tsc -b` 0 feil; `vite build` grønn med flagg av/på; 0 nye tsc-feil (66 pre-existing på main, uendret) |
| C7 | state-oppdatering | `5fee447` | rollback-drill: anker-worktree `py_compile main.py` OK; anker-worktree `vite build` OK |
| C8 | lokal DEMO-kjører: `tools/brain_local_demo.py`, `scripts/brain_demo_up.sh`, `.gitignore` (+`.brain_demo_pg/`, `.brain_demo_venv/`), STATE | `40e4a6a` | kjørt i VM mot pgserver: seed = [EPOCH_START, COMMIT, REVEAL, HOLD], `brain_verify_chain()` 0 rader, `/health` mode=DEMO |
| C9 | frontend (sesomnod repo): DEMO-banner i `BrainV2.tsx` når `envelope.source` starter med `demo` (+ `document.title` prefiks) | `25bfb36` | `tsc -b` 0 feil (VM); `vite dev/build` må kjøres på Mac (rolldown darwin-binding) |

## Syntetisk M1-løp (lokal DB, dokumentert 2026-09-15)
kandidat (sniper PENDING, p=0.61, odds 1.95, lock age 20 min) → TICK1 COMMIT seq=2 → kilde setter close 1.85 + WIN →
TICK2 waiting (første observasjon) → TICK3 SETTLED (REVEAL seq=3) + HOLD(NO_MODEL_ROWS) →
`/public/oraklion/brain`: engine RUNNING, n_settled=1, net +950, ROI 0.95, clv_odds +5.41 %, coverage 1.0, preliminary=true →
`ledger.json`: [EPOCH_START, COMMIT, REVEAL, HOLD], `tools/verify_ledger.verify` OK, `brain_verify_chain()` 0 rader,
negativ grep (bookmaker/hostnavn/kroner/DSN) på begge responser: 0 treff.

## Kjente begrensninger i M1 (ærlig, ikke skjult)
- `clv_fair_pct`, `p_fair_close`, `brier_market` = null: kilden lagrer ikke Under-odds ved close (`pinnacle_markets_at_close` = navn+antall). M2: utvide Sniper close-capture (ikke guard-fil, men egen scope-godkjenning).
- Bootstrap-intervall, compound-simulering (kapitalbinding/R-CORR), deliveries/CHAIN_HEAD, decisions.csv, methodology-endepunkt: M2.
- BrainV2-chunken bygges også med flagg av (lazy import), men ruten registreres ikke. Demo-fixture: `tools/brain_local_demo.py` (C8) — kun lokal, aldri prod.
- Frontend `tsconfig.tsbuildinfo` (generert cache) ble tilbakestilt til HEAD under bygg-test; regenereres av `npm run build`.

## Lokal DEMO-kjøring (C8/C9, 2026-09-15 19:21 UTC, verifisert i VM)
- `tools/brain_local_demo.py`: importerer IKKE main.py, leser IKKE .env, ingen scheduler/Telegram/API-kall, bind 127.0.0.1:8100, nekter DSN med `railway`/`rlwy`. Innebygd PostgreSQL (pgserver) i `.brain_demo_pg/` (gitignored). Seeder syntetisk M1-løp relativt til nå (T0 = nå−6 t) → identisk resultat som §Syntetisk: RUNNING, n_settled=1, net +950, ROI 0.95, clv_odds +5.4054 %, coverage 1.0, chain_head seq 4, HOLD(NO_MODEL_ROWS). Alle "live"-konvolutter får `source="demo"`; `/health` → `{"mode":"DEMO"}`. Negativ grep 0 treff.
- Frontend: banner (gul, sticky, `data-testid="brain-demo-banner"`) vises KUN når `envelope.source` starter med `demo` — prod-API sender aldri dette.
- Kjør på Mac (én kommando): `bash /Users/don/sesomnod-api/scripts/brain_demo_up.sh` → åpne `http://localhost:5173/oraklion.html#/brain-v2`. Ctrl-C stopper begge.
- Cowork-VM-begrensninger (viktig for neste sesjon): bakgrunnsprosesser dør når kallet avsluttes (bwrap --die-with-parent) → servere må startes på Mac av Don; `mnt/` tillater ikke unlink uten slette-tillatelse → `git status` etterlater `.git/index.lock` (bruk `git --no-optional-locks status`); `vite build/dev` feiler i VM (kun `binding-darwin-x64`), `tsc -b` går.

## Utestående godkjenninger
"godkjent ALTER oraklion v2" · "OK job" · "push feat/brain-v2" · "deploy frontend" · "M2"
