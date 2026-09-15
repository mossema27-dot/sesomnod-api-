# BRAIN_V2_STATE — gjenopptakbar utførelse (brev v2.1 rev 2, §9)

Ny sesjon leser denne først og fortsetter fra siste grønne commit. Aldri start forfra.

## Ankre
- Backend git: `brain-v1-anchor` = `8c46de7` (lokal `main`, 1 commit foran origin). **`PROD_BACKEND_SHA = ccbb6b1`** (verifisert 2026-09-15 22:35 UTC: `git ls-remote origin main` = ccbb6b1 og prod `/public/oraklion/telemetry` er fortsatt ugatet = 8c46de7 ikke deployet). Railway deployer ved push til `origin/main`.
- Frontend git: `web-brain-v1-anchor` = `0e9af88` (lokal `main` HEAD 2026-08-29)
- Frontend prod: **Cloudflare Pages** (prosjekt `sesomnod`, wrangler fra `dist/`), IKKE Netlify (DEPLOY_POLICY 29. aug). Siste kjente deployment `46856178` = `0e9af88`. Rollback = bygg forrige commit + `wrangler pages deploy` (eller dashboard «Rollback to this deployment»).
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

## Nettleser-verifikasjon på Mac (2026-09-15 19:39 UTC, Chrome, `scripts/brain_demo_up.sh` kjørt av Don)
- `http://localhost:5173/oraklion.html#/brain-v2` rendret: fanetittel `DEMO · SESOMNOD · Command Deck`; gult sticky DEMO-banner øverst; status RUNNING; HOLD · NO_MODEL_ROWS; DEMO HOME v DEMO AWAY, locked 1.950, p 61.0 %, EV +18.9 %, lock age 20 min, closing 1.850 @ T-5, CLV +5.41 %, Brier 0.152100, WIN; Results: settled 1, net +950 units, ROI 95.0 %, max DD 0. Chain head #4.
- C10 `cc97654`: demo-kjøreren serverer også `/public/oraklion/state` avledet fra lokal DB (engine_paused=false, leagues 1, scanned/sealed fra brain_ticks/brain_events, chain_intact fra brain_verify_chain) → TopBar RUNNING · 1 · 1 · 1 · INTACT, ingen 404. main.py urørt; prod uendret.
- Playwright-verifikasjon (sky-Chromium, samme kode, 2026-09-15 21:43 UTC): 0 konsollfeil/-advarsler, alle API-kall 200 (`net::ERR_ABORTED` = React StrictMode dobbelt-mount i dev, deretter 200), banner synlig, 4 ledger-rader, desktop uten horisontal scroll. Skjermbilder: `brain-v2-demo-{desktop,mobile}-{viewport,full}.png` (levert i chat).
- FUNN (pre-existing, gjelder alle /oraklion-sider inkl. prod): skallet har ingen responsive regler (`tokens.css` har kun én `@media prefers-reduced-motion`); sidebar er `position: sticky; height: 100vh` med fast bredde → på 390 px mobil tar sidebar ~halve skjermen og innholdet får horisontal scroll. Ikke BrainV2-spesifikt. Fiks = egen scope-godkjenning («responsivt skall»).
- Brier (model) `—` i Results er korrekt: `stats.compute_flat` krever parret brier_market (M1-begrensning), mens beslutningskortet viser brier_model 0.152100.
- Claude-in-Chrome-utvidelsen var ikke tilkoblet, og "Control Chrome"-MCP feilet på JS-eksekvering (`Google Chrome is not running`) → konsoll kunne ikke leses maskinelt; rendering verifisert via skjermbilde av Chrome-vinduet (lesetilgang).

## Responsivt skall + Brier (C11–C13, 2026-09-15 22:25 UTC, lokalt, ingen push)
- Skitne filer: tidligere «rene»-rapport i chat skyldtes `grep -v "^??"` (skjulte untracked). Faktisk: api 1 M (`services/market_scanner.py`) + 7 untracked (6 opprinnelige + `Claude outputs/` som Cowork-appen la inn 21:45 med PNG-kopier); web 10 untracked (`.claude/settings.local.json`, 3 md-notater, `public/_headers|images/|robots.txt|sitemap.xml|videos/`, `src/vite-env.d.ts`). Ingenting slettet/tilbakestilt. `.claude/worktrees/friendly-ritchie` ligger fortsatt (med `.git.orphaned-20260915`).
- web `f695cc9` C11: `Shell.tsx`/`Sidebar.tsx`/`TopBar.tsx`/`tokens.css` — ≤1239px: sidebar = fast skuff (`#ork-sidebar.is-open`, `visibility:hidden` når lukket), `.ork-nav-toggle` i TopBar (`aria-expanded`, `aria-controls="ork-sidebar"`), lukkeknapp i skuffen, scrim, Escape lukker, fokus → lukkeknapp ved åpning og tilbake til toggle ved lukking, Tab-felle, lukkes ved rutebytte og ved resize til desktop, body-scroll låst mens åpen. ≤767px: inline `grid-template-columns` legges om via attributt-selektorer (`!important`), registerrader strammes (32/28px + 56/64px). ≥1240px: null nye regler → desktop uendret (pixel-diff 1440 før/etter: brain/eye/chain 0.006–0.008 % = kun UTC-klokke; deck 0.059 % = klokke + live-data).
- web `a3fa717` C12 + api `5bdb489` C13: `stats.compute_flat` → `brier.model` over alle oppgjorte med brier_model (`n_model`, `coverage_model`); `market`/`diff` kun parret (`n` = parrede, diff = parret modell − marked). BrainV2 caption: `n=1 · market Brier and comparison need paired closing data (0 paired)`. `test_brain_pure` 15/15 (ny test). Demo `/public/oraklion/state` har nå degraded-fallback (`source="demo:error"`).
- SEALED: prod `sealed_today` = COUNT(oraklion_chain WHERE sealed_at::date = i dag) — hash-forseglede forpliktelser før avspark; Chain-siden viser `sealed_at`-tid; Brain-siden «Sealed today — Commitments in the day». Teller IKKE oppgjorte beslutninger og antyder ikke compound. Demo mapper SEALED → COMMIT-hendelser i dag (samme semantikk). Ingen omdøping nødvendig.
- Playwright (sky-Chromium, prod-API read-only GET for deck/brain/eye/chain, demo-backend for brain-v2): 360/390/768/1440 × 5 sider → 0 horisontal sidescroll, 0 elementer utenfor viewport, 0 konsollfeil. Før: alle ≤768 hadde scrollWidth 1240. Bilder: `shell-before-*`, `shell-after-*` (levert i chat).
- Sky-notat: pgserver-datamappe må ligge utenfor scratchpad (`/home/claude/pgdemo`) — scratchpad-rettigheter endres og PostgreSQL PANIC-er ved checkpoint.

## PROD-RELEASE av M1 (autorisert av Don 2026-09-16 00:35 Oslo — samlet: ALTER + push + OK job + deploy frontend)
- Verktøy (C14 `e673ca2`, `3c293bf`): `scripts/brain_prod_release.sh` (faser: preflight → push → wait-backend → migrate → activate → wait-tick → frontend → verify → status; hver fase idempotent), `tools/brain_prod_migrate.py` (asyncpg via `railway run -s Postgres`, DSN aldri skrevet ut; `--preflight` kun lesing: kolonnesjekk mot engine-feltene, kandidatvindu, oppgjørsfelter, duplikater, Sniper-helse; `--up` idempotent i transaksjon + verifisering; `--status`; `--down` nektes uten `--i-understand-ledger-loss`), `scripts/brain_prod_preflight.sql`. Testet i VM mot pgserver: preflight OK, up idempotent (6 tabeller, 19 config-nøkler, 0 kjedeproblemer), status OK, down nektet.
- main.py C14: brain-jobben får `next_run_time = oppstart + 90 s` (første prod-skanning innen 2 min etter aktivering, deretter 15 min).
- Frontend C15 `ef62381`: `.env.production` += `VITE_BRAIN_V2=true` (prod-bygg inkluderer ruten; `VITE_API_BASE` = prod-API).
- Det som deployes = `origin/main ccbb6b1` + 8c46de7 (Don, 29. aug: telemetry bak operator-auth — frontend 0e9af88 sluttet å bruke den offentlig samme dag) + M1 C1–C14. 20 filer, kun additivt. Beskyttede filer urørt. `services/market_scanner.py` (M) pushes IKKE.
- Kandidat-integritet i prod: `fetch_candidates` tar kun `result='PENDING'` med avspark i (nå, nå+48 t] uten eksisterende beslutning; regler krever lock age ≤ 90 min og ≥ 60 min til avspark → gamle Sniper-rader kan aldri bli «nye» Brain-valg. Ingen syntetiske rader i prod (demo-seed finnes kun i `tools/brain_local_demo.py`).
- SOURCE-SLA: `source_last_fetch` leser `sniper_scan_log.max(scanned_at)` (finnes i prod — `/state.scanned_today=70`) → SOURCE_STALE kun hvis Sniper faktisk slutter å skanne.
- Kundetilgang/betaling: sesomnod.com har ingen offentlig checkout/Whop/Stripe-lenke (grep 22:40 UTC) → ingenting å verifisere; ikke bygget nytt.
- Kjør på Mac: `bash /Users/don/sesomnod-api/scripts/brain_prod_release.sh` (logg i `$TMPDIR/brain_release_*.log`). Stopper ved første feil; gammel deploy forblir live til ny er healthy (Railway healthcheck `/health`).

## Utestående godkjenninger
~~"godkjent ALTER oraklion v2" · "OK job" · "push feat/brain-v2" · "deploy frontend"~~ → gitt samlet 2026-09-16 (se PROD-RELEASE). Gjenstår: "M2".
