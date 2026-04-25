# SesomNod API — Prosjektkonfigurasjon
# Generert: 9. april 2026 · oppdatert: 2026-04-19

## LIVE INFRASTRUKTUR
- Backend: https://sesomnod-api-production.up.railway.app
- Frontend: https://sesomnod.netlify.app (PIN: <stored in env / ask Don>)
- MiroFish CLV: https://mirofish-service-production.up.railway.app
- GitHub: mossema27-dot/sesomnod-api-
- DB: Railway PostgreSQL (aldri eksponér DATABASE_URL)

## ABSOLUTTE REGLER — SESOMNOD-SPESIFIKKE
1. Aldri hardkod PORT (Railway setter $PORT automatisk)
2. Aldri gjør MiroFish/Notion-kall synkrone — alltid asyncio.create_task()
3. Kun Pinnacle no-vig closing odds er gyldig CLV-referanse
4. pick_id-format er hellig: {home_slug}-{away_slug}-{YYYYMMDD}-{market_type}
5. Aldri slett eller migrer mirofish_clv-tabellen uten eksplisitt godkjenning
6. Bekreft alltid live /health etter Railway-deploy
7. Aldri deploy sesomnod-api og MiroFish i samme operasjon

## ARKITEKTUR
- FastAPI + asyncpg + Railway PostgreSQL v10.2.0-btts
- Python: asyncio gjennomgående — ingen sync DB-kall
- Logging: logger.info/warning (aldri print())
- HTTP: httpx.AsyncClient med timeout=10

## KRITISKE FILER
- main.py: ~14 000 linjer (450 KB) — les seksjonsvis, aldri helhetlig
- services/dixon_coles_engine.py: ML-modell, ikke endre schema
- services/kelly_calculator.py: Kelly Criterion logikk
- database.py: connection pool — aldri modifiser uten backup

## DATABASE
- Primærtabell: picks_v2 (ALDRI tabellen "picks")
- MiroFish-tabell: mirofish_clv (57 tracked picks, +3.28% avg CLV)
- CLV-data er Phase 1-beviset — integritet er ufravikelig
- Omega-tiers: BRUTAL ≥72, STRONG ≥55, MONITORED ≥40, SKIP <40
- Phase 1-gate: CLV >+2% ✅, hit rate >55% (pending), picks ≥30 (pending)

## SQL-REGLER
- Parameteriserte queries alltid: $1, $2 (aldri f-strings i SQL)
- Inspiser schema før ny SQL: grep -n "CREATE TABLE" main.py
- Legg alltid til kolonner med IF NOT EXISTS

## KRITISKE INTEGRASJONER
- Telegram: SesomNodBot → Channel -1003747091014 (aldri bryt posting-flyten)
- MiroFish tracker picks automatisk fra Telegram-posting
- _check_live_results → _submit_result_to_mirofish (fire-and-forget)
- Frontend deployer via Netlify CLI (ikke git — frontend er IKKE et git-repo)

## TELEGRAM-REGLER
- Aldri bryt posting-flyten
- MiroFish-kall: asyncio.create_task() etter bot.send_message()
- build_telegram_message() er i main.py — les den før du endrer

## DEPLOY-SEKVENS (BACKEND)
1. python3 -m py_compile main.py (syntaksjekk)
2. git add -A
3. git commit -m "beskrivende melding"
4. git push origin main
5. Vent 2 min → curl https://sesomnod-api-production.up.railway.app/health
6. Bekreft: status: online, db_connected: true

## DEPLOY-SEKVENS (FRONTEND)
- npm run build → npx netlify-cli deploy --prod --dir=dist

## MIROFISH-INTEGRASJON
- _log_pick_to_mirofish: kalles fra 3 steder i main.py
- _submit_result_to_mirofish: fire-and-forget ved kamp-resultat
- pick_id: {home_slug}-{away_slug}-{YYYYMMDD}-{market_type}
- Aldri endre pick_id-format uten å koordinere med MiroFish

## KONTEKST-GRENSE
- /compact manuelt ved 50% kontekst — ikke vent på automatisk
