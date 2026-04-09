# SesomNod API — Prosjektkonfigurasjon
# Generert: 9. april 2026

## ARKITEKTUR
- FastAPI + asyncpg + Railway PostgreSQL v10.2.0-btts
- Python: asyncio gjennomgående — ingen sync DB-kall
- Logging: logger.info/warning (aldri print())
- HTTP: httpx.AsyncClient med timeout=10

## KRITISKE FILER
- main.py: over 4500 linjer — alltid grep før edit
- services/dixon_coles_engine.py: ML-modell, ikke endre schema
- services/kelly_calculator.py: Kelly Criterion logikk
- database.py: connection pool — aldri modifiser uten backup

## SQL-REGLER
- Primærtabell: picks_v2 (ALDRI tabellen "picks")
- Parameteriserte queries alltid: $1, $2 (aldri f-strings i SQL)
- Inspiser schema før ny SQL: grep -n "CREATE TABLE" main.py
- Legg alltid til kolonner med IF NOT EXISTS

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

## MIROFISH-INTEGRASJON
- _log_pick_to_mirofish: kalles fra 3 steder i main.py
- _submit_result_to_mirofish: fire-and-forget ved kamp-resultat
- pick_id: {home_slug}-{away_slug}-{YYYYMMDD}-{market_type}
- Aldri endre pick_id-format uten å koordinere med MiroFish

## KONTEKST-GRENSE
- /compact manuelt ved 50% kontekst — ikke vent på automatisk
