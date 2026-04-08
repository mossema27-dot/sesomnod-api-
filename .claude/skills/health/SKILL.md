---
name: health
description: Full systemsjekk av SesomNod — Railway API, picks, dagens-kamp, Telegram, Notion MATCH_PREDICTIONS, og API-budsjett. Kjører alle sjekker parallelt og rapporterer responstid.
---

# /health — Full systemstatus

## Konstanter

```
API_BASE               = https://sesomnod-api-production.up.railway.app
NOTION_MATCH_PRED_ID   = 541e9c96-b2e8-4424-9bfa-7bac08d97e79
NOTION_DATA_SOURCE_ID  = db4f2beb-a403-46a6-a8e2-6ef950c5b344
```

## Sjekker (kjør parallelt)

Bruk én message med flere tool-kall når du gjør HTTP + MCP-kallene. Mål latency per kall med wall-clock timer. Kategorisering:

```
✅ OK         (< 1000 ms)
⚠️ TREG       (1000 – 3000 ms)
🔴 KRITISK    (> 3000 ms)
❌ FEIL       (timeout, 4xx, 5xx, eller status != forventet)
```

### 1. Railway API

```
GET {API_BASE}/health
```

Forvent `200` med `{"status": "online", "db": {"connected": true, ...}}`. `db.connected == false` → `❌ FEIL: DB offline`.

### 2. Picks endpoint

```
GET {API_BASE}/picks
```

Forvent `200` med `{"status": "ok", "data": [...], "count": N}`. `status == "offline"` → `❌ FEIL: DB ikke tilgjengelig`.

### 3. Dagens kamp

```
GET {API_BASE}/dagens-kamp
```

Forvent `200` med `{"status": "ok", "data": [...], "count": N}`.

### 4. Telegram bot

```
POST {API_BASE}/test-telegram
```

(Viktig: `/test-telegram` er `POST`, ikke `GET` — verifisert i main.py:4357.) Forvent `{"status": "sent", "telegram_http": 200}`. `telegram_http != 200` → `❌ FEIL`.

### 5. Notion MATCH_PREDICTIONS

Bruk Notion MCP `API-query-data-source` mot **data source ID-en** (ikke database-ID-en):

```json
{
  "data_source_id": "db4f2beb-a403-46a6-a8e2-6ef950c5b344",
  "page_size": 1
}
```

Forvent én side i resultatet. 401/403 → `❌ FEIL: NOTION_TOKEN mangler/utløpt`. 404 → `❌ FEIL: Data source slettet eller ikke delt med integrasjonen`.

### 6. Notion schema-integritet

Bruk `API-retrieve-a-data-source` med samme ID. Sjekk at kritiske nye felter eksisterer (lagt til 2026-04-08):

```python
required_properties = {
    "Edge_pct", "Kelly_stake", "Omega_score", "BTTS_yes",
    "xG_home", "xG_away", "Market_type", "CLV", "Closing_odds",
    "Brier Score"  # merk: mellomrom, ikke underscore
}
missing = required_properties - set(data_source.properties.keys())
```

Hvis `missing` er ikke-tom: `⚠️ SCHEMA-DRIFT: {missing} mangler — /morning og /log vil feile på disse feltene. Re-kjør schema-migrering.`

Sjekk også Status-options inneholder `{"PENDING", "WIN", "LOSS", "PUSH"}`. Hvis ikke: `⚠️ Status-options endret manuelt.`

### 7. API-budsjett

```
GET {API_BASE}/status
```

Forvent `200`. Les `scheduler.api_calls_this_month`, `scheduler.api_budget_monthly`, `scheduler.api_calls_remaining`. (Merk: `/dashboard/stats` har ikke budsjett-feltet — bruk `/status`.)

Kategoriser:
- `remaining > 100` → `✅`
- `50 < remaining <= 100` → `⚠️ LAV`
- `remaining <= 50` → `🔴 KRITISK`
- `remaining <= 0` → `❌ BUDSJETT OPPBRUKT`

## Retry-regel

- Maks 3 forsøk, 2s mellom.
- Timeout 5s per forsøk.
- Ved 401/403 på Notion: ikke retry — rapporter `🚨 Autentiseringsfeil`.

## Output

```
╔══════════════════════════════════════════════╗
║   SesomNod Systemstatus {HH:MM UTC}         ║
╠══════════════════════════════════════════════╣
║ Railway API        {status}  {Xms}           ║
║ Picks endpoint     {status}  {Xms}           ║
║ Dagens kamp        {status}  {Xms}           ║
║ Telegram           {status}  {Xms}           ║
║ Notion Picks       {status}  {Xms}           ║
║ Notion Schema      {status} ({N} felter OK)  ║
║ API budget         {calls}/{budget}          ║
╠══════════════════════════════════════════════╣
║ OVERALL: {✅ OPERATIV | ⚠️ DEGRADERT | 🛑 NEDE} ║
╚══════════════════════════════════════════════╝
```

## Overall-logikk

```python
if any(check == "❌ FEIL" for check in critical_checks):  # Railway, Picks, DB
    overall = "🛑 NEDE"
elif any(check in ("⚠️ TREG", "🔴 KRITISK", "⚠️ LAV", "⚠️ SCHEMA-DRIFT") for check in all_checks):
    overall = "⚠️ DEGRADERT"
else:
    overall = "✅ OPERATIV"
```

`critical_checks` = [Railway, Picks, Dagens-kamp]. Telegram-feil, Notion-feil, schema-drift, og budget `KRITISK` er degradert, ikke nede.

## Ved kritisk feil: foreslå handling

- **Railway API `❌ FEIL`** → `Sjekk Railway dashboard → Service logs. Mest sannsynlig: container restart, DATABASE_URL mismatch, eller healthcheck-timeout. Prøv 'railway logs' eller Railway web UI.`
- **DB offline (`db.connected == false`)** → `Railway Postgres er nede eller DATABASE_URL er endret. Verifiser env var og restart service.`
- **Telegram `❌ FEIL`** → `Sjekk TELEGRAM_TOKEN og TELEGRAM_CHAT_ID i Railway env. Kjør POST /test-telegram manuelt for detaljer.`
- **Notion 401/403** → `NOTION_TOKEN utløpt eller revokert. Generer nytt token i Notion → Integrations og oppdater Railway env.`
- **Notion 404 på database-ID** → `Databasen 541e9c96-... er slettet eller ikke delt med integrasjonen. Del databasen med SesomNod-integrasjonen igjen.`
- **Schema-drift** → `Kjør skill-oppdateringsflowen på nytt: MCP update-data-source med properties-diffen for {missing}. Sjekk memory/notion_match_predictions.md for autoritativ liste.`
- **API budget `❌ OPPBRUKT`** → `The Odds API månedsbudsjett er brukt. Ingen nye scans til månedsskifte. Vurder å pause scheduler-jobbene: /admin/… (sjekk tilgjengelige admin-endepunkter i main.py).`
