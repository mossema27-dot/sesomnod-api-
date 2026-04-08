---
name: log
description: Logg kampresultat, beregn CLV og Brier Score. Henter PENDING-picks fra Notion MATCH_PREDICTIONS, tar inn resultat + closing odds + score, oppdaterer backend via /log-results og skriver resultatdata tilbake til samme Notion-rad.
---

# /log — Logg kampresultat med CLV og Brier

## Konstanter

```
API_BASE              = https://sesomnod-api-production.up.railway.app
NOTION_MATCH_PRED_ID  = 541e9c96-b2e8-4424-9bfa-7bac08d97e79
NOTION_DATA_SOURCE_ID = db4f2beb-a403-46a6-a8e2-6ef950c5b344
BRIER_OK              = < 0.25
BRIER_OBSERVE         = 0.25 – 0.35
BRIER_STOP            = > 0.35
```

## Arkitektur

All resultat-data (`CLV`, `Closing_odds`, `Brier Score`, `Status`) skrives tilbake til **samme rad** i `MATCH_PREDICTIONS` som `/morning` opprettet. Det finnes **ingen** separat RESULTS_LOG-database — schemaet ble utvidet 2026-04-08 med disse feltene nettopp for å konsolidere alt på én rad.

Endepunktet `/log-results` (main.py:4554) brukes fortsatt for å oppdatere Postgres (`dagens_kamp.result` + `picks_v2.outcome` via trigger). Ingen `/results-log` eller `/results-log/calculate-brier` finnes — Brier beregnes klient-side.

## STEG 1 — Hent PENDING picks fra Notion

Bruk Notion MCP `API-query-data-source`:

```json
{
  "data_source_id": "db4f2beb-a403-46a6-a8e2-6ef950c5b344",
  "filter": {
    "and": [
      {"property": "Status",  "select": {"equals": "PENDING"}},
      {"property": "Kickoff", "date":   {"on_or_before": "{today ISO}"}}
    ]
  },
  "sorts": [{"property": "Kickoff", "direction": "descending"}],
  "page_size": 50
}
```

For hver returnerte side, pluk ut fra `properties`:
- `page.id` (trenger vi for update)
- `Name.title[0].plain_text` → kamp
- `Hjemmelag.rich_text[0].plain_text`, `Bortelag.rich_text[0].plain_text`
- `Odds.number` → våre_odds
- `Pick.rich_text[0].plain_text` → pick-string
- `Edge_pct.number` (nytt felt, fallback til parsing av `Edge.rich_text` hvis tomt)
- `Kelly_stake.number` (nytt)

Presenter som nummerert liste:

```
PENDING picks klar for logging:
  1. Arsenal vs Liverpool         — Odds: 2.15  Kickoff: 2026-04-08 18:30
  2. Real Madrid vs Barcelona     — Odds: 1.95  Kickoff: 2026-04-08 20:45
  ...
Velg kamp (1-N): 
```

Hvis listen er tom: `"Ingen PENDING picks før i dag. Alt er logget."`

## STEG 2 — Hent input fra bruker

Spør kun om:
1. **Resultat:** `WIN` / `LOSS` / `PUSH`
2. **Closing odds** (desimal, f.eks `1.85`) — Pinnacle closing om mulig.
3. **Home score** og **Away score** (heltall) — nødvendig for `/log-results`.

## STEG 3 — Beregn CLV og Brier

```python
våre_odds = float(notion_page.properties.Odds.number)

# CLV: positiv = vi fikk bedre pris enn close
clv_decimal = (våre_odds / closing_odds) - 1    # f.eks 0.035 = +3.5%

# Brier: bruk implied prob fra våre odds som model_prob-proxy
# (hvis Confidence-feltet er satt, bruk det i stedet)
confidence_val = notion_page.properties.Confidence.number  # 0.0–1.0 eller None
model_prob     = confidence_val if confidence_val else (1 / våre_odds)
outcome        = 1 if resultat == "WIN" else 0
brier          = (model_prob - outcome) ** 2
```

## STEG 4 — Oppdater backend via /log-results

```
POST {API_BASE}/log-results
Content-Type: application/json

[{
  "home":       "{home_team}",
  "away":       "{away_team}",
  "home_score": {home_score},
  "away_score": {away_score},
  "pick":       "{pick_text fra Notion}",
  "odds":       {våre_odds}
}]
```

Forvent `{"logged": [...], "total_settled": N, "phase0": "N/30"}`. Backenden beregner selv WIN/LOSS/VOID fra score + pick-string og oppdaterer `dagens_kamp.result`. Hvis backendens utledning avviker fra brukerens input, advar men fortsett med Notion-oppdatering (vi stoler på brukerens observasjon).

## STEG 5 — Oppdater Notion-raden

Bruk `API-patch-page` med `page_id` fra STEG 1:

```json
{
  "properties": {
    "Status":       {"select": {"name": "{resultat}"}},
    "Closing_odds": {"number": {closing_odds}},
    "CLV":          {"number": {clv_decimal}},
    "Brier Score":  {"number": {brier}}
  }
}
```

**Viktig navnedetaljer:**
- `Brier Score` har **mellomrom**, ikke underscore (eksisterende felt i schemaet).
- `CLV` er percent-format → send rå desimal (`0.035`), Notion viser `3.5%`.
- `Status`-options inkluderer nå `WIN`, `LOSS`, `PUSH` (lagt til 2026-04-08).

Ingen RESULTS_LOG-side opprettes — alt er på samme rad.

## STEG 6 — Bankroll P&L

Ingen Notion bankroll-database finnes. Beregn lokalt og rapportér i STEG 7:

```python
kelly_stake = notion_page.properties.Kelly_stake.number or 0.01   # fallback 1%
if resultat == "WIN":
    pnl_units = (våre_odds - 1) * kelly_stake * 100               # i bankroll-units
elif resultat == "PUSH":
    pnl_units = 0
else:
    pnl_units = -kelly_stake * 100
```

For lenge-sikts tracking: Postgres `bankroll`-tabellen mottar oppdateringer via scheduler-jobben `track_clv` i `main.py`, ikke denne skill-en.

## STEG 7 — Rapport

Hent aggregat fra `GET {API_BASE}/dashboard/stats` → `live.phase0_picks`, `live.hit_rate`, `live.avg_clv`, `backtest.avg_brier`.

```
═══════════════════════════════
📊 Resultat logget: {home} vs {away}
Resultat:   {WIN|LOSS|PUSH}
Våre odds:  {våre_odds:.2f}
Closing:    {closing_odds:.2f}
CLV:        {clv_decimal*100:+.2f}%  {✅ hvis >0 ellers ❌}
Brier:      {brier:.3f}  {✅ <0.25 | ⚠️ <0.35 | 🛑 ≥0.35}
P&L:        {pnl_units:+.2f} units
───────────────────────────────
Phase 0 status (fra backend):
Picks logget:      {live.phase0_picks}/30
Hit rate:          {live.hit_rate*100:.1f}%   (gate: >55%)
Snitt CLV (30d):   {live.avg_clv:+.2f}%       (gate: >+2%)
Avg Brier (bt):    {backtest.avg_brier:.3f}   (gate: <0.25)
───────────────────────────────
{✅ Phase 0 on track | ⚠️ Observer | 🛑 STOPP}
═══════════════════════════════
```

Gate-logikk:

```python
if hit_rate > 0.55 and avg_clv > 2.0 and avg_brier < 0.25:
    status = "✅ Phase 0 on track"
elif avg_brier >= 0.35 or hit_rate < 0.40:
    status = "🛑 STOPP"
else:
    status = "⚠️ Observer"
```

## Feilhåndtering

- Notion 401/403: `🚨 Notion-autentiseringsfeil — sjekk NOTION_TOKEN i Railway`.
- Notion `property not found`: schemaet er utvidet 2026-04-08 — hvis feltet mangler, kall `API-retrieve-a-data-source` og verifiser at det ikke er blitt endret manuelt.
- `/log-results` 500: rapportér `{error}` men fortsett med Notion-oppdatering.
- Bruker velger nummer som ikke finnes: re-prompt, ikke crash.
