---
name: morning
description: SesomNod morgensritual — kjøres kl 08:45 for å verifisere Railway, hente dagens picks, poste til Telegram og logge kvalifiserte picks til Notion. 7 steg med full feilhåndtering.
---

# /morning — SesomNod morgensritual

Kjør dette hver dag kl 08:45. Hvert steg kjører uavhengig — feil i ett steg stopper ikke de andre (unntatt STEG 1 som stopper alt).

## Konstanter (ikke endre uten å oppdatere alle skills)

```
API_BASE               = https://sesomnod-api-production.up.railway.app
NOTION_MATCH_PRED_ID   = 541e9c96-b2e8-4424-9bfa-7bac08d97e79
EDGE_THRESHOLD         = 0.08       # 8% — brukt på normaliserte desimaledger
KICKOFF_WINDOW         = NOW()-3h .. NOW()+36h
PICKS_TABLE            = picks_v2    # dashboard/bankroll; /picks leser fra dagens_kamp
```

## Viktig om datastruktur (verifisert mot main.py)

- `/picks` leser fra `dagens_kamp` og returnerer `{status, data, count}` der hver `data[i]` er et enriched pick med feltene:
  `id, match_name, home_team, away_team, odds, edge, ev, atomic_score, omega_score, omega_tier, tier, market_hint, our_pick, league, kickoff_time, confidence, btts_yes, xg_home, xg_away, home_win_prob, draw_prob, away_win_prob, smart_bets, dixon_coles, kelly, xgboost, live_score, is_live`
- **Edge-format er inkonsistent i koden.** `dagens_kamp.edge` lagres noen ganger som desimal (0.08), andre ganger som prosentpoeng (8.0). Normaliser alltid før sammenligning: `edge_norm = edge if edge < 1 else edge / 100`.
- **`confidence` er numerisk, ikke en string.** Det finnes ingen `"HIGH"`-label i DB. Bruk `tier in ("ATOMIC","EDGE")` som HIGH-confidence-proxy (dette er det produksjonsgaten i `/post-telegram` bruker).
- **Notion-schema utvidet 2026-04-08.** Data source ID: `db4f2beb-a403-46a6-a8e2-6ef950c5b344`. Felter vi skriver til:
  - **Legacy (backend-kompatible, skrives også av `_log_notion_pick`):** `Name (title), Liga (select), Hjemmelag, Bortelag, Kickoff (date), Pick, Odds (number), Edge (rich_text), EV (rich_text), Confidence (number, percent), Stake (rich_text), Status (select)`
  - **Nye numeriske (2026-04-08):** `Edge_pct (number, percent), Kelly_stake (number, percent), Omega_score (number), BTTS_yes (number 0–100), xG_home (number), xG_away (number), Market_type (rich_text), CLV (number, percent), Closing_odds (number)`
  - `BTTS_yes` er `number`-format (ikke percent) — `enrich_pick` returnerer heltall 0–99.
  - `Edge_pct`, `Kelly_stake`, `CLV` er percent-format: send rå desimal (0.12 → vises som 12%).
  - `Brier Score` (med mellomrom, ikke underscore) finnes også, brukes i `/log`.

## Retry-regel (alle HTTP-kall)

- Maks 3 forsøk, 2s mellom.
- Timeout >5s: logg `⚠️ TREG` og fortsett.
- 5xx: logg `❌ FEIL` med årsak.
- 401/403: STOPP hele ritualet og rapportér `🚨 Autentiseringsfeil — sjekk Railway env vars`.

## STEG 1 — HEALTH CHECK (08:45)

```
GET {API_BASE}/health
```

Forvent `200 OK` med `{"status": "online", "service": "sesomnod-api", "db": {...}}`.

- ✅ Hvis `status == "online"` og `db.connected == true`: fortsett.
- 🛑 Hvis ikke: STOPP HELE RITUALET, skriv:
  `🚨 Railway er nede (eller DB offline). Ingen picks i dag. Sjekk Railway dashboard og /status-endpointet.`

## STEG 2 — HENT DAGENS KAMP (09:00)

```
GET {API_BASE}/dagens-kamp
```

Parse `data[]`. Filtrer klient-side på `kickoff` innenfor `NOW()-3h .. NOW()+36h` (`/dagens-kamp` returnerer siste 50 uten filtrering, sortert på `timestamp DESC`).

Hvis tom liste etter filter: `"Ingen kamper i vinduet i dag (08:45–i morgen 21:00 CET)."`

## STEG 3 — HENT OG VALIDER PICKS (09:00)

```
GET {API_BASE}/picks
```

Responsen er allerede filtrert på kickoff-vindu (1h bak → 36h frem) og returnerer maks 100. For hvert pick i `data[]`, klassifiser:

```
edge_norm = edge if edge < 1 else edge / 100

if edge_norm >= EDGE_THRESHOLD and tier in ("ATOMIC", "EDGE") and omega_score is not None:
    klassifisering = "✅ BET"
else:
    klassifisering = "❌ NO BET"
    årsak = (
        f"Lav edge ({edge_norm*100:.1f}%)"  if edge_norm < EDGE_THRESHOLD
        else f"Tier = {tier}"               if tier not in ("ATOMIC","EDGE")
        else "omega_score mangler"
    )
```

Sanity-sjekk: logg (men ikke blokker) hvis `btts_yes`, `xg_home`, `xg_away`, eller `smart_bets` er `None`. Disse kan være `None` når xG-data ikke finnes — det er by design (`enrich_pick` returnerer `None` i stedet for fabrikert data).

Hvis alle picks er NO BET: poste `"📭 Ingen kvalifiserte picks i dag (edge ≥ 8% + tier ATOMIC/EDGE)"` til Telegram via `POST {API_BASE}/send-message` med `{"text": "..."}`.

## STEG 4 — TELEGRAM-VERIFISERING OG POSTING (09:00)

```
POST {API_BASE}/post-telegram
```

Dette endpointet poster selv upostede ATOMIC/EDGE picks (`edge >= 0.06`, daglig grense). Responsen:
- `{"status": "done", "posted_count": N, "results": [...]}` — OK
- `{"status": "no_qualified_picks", "reason": "..."}` — OK, ingen å poste
- `{"status": "skipped", "reason": "Daglig grense..."}` — OK, allerede postet
- `status_code != 200` eller `"error"`: prøv `POST {API_BASE}/test-telegram` for å verifisere bot-tilkobling.

Ikke poste manuelt via `/send-message` hvis `/post-telegram` allerede returnerte `posted_count > 0` — det dupliserer picks i kanalen.

## STEG 5 — LOGG TIL NOTION MATCH_PREDICTIONS

Bruk Notion MCP `API-post-page` mot `db4f2beb-a403-46a6-a8e2-6ef950c5b344` (data source ID, ikke database ID). For hver pick klassifisert `✅ BET` i STEG 3, opprett side med både legacy- og nye numeriske felter:

```json
{
  "parent": {"database_id": "541e9c96-b2e8-4424-9bfa-7bac08d97e79"},
  "properties": {
    "Name":         {"title":     [{"text": {"content": "{home_team} vs {away_team}"}}]},
    "Liga":         {"select":    {"name": "{league_clean}"}},
    "Hjemmelag":    {"rich_text": [{"text": {"content": "{home_team}"}}]},
    "Bortelag":     {"rich_text": [{"text": {"content": "{away_team}"}}]},
    "Kickoff":      {"date":      {"start": "{kickoff_time ISO}"}},
    "Pick":         {"rich_text": [{"text": {"content": "{our_pick}"}}]},
    "Odds":         {"number":    {odds}},

    "Edge":         {"rich_text": [{"text": {"content": "+{edge_norm*100:.2f}%"}}]},
    "Edge_pct":     {"number":    {edge_norm}},
    "EV":           {"rich_text": [{"text": {"content": "+{ev:.2f}%"}}]},
    "Confidence":   {"number":    {confidence_norm}},
    "Stake":        {"rich_text": [{"text": {"content": "{kelly_stake_pct:.1f}"}}]},
    "Kelly_stake":  {"number":    {kelly_stake_decimal}},

    "Omega_score":  {"number":    {omega_score}},
    "BTTS_yes":     {"number":    {btts_yes}},
    "xG_home":      {"number":    {xg_home}},
    "xG_away":      {"number":    {xg_away}},
    "Market_type":  {"rich_text": [{"text": {"content": "{market_hint or 'h2h'}"}}]},

    "Status":       {"select":    {"name": "PENDING"}}
  }
}
```

### Normalisering før skriving

```python
# Edge → raw desimal (0.08 = 8%)
edge_norm = pick["edge"] if pick["edge"] < 1 else pick["edge"] / 100

# Confidence → 0.0–1.0 for percent-format
conf_raw = pick.get("confidence") or 0
confidence_norm = conf_raw if conf_raw <= 1 else conf_raw / 100

# Kelly → raw desimal for Kelly_stake, prosentpoeng-streng for Stake (legacy)
kelly_obj = pick.get("kelly") or {}
kelly_stake_pct = float(kelly_obj.get("stake_pct") or 0)     # e.g. 2.5 (= 2.5%)
kelly_stake_decimal = kelly_stake_pct / 100                  # 0.025 for percent-format

# Liga → strip flag emoji
import re
league_clean = re.sub(r"[^\w\s-]", "", pick.get("league", "")).strip()
```

### Nullable-felter

`BTTS_yes`, `xG_home`, `xG_away`, `Omega_score`, `Kelly_stake` kan være `None` fra `/picks` (by design — `enrich_pick` returnerer `None` når xG-data mangler). Hopp over property-en hvis verdien er `None` — ikke send `{"number": null}` fordi Notion avviser det for eksisterende rader. Kun inkluder nøkkelen hvis verdien finnes.

### Liga-validering

`Liga`-select-options i Notion: `La Liga, Serie A, Eredivisie, Premier League, Bundesliga 2, Süper Lig, La Liga 2, Serie B` (+ TEST, STRONG legacy). Hvis pickens `league` ikke matcher noen av disse, utelat `Liga`-property (heller enn å få 400-feil). Logg warning.

Alternativ: backend-endepunktet `POST {API_BASE}/add-pick` logger picks til DB + Notion automatisk via `_log_notion_pick` (kun legacy-felter). For morgenritualet anbefales direkte MCP-kall så vi får begge feltsettene.

## STEG 6 — BANKROLL-OPPDATERING

`/bankroll` leser fra Postgres-tabellen `bankroll`, ikke fra Notion. Det finnes ingen `NOTION_BANKROLL_ID` i koden. Velg én av to veier:

1. **Rapportér bare** total Kelly-eksponering fra `sum(pick.kelly.stake_pct for pick in BET-picks)` i dagens output. Ikke skriv noe.
2. Hvis brukeren har satt opp en egen Bankroll Notion-database utenfor koden, be ham oppgi database-ID før du prøver å logge. Ikke gjett.

Default i denne skill-en: bare rapportér totalen i STEG 7. Ikke skriv til Notion.

## STEG 7 — DAGLIG RAPPORT

Hent Phase 0 status fra `GET {API_BASE}/dashboard/stats` → `live.phase0_picks` og `live.hit_rate`.

```
═══════════════════════════════
🟢 SesomNod Morgenrapport {YYYY-MM-DD}
═══════════════════════════════
Railway:              ✅ Online (db: {db.connected})
Picks i vinduet:      {total} ({bet_count} kvalifiserte BET)
Edge-terskel:         ≥ 8% + tier ATOMIC/EDGE
Total Kelly-eksp.:    {sum_kelly:.1f}%
Telegram:             {telegram_status} (postet: {posted_count})
Notion:               ✅ {notion_logged} picks logget  ({notion_errors} feil)
Phase 0:              {phase0_picks}/30 picks (hit rate: {hit_rate*100:.1f}%)
API budget:           {api_calls_this_month}/{api_budget_monthly}
═══════════════════════════════
```

Hvis noen steg feilet, list årsakene etter rapporten. Ved `🛑 STOPP` i STEG 1, rapportér kun feilmelding.
