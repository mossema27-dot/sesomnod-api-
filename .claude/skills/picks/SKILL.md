---
name: picks
description: Hent, valider og presenter dagens picks mot Phase 0 gate (edge ≥ 8% + ATOMIC/EDGE tier). Klassifiserer hvert pick som BET eller NO BET med årsak.
---

# /picks — Hent og valider dagens picks

## Konstanter

```
API_BASE        = https://sesomnod-api-production.up.railway.app
EDGE_THRESHOLD  = 0.08      # 8% — normalisert desimal
HIGH_TIERS      = {"ATOMIC", "EDGE"}
```

## Datakilde (verifisert mot main.py:3955)

`GET {API_BASE}/picks` returnerer `{status, data, count}`. `data[i]` er enriched pick fra `dagens_kamp` med disse feltene:

```
id, match_name, home_team, away_team, odds, edge, ev,
atomic_score, omega_score, omega_tier, tier,
market_hint, our_pick, league, kickoff_time, kickoff_cet,
confidence, signal_xg, xg_divergence_home, xg_divergence_away,
btts_yes, btts_no, xg_home, xg_away, lambda,
home_win_prob, draw_prob, away_win_prob,
over_05, over_15, over_25, over_35, over_45, under_25,
smart_bets, dixon_coles, kelly, xgboost,
live_score, minute, is_live,
result, closing_odds, clv, pinnacle_h2h
```

## Viktig kontekst

- **Edge-magnitude er inkonsistent.** Normaliser alltid: `edge_norm = edge if edge < 1 else edge / 100`.
- **`confidence` er et tall, ikke en streng.** Bruk `tier in HIGH_TIERS` som HIGH-confidence-proxy — det er samme gate som `/post-telegram` i prod.
- **`btts_yes`, `xg_home`, `xg_away` kan være `None` by design** (`enrich_pick:3782–3791`). Ikke behandle som feil.
- **`edge` reflekterer `soft_edge`** fra `enrich_pick` (linje 3795) — det er den samme verdien `/post-telegram` filtrerer på.
- **Notion MATCH_PREDICTIONS har utvidet schema (2026-04-08)** — nå med numeriske felter `Edge_pct`, `Kelly_stake`, `Omega_score`, `BTTS_yes`, `xG_home`, `xG_away`, `CLV`, `Closing_odds`, og rich_text `Market_type`. Status-select har også `WIN`, `LOSS`, `PUSH`. /picks leser kun; /morning og /log skriver til dette schemaet.

## Steg

1. Kall `GET {API_BASE}/picks` med retry (maks 3, 2s mellom, >5s = `⚠️ TREG`).
2. Hvis `status != "ok"`: skriv `❌ /picks returnerte {status}: {error}` og stopp.
3. Hvis `data` er tom: skriv `Ingen kamper i vinduet (NOW()-1h .. NOW()+36h).`
4. For hvert pick, klassifiser:

```python
edge_norm = pick["edge"] if pick["edge"] < 1 else pick["edge"] / 100
tier      = pick.get("tier") or pick.get("omega_tier")

if edge_norm >= 0.08 and tier in ("ATOMIC", "EDGE"):
    verdict = "BET"
else:
    if edge_norm < 0.08:
        reason = f"Lav edge ({edge_norm*100:.1f}%)"
    elif tier not in ("ATOMIC", "EDGE"):
        reason = f"Tier = {tier or 'MONITORED'}"
    else:
        reason = "Ukjent"
    verdict = "NO BET"
```

## Presentasjon

For hvert `BET`:

```
✅ BET
├─ Kamp:   {home_team} vs {away_team}  ({league})
├─ Market: {market_hint or our_pick}
├─ Odds:   {odds:.2f} | Edge: {edge_norm*100:.1f}% | Kelly: {kelly.stake_pct or "—"}%
├─ Omega:  {omega_score} ({tier}) | xG: {xg_home or "—"}–{xg_away or "—"}
├─ BTTS:   {btts_yes or "—"}% | Over 2.5: {over_25 or "—"}%
└─ Smart:  {smart_bets[0].edge_label if smart_bets else "—"}
```

For hvert `NO BET` (komprimert):

```
❌ NO BET: {home_team} vs {away_team} — {reason}
```

## Oppsummering (alltid sist)

Hent Phase 0 fremgang fra `GET {API_BASE}/dashboard/stats` → `live.phase0_picks`.

```
━━━━━━━━━━━━━━━━━━━━
Total:    {len(data)} picks analysert
✅ BET:   {bet_count} kamper
❌ NO BET: {no_bet_count} kamper
Phase 0:  {phase0_picks}/30 picks
━━━━━━━━━━━━━━━━━━━━
```

## Retry / feilhåndtering

- Timeout >5s: fortsett men merk `⚠️ TREG`.
- 5xx eller `{"status": "error"}`: skriv `❌ FEIL: {error[:200]}` og stopp.
- 401/403: `🚨 Autentiseringsfeil — sjekk Railway env vars`.
- 503 (`{"status": "offline"}`): `❌ DB offline — sjekk Railway`.
