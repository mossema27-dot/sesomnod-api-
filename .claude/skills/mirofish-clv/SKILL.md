---
name: mirofish-clv
description: MiroFish v2.3 CLV orchestration — track picks, submit WIN/LOSS/PUSH + closing odds, check Phase 1 gate. Pinnacle no-vig closing odds er eneste gyldige CLV-referanse. Bruk når brukeren spør om CLV, Phase 1 gate, MiroFish, eller vil se /phase1-gate status.
---

# MiroFish v2.3 — CLV Orchestration

## LIVE STATUS
```bash
curl -s https://mirofish-service-production.up.railway.app/health 2>/dev/null \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(f'MiroFish: {d[\"status\"]} v{d.get(\"service\",\"?\")} | auth_enforced: {d.get(\"auth_enforced\",\"N/A\")} | db: {d.get(\"db_connected\",\"?\")}')" \
  2>/dev/null || echo "MiroFish ikke tilgjengelig"
```

## ENDEPUNKTER
- `POST /track` — logger pick ved Telegram-posting (upsert)
- `POST /clv` — submit WIN/LOSS/PUSH + closing odds etter kamp
- `GET /phase1-gate` — sjekk alle 4 Phase 1-gate-verdier
- `GET /summary` — full statistikk (v2.2 + v2.3 felter)
- `POST /poll-odds` — manuell poll av closing odds (ellers cron)

## CLV-FORMEL (industristandardens definisjon)
```
clv_pct = (odds_at_bet / pinnacle_closing_odds - 1) * 100
```
KUN Pinnacle no-vig closing odds er gyldig referanse. Ingen andre bokmakere.

## PICK_ID FORMAT (HELLIG — MÅ ALDRI ENDRES)
```
{home_slug}-{away_slug}-{YYYYMMDD}-{market_type}
```
Eksempel: `caykur-rizespor-samsunspor-20260409-h2h`

## PHASE 1 GATE STATUS
```bash
curl -s https://mirofish-service-production.up.railway.app/phase1-gate 2>/dev/null \
  | python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
    g = d.get('phase1_gate', {})
    print(f'CLV >2%: {g.get(\"clv_above_2pct\")} | Hit rate >55%: {g.get(\"hit_rate_above_55pct\")} | Picks >=30: {g.get(\"picks_above_30\")} | ALL PASSED: {g.get(\"all_gates_passed\")}')
except Exception as e:
    print(f'Kunne ikke lese gate: {e}')
" 2>/dev/null || echo "Gate ikke tilgjengelig"
```

## REGLER
- MiroFish-kall aldri blokkerende (alltid `asyncio.create_task()`)
- Aldri migrer `mirofish_clv`-tabellen uten eksplisitt godkjenning
- 57 picks med +3.28% CLV er Phase 1-beviset — integritet er ufravikelig
- Auth: hvis `MIROFISH_INTERNAL_KEY` er satt → send `X-Internal-Key` header
