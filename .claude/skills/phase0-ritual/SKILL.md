---
name: phase0-ritual
description: SesomNod Phase 0 daglig ritual og sjekkliste. Kjøres 08:45-09:10 hver morgen for å verifisere at alt er live, ingen edge < 8%, og no-bet-regelen holdes. Bruk når brukeren sier "morgensjekk", "phase 0", "dagens ritual", eller vil se dagens picks mot gate.
---

# Phase 0 Daglig Ritual

## MORGENSJEKK (08:45–09:10)

1. **Railway health**
   ```bash
   curl -s https://sesomnod-api-production.up.railway.app/health | python3 -m json.tool
   ```

2. **MiroFish health**
   ```bash
   curl -s https://mirofish-service-production.up.railway.app/health | python3 -m json.tool
   ```

3. **Phase 1 gate**
   ```bash
   curl -s https://mirofish-service-production.up.railway.app/phase1-gate | python3 -m json.tool
   ```

4. **Telegram**: verifiser at siste pick er postet korrekt

5. **Sanity check**: odds korrekt? edge >8%? confidence = HIGH?

## NO-BET REGEL (AUTOMATISK)
- edge < 8% = **NO BET**
- confidence ≠ HIGH = **NO BET**
- Omega < 55 = observasjon, ikke bet

## FASE STATUS
- Nå: **Phase 0** — 17 picks igjen til Phase 1
- Gate: CLV >+2% ✅ | hit rate >55% ⏳ | picks ≥30 ⏳
- Grafana: privat til 30 picks akkumulert

## PICKS COUNTER
```bash
curl -s https://sesomnod-api-production.up.railway.app/ladder-history 2>/dev/null \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(f'Picks logget: {d.get(\"total_picks\",\"?\")}/30')" \
  2>/dev/null || echo "Teller ikke tilgjengelig"
```
