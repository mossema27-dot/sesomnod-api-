---
name: sesomnod-deploy
description: SesomNod deploy-protokoll for Railway (backend) og Netlify (frontend). Kirurgisk sekvens med diff-sjekk, syntaksjekk, og live /health-verifisering. Bruk når brukeren sier "deploy", "push", "release", "ship", eller vil pushe endringer til produksjon.
---

# SesomNod Deploy Protocol

## BACKEND (sesomnod-api → Railway)
Kjøres fra `~/sesomnod-api/`:

1. `python3 -m py_compile main.py` — syntaksjekk
2. Vis diff: `git diff`
3. `git add -A && git commit -m "beskrivende melding"`
4. `git push origin main`
5. Vent 90 sekunder
6. Verifiser: `curl -s https://sesomnod-api-production.up.railway.app/health | python3 -m json.tool`

## FRONTEND (React/Vite → Netlify)
Kjøres fra frontend-mappen:

1. `npm run build` — MUST BE GREEN
2. Sjekk bundle: `ls -la dist/assets/*.js | tail -3`
3. `npx netlify-cli deploy --prod --dir=dist`
4. Verifiser bundle-hash er ny
5. Test i inkognitovindu

## ABSOLUTT REGEL
- Aldri deploy backend og frontend i samme operasjon.
- Aldri deploy uten å ha sett diff.

## LIVE STATUS
```bash
curl -s https://sesomnod-api-production.up.railway.app/health 2>/dev/null \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(f'API: {d[\"status\"]} | DB: {d[\"db\"][\"connected\"]} | v{d[\"version\"]}')" \
  2>/dev/null || echo "API ikke tilgjengelig"
```
