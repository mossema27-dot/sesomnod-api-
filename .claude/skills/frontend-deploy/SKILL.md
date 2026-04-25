---
name: frontend-deploy
description: SesomNod frontend-deploy til Netlify. Kirurgisk sekvens — build → diff → deploy → verifisér live. Bruk når brukeren sier "deploy frontend", "push frontend", eller vil pushe sesomnod-frontend endringer.
---

# Skill: frontend-deploy

Trigger: når frontend-endringer skal til Netlify (https://sesomnod.netlify.app).

## Sjekkliste FØR deploy
- [ ] `npm run build` kjørt uten feil
- [ ] Intro-animasjon finnes i dist/
- [ ] PIN-gate finnes i dist/
- [ ] Glasskule finnes i dist/
- [ ] Stadium-seksjon finnes i dist/
- [ ] ROLLBACK_POINT tag satt i git (hvis frontend er git-repo)

## Deploy-sekvens
```bash
cd ~/sesomnod
git add -A 2>/dev/null || true
git diff --staged 2>/dev/null || true   # vis til bruker — vent på OK
git commit -m "feat: [beskriv endring]" 2>/dev/null || true
npm run build 2>&1                       # vis output — stopp ved feil
npx netlify-cli deploy --prod --dir=dist
curl -s https://sesomnod.netlify.app | head -20   # verifiser live
```

## Aldri gjør
- Deploy uten `npm run build`
- Deploy hvis intro / PIN / glasskule / stadium mangler i dist/
- Deploy uten å vise diff til Don først
- Overskriv eksisterende intro/glasskule/stadium-kode
- Deploy sesomnod-api og frontend i samme operasjon

## Kontekst
- Frontend er IKKE et git-repo (per CLAUDE.md) — derfor er `git`-stegene best-effort
- PIN-gate: `<stored in env / ask Don>`
- Farger / struktur er hellig — røres ikke uten eksplisitt godkjenning
