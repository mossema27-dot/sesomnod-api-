# SesomNod — Approval Matrix
# Gjelder alle agenter i .claude/agents/

## AUTO-APPROVED — agenten handler uten å spørre
- Lese filer, grep, curl mot live API
- Kjøre `python3 -m py_compile`
- Kjøre `npm run build` (ingen deploy)
- Produsere rapport, diff eller plan

## KREVER DON SIN REVIEW — agent forbereder, Don bestemmer
- Kodeendringer i hvilken som helst fil
- `npm install`, `pip install` av nye pakker
- `git add` + `git commit` (ikke push)
- Opprette nye filer eller mapper

## ALLTID DON SIN BESLUTNING — hard stop, aldri auto
- `git push` til main
- `netlify deploy --prod`
- Railway redeploy / restart
- Slette rader fra `picks_v2` eller `mirofish_clv`
- `git checkout` / `git reset` / `git revert` (rollback)
- Rotere API-nøkler

## Håndhevelse
- Hver agent må lese denne filen før første handling i en session
- Ved tvil: behandle som "alltid Don sin beslutning"
- Aldri eskaler rettigheter — agenter kan ikke gi hverandre tillatelse
