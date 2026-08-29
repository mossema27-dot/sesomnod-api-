# SesomNod Deploy Policy

Versjon: 1.0
Dato: 2026-05-04
Eier: Don

## LOCKED DRAFT-DOKTRINE (gjeldende fra 4. mai 2026)

Ingen ny tekst-deploy til sesomnod.com før følgende er sant:
- ≥20 settled picks generert av atomic-pipelinen
  (scan_session NOT NULL + atomic_score > 0)
- pinnacle_no_vig_close populated for alle 20+
- CLV-snitt verifisert mot Pinnacle (ekte close, ikke estimat)
- Don har eksplisitt sagt "deploy proof"

## TILLATT UTEN "DEPLOY PROOF"
- Backend bug-fixes som ikke endrer claims
- Backend observability/admin endpoints (read-only, additive)
- Frontend bug-fixes som ikke endrer tekst eller tall
- Visuell oppgradering så lenge ingen tekst eller numerisk claim endres

## IKKE TILLATT UTEN "DEPLOY PROOF"
- Ny tekst på sesomnod.com (alle pages)
- Endring av eksisterende claims (også "tone ned" — Don beslutter)
- Numeriske counters mot mock-data
- Ny SEO-tagline eller meta-description med påstander
- Ny llms.txt eller schema.org-claim

## EKSISTERENDE LIVE TEKST (per 30. apr 2026)
Status: FROZEN. Ikke endre, ikke utvide, ikke rollback uten
Don's "rollback X" eller "deploy proof".

## CLAIM-AUDIT
Se /Users/don/sesomnod-api/CLAIM_AUDIT_2026_05_04.md
Hver claim klassifisert: VERIFIED / UNVERIFIED_BUT_NOT_FALSE / UNSUPPORTED.

## OVERTREDELSE
Hvis denne policyen brytes (av Claude Code, Kimi, Manus,
Higgsfield, eller annen AI/operator), Don ruller tilbake
umiddelbart og krever post-mortem.

---

## DEPLOY-MEKANIKK (tillegg 29. aug 2026)

### Produksjon serveres via Cloudflare Pages
sesomnod.com serveres av Cloudflare Pages, prosjekt `sesomnod`,
Production-branch `main`. Deploy skjer med wrangler fra bygget `dist/` —
aldri git-basert auto-deploy.

    cd /Users/don/sesomnod
    npm run build
    npx wrangler pages deploy dist --project-name=sesomnod --branch=main

### NETLIFY — UTDATERT
Netlify brukes IKKE lenger for sesomnod.com. Alle Netlify-prosedyrer i
dette repoet og i frontend-repoet er historikk og skal ikke følges:

  - sesomnod/netlify.toml                          (utdatert, ikke i bruk)
  - .claude/skills/frontend-deploy/SKILL.md        (Netlify-steg utdatert)
  - .claude/skills/sesomnod-deploy/SKILL.md        (Netlify-steg utdatert)
  - .claude/agents/{frontend-builder,release-guard,browser-qa}.md
  - .claude/APPROVAL_MATRIX.md

Ser du en Netlify-instruksjon: bruk wrangler-kommandoen over i stedet.

### ROLLBACK — Cloudflare Pages
Pages-deployments er immutable, og wrangler har ingen `rollback`-
subkommando. Rollback = bygg forrige commit på nytt og deploy den; den
nye deployen overtar produksjonstrafikken.

1. Finn commit å gå tilbake til (kolonnen `Source` er commit-SHA):

       cd /Users/don/sesomnod
       npx wrangler pages deployment list --project-name=sesomnod

2. Bygg og deploy den commiten:

       git stash                       # hvis work-tree er skitten
       git checkout <commit-sha>
       npm ci && npm run build
       npx wrangler pages deploy dist --project-name=sesomnod --branch=main
       git checkout main               # tilbake til main etterpå

3. Verifiser at produksjon serverer den nye bundelen:

       curl -s https://sesomnod.com/ | grep -oE 'main-[^"]+\.js'
       ls dist/assets/ | grep -E '^main-.*\.js$'      # må være samme hash

Alternativ uten CLI: Cloudflare-dashboard → Pages → sesomnod →
Deployments → «Rollback to this deployment» på ønsket rad.

### Kjente deployments (nyeste først, per 29. aug 2026)
    46856178  0e9af88   pre-gate landing, Eye gated, telemetry av
    00f76f0d  91111f3   /proof + /results pensjonert
    b576435a  af25205   NOK-rester nøytralisert

### Backend
sesomnod-api deployes av Railway på push til `origin/main`.
Rollback: `git revert <hash> && git push` — Railway redeployer selv.
Verifiser alltid `/health` = 200 etterpå.
