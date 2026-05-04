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
