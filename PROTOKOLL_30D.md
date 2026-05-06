# 30-DAGERS PROVE-REALITY-PROTOKOLL

**Versjon:** 1.0
**Aktivert:** 5. mai 2026 18:30 Oslo
**Slutt:** 4. juni 2026 18:30 Oslo
**Eier:** Don

## DOKTRINE

SesomNod er i "prove reality exists"-fase, ikke "build company"-fase.
Vi optimaliserer for stabil positiv median CLV, ikke ROI eller
volum alene.

Sniper-systemet er eneste ekte produkt-kandidat. Atomic-pipeline
er locked under protokoll.

## DAGLIG RUTINE

Hver dag kl 18:00 Oslo:
- curl /admin/sniper-30day-protocol-status
- Logg verdict
- Ingen frontend-aktivitet
- Ingen tekstendringer
- Ingen B0/B1/B2-diskusjon

Hver dag kl 22:00 Oslo:
- scheduler-job kjører /admin/sniper-snapshot-capture automatisk
- Don kan verifisere med /admin/sniper-snapshot-history?days=7

## SJEKKPUNKTER

- Dag 1 (6. mai 18:00): bekreft minst én pick generert eller
  diagnose volum-bottleneck
- Dag 3 (8. mai 18:00): baseline median CLV fanges
- Dag 7 (12. mai 18:00): early-kill-vurdering vs dag 3
- Dag 14 (19. mai 18:00): halvveis-dom + trend-analyse
- Dag 21 (26. mai 18:00): pre-final review
- Dag 30 (4. juni 18:00): GO / KILL / EXTEND

## EXIT-VEIER (på dag 30)

GO (alle 4 suksess-kriterier passert):
- Bygg public proof-side med rådata-eksport
- Revider frontend-narrativ til sniper-fokus
- Begynn forberedelse til Open Oraklion Telegram-publishing
- Aktiver bot_simulator_state mot live picks

KILL:
- Skru av sniper_pick_generation-jobs
- Behold infrastruktur for fremtidig modell-debug
- Dokumenter eksakt hvilket kriterium som feilet
- 7-dagers post-mortem-pause før neste pivot

EXTEND (n<15 = diagnose):
- Identifiser om bottleneck er sesong, modell, tracking, eller bug
- Forleng med 30 dager hvis sesong (Champions League/EM-quals/sommerturneringer)
- Aksepter at edge-bevis tar lengre enn forventet

## LOCKED RESSURSER

- Sesomnod.com: Locked Draft fortsatt aktiv (per 4. mai)
- "+14.62% ROI 149 signals"-claim: skal merkes "Backtest,
  out-of-sample" når narrativ-revisjon kommer (ikke i denne runden)
- Open Oraklion Telegram-publishing: bygges som draft-only nå,
  publishes etter protokoll passerer
- Bot-simulator: replay-only nå, live-tracking etter protokoll passerer
