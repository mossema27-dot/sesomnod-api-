# False Confidence Doctrine — SesomNod v1

## Definisjon
En pick har false confidence når edge ser attraktiv ut
men hviler på skjøre, ukonfirmerte eller motstridende
antakelser. Systemet skal aktivt oppdage og flagge dette.

## Trigger-betingelser (én er nok)
- Lineup ukonfirmert <4t før kickoff + edge <15%
- Odds beveger seg >10% mot oss siste 6t
- EIS <60 (edge integrity svak)
- Core confidence HIGH men 3+ motsigende signaler
- xG form siste 3 kamper divergerer >30% fra sesong-snitt
- Dossier completeness <65%

## Respons per trigger
| Trigger | Respons |
|---------|---------|
| 1 trigger | Flagg WARN i output |
| 2 triggere | Downgrade confidence HIGH → MEDIUM |
| 3 triggere | Automatisk REVIEW |
| 4+ triggere | Automatisk NO-BET |

## Logging
Alle false confidence-events logges i pick_intelligence_log.
Post-match: marker om flagget var korrekt eller støy.
Ukentlig: kalibrerer hvilke triggers som er mest presise.

## Prinsipp
False confidence er farligere enn svak edge.
En pick med 8% edge og lav fragility er tryggere
enn en pick med 19% edge og 4 aktive FC-triggers.
