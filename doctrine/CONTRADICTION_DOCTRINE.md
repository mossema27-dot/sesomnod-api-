# Contradiction Doctrine — SesomNod v1

## Definisjoner

### Svake motsigelser (logg, ignorer i scoring)
- H2H peker annen vei enn form
- Mediesentiment mot markedsretning
- Værmeldinger (kun ekstrem vær teller)
- Historisk over/under rate >2 sesonger gammel

### Medium motsigelser (legg til WARN, vekt i NBPI)
- Line movement >8% mot oss siste 12t
- Sharp money indikator peker mot oss
- xG form siste 5 divergerer fra modell-output
- Toppscorer ute (ukonfirmert)

### Kritiske motsigelser (trigger REVIEW automatisk)
- Closing line beveger seg >15% mot oss
- Bekreftet nøkkelspiller ute som modell ikke hadde
- Sharp alignment score <35
- MiroFish SSR <40% (flertall scenarioer overlever ikke)

### Dødelige motsigelser (trigger NO-BET automatisk)
- 2+ kritiske motsigelser samtidig
- Lineup revolusjon (5+ endringer fra forventet)
- Odds beveger seg >25% mot oss
- NBPI >80

## Contradiction density score
0 = ingen motsigelser
1-2 svake = score 10-20
1 medium = score 30
2 medium = score 50
1 kritisk = score 65
2+ kritisk = score 80+ (NO-BET)

## Logging og kalibrering
Post-match: hvilke contradictions predikerte tap?
Ukentlig: juster contradiction density weights.
