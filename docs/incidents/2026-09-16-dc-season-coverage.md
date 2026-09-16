# INCIDENT 2026-09-16 — Dixon-Coles: manglende sesongdekning 2026/27 og feilaktig delstreng-matching

Status: RETTELSE KLAR PÅ EGEN BRANCH (`fix/dc-season-2627`), IKKE DEPLOYET. Ingen Sniper-port er endret.

## Symptom
Sniper (services/sniper_live.py) skanner 290 Big5-kamper per helg men avviser nesten alle: 0 picks siden 2026-09-10, 13 picks på 35 dager.
9–13 Big5-kamper per serierunde havner i Dixon-Coles-fallback fordi modellen ikke kjenner laget (`fallback_used=True`, «119 teams in dataset»).

## Rotårsak (verifisert read-only 2026-09-16)
1. **Sesongdekning.** `services/football_data_fetcher.LEAGUE_URLS` hadde 15 CSV-er (2023/24–2025/26). 2026/27-filene (`mmz4281/2627/{E0,SP1,D1,I1,F1}.csv`)
   finnes hos football-data.co.uk (27–51 rader, siste kamp 13.–14. sept) men var ikke lagt inn. Ti opprykkede lag manglet i lagsettet:
   Coventry, Hull (PL) · La Coruna, Malaga, Santander (La Liga) · Elversberg, Paderborn, Schalke 04 (Bundesliga) · Le Mans, Troyes (Ligue 1). Serie A: ingen.
2. **Diakritika.** `team_normalizer.NORMALIZATION_MAP` slo opp på råstreng: «Atlético Madrid» (Sniper skriver om til aksent, sniper_live ~l. 356),
   «Bayern München» og «1. FC Köln» fant aldri sine datasett-navn (Ath Madrid, Bayern Munich, FC Koln). Alle Bayern-kamper var uskårbare.
3. **Delstreng-matching.** `find_best_team_match` falt tilbake på «delstreng i begge retninger». Over 10 523 prod-lagnavn (api_football_cache siden 1. aug)
   ga det 431 falske treff utenfor Big5: Inter ×45 (Inter San Carlos, Internacional …), Angers ×27, Lens ×14 (Nublense, LD Alajuelense), Arsenal ×11
   (Arsenal U21), Liverpool Montevideo → Liverpool, Lillestrom → Lille, Everton de Vina → Everton, Remo → Cremonese, ADT → Darmstadt. Disse forurenset
   SHADOW_GLOBAL-prediksjoner med Big5-parametre for helt andre klubber.

## Påvirkning (målt på 196 distinkte Big5-kamper 2026-08-19..09-16 fra prod-cache)
- Skårbare kamper: **154 → 196 (79 % → 100 %)**. 42 nye, hvorav 6 Bayern/Köln-kamper som bare manglet på grunn av ö.
- Sannsynlighetsskift på de 154 felles kampene ved å legge til 194 kamper fra 2026/27 (høyest tidsvekt): gj.snitt |Δ P(over 2,5)| 0,027, maks 0,106, 22 kamper > 0,05.
- Modellfit: 5 256 → 5 450 kamper, 119 → 129 lag, 0,9 s → 2,0 s (penaltyblog 1.12.2, Python 3.11, isolert kjøring på Mac).

## Rettelse (denne branchen)
- `services/football_data_fetcher.py`: fem 2627-URL-er (20 CSV-er), docstring, feilmelding uten hardkodet «10».
- `services/team_normalizer.py`: NFKD-folding (ö/ø/é), ordbasert entydig matching (alle datasett-ord må finnes som hele ord, resten må være rene
  klubbtype-ord som fc/sc/04/borussia), ingen delstreng, ingen kortform-gjetting, flere kandidater → None. Aliaser for 2026/27-opprykk og
  diakritiske varianter. `real`, `deportivo`, `racing`, `united`, `city`, `town` er bevisst ikke generiske: «Real Santander», «Deportivo Español»,
  «Coventry United», «Malaga City», «Celta de Vigo II», «Real Sociedad II» får ingen match.
- `tests/test_team_normalizer.py` (11 tester): diakritika, alle ti opprykkslag, Internacional ≠ Inter, Remo ≠ Cremonese, Inter Miami ≠ Inter,
  Paris/Madrid/United/Frankfurt → None, ukjente ligaer → None, alle 160+ aliaser løses, alle universlag løses til seg selv, og alle Big5-navn
  API-Football faktisk leverte i august–september (tests/fixtures/api_football_big5_names_2026-09.json) løses entydig.
- Motoren (`dixon_coles_engine.py`) og Sniper-portene er urørt. Isolert sammenligning gammel→ny matcher: Big5 12 gevinster, 0 tap; andre ligaer 431 falske
  treff fjernet; nye treff utenfor Big5 er samme klubber i DFB Pokal, League Cup, UCL og Ligue 2 (St Etienne).

## Kjent restrisiko (krever eierbeslutning, ikke gjort)
Opprykkslagene har 3–5 kamper i datasettet. Parametrene er støyende: attack −2,475 (Coventry, Paderborn), +1,815 (Elversberg). Det gir
P(over 2,5) fra 0,11 (Paderborn v Freiburg) til 0,86 (Elversberg v Leverkusen). Med Snipers uendrede port (edge ≥ 9 %) kan slike ekstremverdier
gi PRIMARY-picks bygget på tre kamper. Forslag: minstekrav på antall kamper per lag (f.eks. 6, dvs. ca. to serierunder til) før modellen brukes,
ellers fallback som i dag. Det er en ny, strammere port i motoren og tas som egen beslutning. Uten port: parametrene korrigerer seg ukentlig.

## Verifisering etter deploy (når eier bestemmer)
Railway-logg «Dixon-Coles model fitted. 129 teams» ved oppstart · `sniper_scan_log.raw_stats.rejected_model − shadow_team_mismatches_logged` ≈ 0 på
Big5 neste fredag/lørdag · ingen nye picks på lag med < 6 kamper uten eierens aksept.

## Rollback
`git revert` av commiten på denne branchen + redeploy. Ingen persistent tilstand endres av rettelsen (modellen bygges om ved oppstart og hvert døgn).
