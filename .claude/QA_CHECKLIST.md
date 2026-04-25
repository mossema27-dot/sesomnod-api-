# SesomNod QA-sjekkliste
Kjøres FØR enhver deploy. Bestå alle for å shippe.

## VISUELT
- [ ] Intro-animasjon spiller (fotball spretter)
- [ ] PIN-gate vises (PIN: <stored in env / ask Don>)
- [ ] Glasskule animerer korrekt
- [ ] Stadium-seksjon intakt
- [ ] Ingen placeholder-tekst synlig
- [ ] Ingen "undefined" eller "null" i UI
- [ ] Kobber-aksent (#B87333) brukt korrekt
- [ ] Mørk bakgrunn (#0A0A0A) konsistent

## NAVIGASJON
- [ ] / laster uten feil
- [ ] /prism laster og viser data
- [ ] /results laster og viser historikk
- [ ] /proof laster og viser receipts
- [ ] /picks laster og viser aktive picks
- [ ] Navbar-lenker fungerer alle

## DATA
- [ ] /picks returnerer omega_score, btts_yes, smart_bets
- [ ] /dagens-kamp returnerer picks
- [ ] /dashboard/stats returnerer Phase 0-stats
- [ ] phase1_gate.gate_passed er korrekt (false til 30 picks)
- [ ] Ingen "NO_HISTORY" synlig i UI
- [ ] Edge-verdier er >0 på aktive picks

## PRISM
- [ ] Viser 5 gullkort
- [ ] Oraklion velger topp 1
- [ ] Analyse er kamp-spesifikk (ikke generisk)

## SIGNAL
- [ ] OMEGA-tier vises korrekt (BRUTAL/STRONG/MONITOR)
- [ ] CLV-tall stemmer med /dashboard/stats
- [ ] No-bet vises tydelig når edge <8%

## MOBIL
- [ ] Siden er lesbar på 375px bredde
- [ ] Ingen overflow-problemer
- [ ] Touch-targets er store nok

## SHIP STANDARD
Ingenting deployes med:
- Syntaksfeil i Python eller TypeScript
- Secrets i kode
- Intro/glasskule/stadium ødelagt
- "undefined"/"null" synlig i UI
- Phase 0-tall som ikke stemmer med ladder-history
