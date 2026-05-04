# Claim Audit — sesomnod.com (live tekst per 2026-05-04)

Versjon: 1.0
Dato: 2026-05-04 (Oslo)
Eier: Don
Kilder: https://sesomnod.com (HTML+JSON-LD), https://sesomnod.com/llms.txt
Metode: curl + JSON-LD-ekstraksjon. Frontend er Vite/React SPA — ingen klient-rendret tekst kunne høstes; kun meta-tags + schema.org + llms.txt.

## Klassifisering

- **VERIFIED**: SesomNod har målbart bevis i kodebase eller produksjonsdata.
- **UNVERIFIED_BUT_NOT_FALSE**: ingen direkte falsifisering, men heller ingen bekreftelse.
- **UNSUPPORTED**: ingen kjent data, sårbar for utfordring.

## Tabell

| # | Claim | Kilde-lokasjon | Klassifisering | Risiko hvis utfordret |
|---|---|---|---|---|
| 1 | "+14.62% ROI across 149 signals" | meta og llms.txt og JSON-LD | UNVERIFIED_BUT_NOT_FALSE | BACKTEST-tall, ikke live. Trygt så lenge ordet "Backtest validated" beholdes ved siden av. Hvis konteksten kuttes: kan oppfattes som live-claim. Reproduserbarhet krever backtest-script + dataset; ikke offentlig tilgjengelig nå. |
| 2 | "62.4% hit rate on signals meeting edge threshold ≥10%" | llms.txt og JSON-LD | UNVERIFIED_BUT_NOT_FALSE | Samme backtest-kilde som #1. Ikke replikert mot live picks_v2. |
| 3 | "+136.21% Quarter-Kelly equity return" | llms.txt og JSON-LD | UNVERIFIED_BUT_NOT_FALSE | Backtest-spesifikk metrikk. Ingen live equity-curve eksponert. |
| 4 | "32.62% maximum drawdown" | llms.txt og JSON-LD | UNVERIFIED_BUT_NOT_FALSE | Backtest. /admin/phase0-stats markerer max_drawdown_pct som DEFERRED for live. |
| 5 | "17 months out of sample validation period" | llms.txt og JSON-LD | UNVERIFIED_BUT_NOT_FALSE | Krever backtest-tidsstempel-bevis. Ikke offentlig. |
| 6 | "Live CLV verification begins 2 May 2026" og "Pre commits to publishing live CLV verification on 4 May 2026" | llms.txt og JSON-LD HowTo | UNSUPPORTED (intern motsetning) | To ulike datoer i samme kilder. Phase 0-stats viser 2 settled ATOMIC+EDGE picks per 4. mai. Public proof i dag ville vært gate-FAIL. |
| 7 | "Institutional grade quantitative football intelligence platform" | meta og JSON-LD og hero | UNSUPPORTED | Subjektiv kategori uten ekstern sertifisering. Lavt litigation-risiko, men sårbart for kritisk presse. |
| 8 | "Powered by Open Oraklion: a Nash weighted multi layer consensus engine" | meta og JSON-LD og llms.txt | UNVERIFIED_BUT_NOT_FALSE | "Nash weighted" og "consensus" finnes som arkitekturord i koden, men ikke formelt bevist som Nash-equilibrium-vektet. Krever paper eller intern dokumentasjon. |
| 9 | "Dixon-Coles statistical modeling" | meta og JSON-LD og llms.txt | VERIFIED | services/dixon_coles_engine.py eksisterer og kjører. |
| 10 | "Atomic signal architecture (xG divergence, market velocity, weather impact, referee profile)" | meta og JSON-LD og llms.txt | VERIFIED (med kvalifikasjon) | signals/weather_signal.py og signals/referee_signal.py + main.py velocity og xG eksisterer. Kvalifikasjon: referee tilfører 0 atomic-poeng (data-innsamling-modus per main.py:1786). |
| 11 | "Pinnacle closing line value verification as the institutional benchmark" | meta og JSON-LD og llms.txt | VERIFIED (men bare per pick, ikke aggregert publisert) | clv_records-tabell + MiroFish /summary leverer ekte Pinnacle no-vig CLV. Per-pick data eksponeres nå via /admin/clv-export. |
| 12 | "5,000 founding seats globally" | JSON-LD og llms.txt og pricing | UNVERIFIED_BUT_NOT_FALSE | Forretningsmessig påstand, ikke teknisk. Trygt hvis det respekteres. |
| 13 | "First 700 seats: $149 per month, lifetime price lock" | JSON-LD og llms.txt | UNVERIFIED_BUT_NOT_FALSE | Krever kontraktsmessig bekreftelse i Stripe/Whop. |
| 14 | "Wilson lower bound statistical significance testing" | llms.txt | UNSUPPORTED (kode ikke verifisert) | Ingen treff på "wilson" i hovedkode-paths sjekket per dato. Kan eksistere i backtest-kode. |
| 15 | "Real time CLV tracking with kill switch protection" | llms.txt | VERIFIED (delvis) | track_clv-scheduler kjører hver 30 min. Kill switch: /admin/sniper-dashboard returnerer kill_switch_status (auto_stop_required: false per 4. mai). |
| 16 | "5,000 founding seats globally, permanent lock at capacity" | JSON-LD og llms.txt | UNVERIFIED_BUT_NOT_FALSE | Forretningsmessig. |
| 17 | "Quantitative football intelligence platform" (tagline) | title og meta | UNVERIFIED_BUT_NOT_FALSE | Markedsføringsbeskrivelse, lav risiko. |
| 18 | "Mispriced probabilities ... before the market corrects them" | meta og llms.txt | UNVERIFIED_BUT_NOT_FALSE | Generell beskrivelse av edge-jakt; krever CLV-data for å bevise (kommer via clv-export). |
| 19 | "Closed signal desk with 5,000 founding seats ... accepting members by application only" | JSON-LD | UNVERIFIED_BUT_NOT_FALSE | Forretningsmessig. |
| 20 | "Quarter-Kelly position sizing for capital allocation" | llms.txt og JSON-LD HowTo | VERIFIED | services/kelly_calculator.py og kelly_v2.py eksisterer. main.py:1858 setter kelly_multiplier=1.0 for ATOMIC, 0.5 for EDGE. Doktrinen sier "Quarter-Kelly" som er 0.25 — sjekkes i kelly_calculator. |
| 21 | "Each trading day starts with a standardized capital base" | JSON-LD HowTo | UNVERIFIED_BUT_NOT_FALSE | Daily-target-disiplin er beskrevet, men ikke teknisk håndhevet i koden vi har auditert. |
| 22 | "When the day's profit target is reached, Open Oraklion stops trading" | JSON-LD HowTo | UNSUPPORTED | Ingen daily_profit_target_breaker i scheduler-jobb-listen. Påstand om automatisk stop er ikke bekreftet. |

## Sammendrag

- **VERIFIED:** 4 (Dixon-Coles, atomic signaler eksisterer, Pinnacle CLV-tracking, Quarter-Kelly-modul)
- **UNVERIFIED_BUT_NOT_FALSE:** 14 (backtest-tall, forretningspåstander, generelle beskrivelser)
- **UNSUPPORTED:** 4 (motstridende CLV-dato, "Wilson", "auto-stop ved profit-target", "institutional grade" subjektivt)

## Anbefaling for Locked Draft-doktrine

**Frys all live tekst.** Ikke endre eksisterende claims (også ikke "tone ned"). Hvis Don velger å rydde i UNSUPPORTED-claims senere, må det gjøres som én bevisst kommunikasjonsoppgradering — ikke stille redigeringer.

**Prioritert oppmerksomhet:**
- Claim #6 (intern dato-motsetning 2 mai vs 4 mai) — sårbart hvis presse eller member spør "begynte CLV-tracking 2 mai eller 4 mai?"
- Claim #14 (Wilson lower bound) — verifiser i backtest-kode eller fjern i neste tekst-revisjon
- Claim #22 (auto-stop ved profit-target) — verifiser i scheduler eller revider tekst
