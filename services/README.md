# Services — SesomNod API

Oversikt over alle service-moduler. Alle services er async, har type hints og bruker asyncpg/httpx.

## Kjernemodeller (ML / prediksjon)
| Fil | Ansvar | Kritisk? |
|-----|--------|----------|
| [dixon_coles_engine.py](dixon_coles_engine.py) | Dixon-Coles probability engine, penaltyblog, 24h cache, ~3000 kamper historikk | JA |
| [xgboost_model.py](xgboost_model.py) | XGBoost outcome-modell med SHAP, lagret i Postgres bytea | JA |
| [xgb_training.py](xgb_training.py) | XGBoost training pipeline fra football-data.co.uk CSVs | NEI |
| [football_data_fetcher.py](football_data_fetcher.py) | Henter historiske kamper fra football-data.co.uk, 10 CSVs (5 ligaer × 2 sesonger) | JA |
| [backtest_engine.py](backtest_engine.py) | Rolling Poisson backtest vs Bet365, ingen lookahead | NEI |

## Kelly / stake-sizing
| Fil | Ansvar | Kritisk? |
|-----|--------|----------|
| [kelly_calculator.py](kelly_calculator.py) | Half-Kelly, max 25% bankroll | JA |
| [kelly_v2.py](kelly_v2.py) | Kelly med confidence, volatility, correlation-justering | JA |

## Market / pick-utvalg
| Fil | Ansvar | Kritisk? |
|-----|--------|----------|
| [market_extractor.py](market_extractor.py) | Ekstraherer alle markeder fra Poisson P(i,j) matrise, NumPy-vektorisert | JA |
| [market_scanner.py](market_scanner.py) | Scanner 500+ kamper / 12 ligaer daglig, top 10 picks ranked | JA |
| [market_selection_engine.py](market_selection_engine.py) | Beste marked per kamp, composite scoring | JA |
| [pick_feature_extractor.py](pick_feature_extractor.py) | XGBoost-features fra live picks + historikk | NEI |
| [pick_formatter.py](pick_formatter.py) | Formaterer picks for Telegram / Notion / JSON API | JA |
| [no_bet_verdict.py](no_bet_verdict.py) | Logger rejected picks, backfiller verdict når kamp er ferdig | NEI |

## CLV / verifisering
| Fil | Ansvar | Kritisk? |
|-----|--------|----------|
| [atlas_engine.py](atlas_engine.py) | Auto-CLV Closer + Decision Quality Score, kun ekte MiroFish-data | JA |
| [receipt_engine.py](receipt_engine.py) | Genererer pick-receipts (hash + secrets) | JA |
| [metrics.py](metrics.py) | Prometheus gauges/counters for Phase 0 + backtest | NEI |

## MiroFish (Claude Haiku-basert AI-lag)
| Fil | Ansvar | Kritisk? |
|-----|--------|----------|
| [mirofish_agent.py](mirofish_agent.py) | 11-agent simulering via Claude API, ~$0.002/kamp, max 15% Omega-vekt | JA |
| [mirofish_client.py](mirofish_client.py) | HTTP-klient mot mirofish-service Railway-app | JA |
| [mirofish_v3.py](mirofish_v3.py) | 6-agent hierarkisk validering | JA |

## MiroFish Swarm V2 — [swarm/](swarm/)
| Fil | Ansvar |
|-----|--------|
| [swarm/consensus_engine.py](swarm/consensus_engine.py) | 100-agent konsensus-motor |
| [swarm/moat_engine.py](swarm/moat_engine.py) | CLV-læring, accuracy, recalibrering |
| [swarm/oraklion_schema.py](swarm/oraklion_schema.py) | Pydantic v2 output-schema validering |

## Eksterne integrasjoner / data
| Fil | Ansvar | Kritisk? |
|-----|--------|----------|
| [api_football.py](api_football.py) | API-Football fixture-fetcher, 100 req/dag budsjett, cache i DB | JA |
| [team_normalizer.py](team_normalizer.py) | Team-navn → football-data.co.uk-format (EPL, La Liga, Bundesliga, Serie A, Ligue 1) | JA |
| [context_engine.py](context_engine.py) | Justerer lambda basert på kontekstuelle faktorer | NEI |
| [model_storage.py](model_storage.py) | Persisterer modell-blobs til Postgres (overlever Railway redeploy) | JA |

## Telegram
| Fil | Ansvar | Kritisk? |
|-----|--------|----------|
| [the_operator.py](the_operator.py) | Intelligent Telegram-agent, max 2 msg/dag, forklarer WHY | JA |

## Regler
- Endre aldri en service uten å ha lest hele filen først
- Alle services har type hints og async — hold det slik
- Sjekk alltid om endring påvirker `/picks` eller `/dagens-kamp`
- ML-skjemaer (Dixon-Coles, XGBoost): endre aldri input-features uten koordinert retraining
- MiroFish-kall: alltid `asyncio.create_task()` fra request-handlers — aldri synkront
- Pinnacle no-vig er eneste gyldige CLV-referanse (atlas_engine)
