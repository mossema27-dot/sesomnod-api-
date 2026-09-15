"""
Oraklion Brain v2 — tynt ledger- og beslutningslag oppå sniper_bets_v1.

Pakken gjør INGEN eksterne API-kall. Kandidater, closing-odds og resultater
leses fra sniper_bets_v1 (som Sniper-jobbene allerede vedlikeholder).

Moduler:
  canonical  — event_public, kanonisk JSON, sha256-kjeding (ren Python)
  rules      — utvalgsregel v1, statusoverganger (ren Python)
  stats      — flat-statistikk, CLV, Brier (ren Python)
  ledger     — append_event / verify mot oraklion.brain_events (asyncpg)
  engine     — tick: skann → commit → oppgjør → snapshot → heartbeat (asyncpg)
  api        — /public/oraklion/brain og /public/oraklion/brain/ledger.json

Aktivering i prod skjer kun bak env-flagg BRAIN_V2_JOBS=on (default off).
"""

METHODOLOGY_VERSION = "v2"
SELECTION_RULE_VERSION = "v1"
SETTLEMENT_RULES_VERSION = "v1"
SCHEMA_VERSION = "v2"
