"""
SesomNod Engine v9.0 — CLV ARCHITECTURE EDITION
=================================================
- Smart API caching: fetch 2x/day only (07:00 + 14:00 UTC)
- Pinnacle-based EV/edge calculations
- SCORE = EV_pct × log(bookmaker_count + 1)
- CLV tracking with clv_records table
- 8 leagues, h2h + totals markets
"""

import sys
import os
from pydantic import BaseModel
from typing import Optional

class ResultUpdate(BaseModel):
    result: str
    closing_odds: Optional[float] = None

_original_exit = sys.exit
def _safe_exit(code=0):
    import logging
    logging.getLogger("sesomnod").critical(
        f"[FORTRESS] sys.exit({code}) blokkert! Appen fortsetter."
    )
sys.exit = _safe_exit

import asyncio
import json
import logging
import math
import time
import httpx
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta

import asyncpg
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("sesomnod")


def _safe_import(module_name: str):
    try:
        import importlib
        mod = importlib.import_module(module_name)
        logger.info(f"[Import] {module_name} lastet OK")
        return mod
    except Exception as e:
        logger.warning(f"[Import] {module_name} feilet: {e}")
        return None

bankroll_module = _safe_import("bankroll")
dagens_kamp_module = _safe_import("dagens_kamp")
auto_result_module = _safe_import("auto_result")


# ─────────────────────────────────────────────────────────
# KONFIGURASJON
# ─────────────────────────────────────────────────────────
SCAN_LEAGUES = [
    {"key": "soccer_epl",                                "name": "Premier League",         "flag": "🏴󠁧󠁢󠁥󠁮󠁧󠁿"},
    {"key": "soccer_spain_la_liga",                      "name": "La Liga",                "flag": "🇪🇸"},
    {"key": "soccer_germany_bundesliga",                 "name": "Bundesliga",             "flag": "🇩🇪"},
    {"key": "soccer_italy_serie_a",                      "name": "Serie A",                "flag": "🇮🇹"},
    {"key": "soccer_france_ligue_one",                   "name": "Ligue 1",                "flag": "🇫🇷"},
    {"key": "soccer_uefa_champs_league",                 "name": "Champions League",       "flag": "🏆"},
    {"key": "soccer_uefa_europa_league",                 "name": "Europa League",          "flag": "🇪🇺"},
    {"key": "soccer_netherlands_eredivisie",             "name": "Eredivisie",             "flag": "🇳🇱"},
    # International & second-tier — active during top-flight breaks
    {"key": "soccer_fifa_world_cup_qualifiers_europe",   "name": "WC Qualifiers Europe",   "flag": "🌍"},
    {"key": "soccer_uefa_nations_league",                "name": "UEFA Nations League",     "flag": "🇪🇺"},
    {"key": "soccer_spain_segunda_division",             "name": "La Liga 2",              "flag": "🇪🇸"},
    {"key": "soccer_england_championship",               "name": "Championship",           "flag": "🏴󠁧󠁢󠁥󠁮󠁧󠁿"},
    {"key": "soccer_germany_bundesliga2",                "name": "Bundesliga 2",           "flag": "🇩🇪"},
    {"key": "soccer_italy_serie_b",                      "name": "Serie B",                "flag": "🇮🇹"},
    {"key": "soccer_argentina_primera_division",         "name": "Primera División ARG",   "flag": "🇦🇷"},
    {"key": "soccer_brazil_campeonato",                  "name": "Campeonato Brasileiro",  "flag": "🇧🇷"},
    {"key": "soccer_portugal_primeira_liga",             "name": "Primeira Liga",          "flag": "🇵🇹"},
    {"key": "soccer_turkey_super_league",                "name": "Süper Lig",              "flag": "🇹🇷"},
]

# Topp-4 ligaer for kveldsscan (Vindu 2 — 18:00 UTC)
TOP4_LEAGUES = [
    {"key": "soccer_epl",                    "name": "Premier League",   "flag": "🏴󠁧󠁢󠁥󠁮󠁧󠁿"},
    {"key": "soccer_spain_la_liga",           "name": "La Liga",          "flag": "🇪🇸"},
    {"key": "soccer_germany_bundesliga",      "name": "Bundesliga",       "flag": "🇩🇪"},
    {"key": "soccer_uefa_champs_league",   "name": "Champions League", "flag": "🏆"},
]

# API-budsjett: 500 credits/mnd — 3-vindu plan: 8+4 calls/dag × 30 = 360/mnd
API_MONTHLY_BUDGET = int(os.getenv("API_MONTHLY_BUDGET", "480"))  # Maks calls/mnd (buffer mot 500)

EV_MIN              = float(os.getenv("EV_MIN", "1.5"))        # Legacy
EDGE_MIN            = float(os.getenv("EDGE_MIN", "1.5"))       # Legacy
CONFIDENCE_MIN      = int(os.getenv("CONFIDENCE_MIN", "65"))    # Fase 0: min confidence
MIN_BOOKMAKERS      = int(os.getenv("MIN_BOOKMAKERS", "3"))     # Fase 0: min antall bookmakers
SOFT_EDGE_MIN       = float(os.getenv("SOFT_EDGE_MIN", "0.5"))  # Dual Benchmark: min edge mot soft benchmark
SOFT_EV_MIN         = float(os.getenv("SOFT_EV_MIN", "0.5"))    # Dual Benchmark: min EV mot soft benchmark
BENCHMARK           = os.getenv("BENCHMARK", "unibet")           # Primær soft benchmark-bok (7/7 ligaer)
PINNACLE_CLV_TRACK  = os.getenv("PINNACLE_CLV_TRACK", "true").lower() == "true"
PINNACLE_EDGE_MIN   = 1.0    # Min edge mot Pinnacle (brukt kun i logging)
PINNACLE_MARGIN_MAX = float(os.getenv("PINNACLE_MARGIN_MAX", "8.0"))  # Max Pinnacle margin%
ODDS_MIN            = float(os.getenv("ODDS_MIN", "1.60"))      # Fase 0: under 1.60 = for lav verdi
ODDS_MAX            = float(os.getenv("ODDS_MAX", "4.50"))      # Fase 0: over 4.50 = for høy varians
MATCH_HOURS_MAX     = int(os.getenv("MATCH_HOURS_MAX", "24"))
DAILY_POST_LIMIT    = 10
MAX_PICKS_PER_MATCH = 2
MAX_PICKS_PER_LEAGUE = 3


def _clean(key: str) -> str:
    val = os.getenv(key, "")
    if val:
        val = (val.strip()
               .replace("\n", "").replace("\r", "")
               .replace("\t", "").replace("\x00", "")
               .replace("\ufeff", ""))
    return val


class Config:
    DATABASE_URL: str    = _clean("DATABASE_URL")
    TELEGRAM_TOKEN: str  = _clean("TELEGRAM_TOKEN")
    TELEGRAM_CHAT_ID: str = _clean("TELEGRAM_CHAT_ID")
    ODDS_API_KEY: str    = _clean("ODDS_API_KEY") or "6241bf29534eb1374817ce0f22463607"
    NOTION_TOKEN: str    = _clean("NOTION_TOKEN")
    NOTION_DB_ID: str    = _clean("NOTION_DATABASE_ID")
    NOTION_CHANGELOG_DB_ID: str = _clean("NOTION_CHANGELOG_DB_ID") or "fd23588f-5099-41c4-8292-ddf45b429d34"
    FOOTBALL_DATA_API_KEY: str = _clean("FOOTBALL_DATA_API_KEY") or "4583e0ae11fa4b64b836fd64c8819d95"
    OPENWEATHER_API_KEY: str   = _clean("OPENWEATHER_API_KEY") or "3610c10d23b33d9a2a7b4bc49eb6ea1a"
    PORT: int            = int(os.getenv("PORT", "8000"))
    ENVIRONMENT: str     = os.getenv("RAILWAY_ENVIRONMENT", "development")
    SERVICE_NAME: str    = os.getenv("RAILWAY_SERVICE_NAME", "sesomnod-api")

cfg = Config()

# Signal-klasser — aldri blokkerende ved import-feil
try:
    from signals.weather_signal import WeatherSignal
    from signals.referee_signal import RefereeSignal
    _SIGNALS_AVAILABLE = True
except Exception as _sig_err:
    logger.warning(f"[Signals] Import feilet: {_sig_err} — legacy gate brukes")
    WeatherSignal = None
    RefereeSignal = None
    _SIGNALS_AVAILABLE = False

# Core-moduler — aldri blokkerende ved import-feil
try:
    from core.kelly_engine import kelly_engine as _kelly_engine
    from core.rate_limiter import football_limiter, weather_limiter, odds_limiter
    from core.circuit_breaker import referee_breaker, weather_breaker
    _CORE_AVAILABLE = True
except Exception as _core_err:
    logger.warning(f"[Core] Import feilet: {_core_err} — fallback brukes")
    _kelly_engine = None
    football_limiter = None
    weather_limiter = None
    referee_breaker = None
    weather_breaker = None
    _CORE_AVAILABLE = False


# ─────────────────────────────────────────────────────────
# NO-BET MELDINGER (Operational Order #001)
# ─────────────────────────────────────────────────────────
NO_BET_MARKET_MOVED = (
    "No pick. "
    "Pinnacle 2.10→1.95. Unibet 2.10 (lagging). "
    "Edge removed by market movement. "
    "No advantage remains. "
    "Discipline = profit. Next scan: 18:00"
)

NO_BET_LOW_EDGE = (
    "No pick. "
    "Model probability: 52%. "
    "Required threshold: 55%. "
    "No positive EV detected. "
    "Capital preserved. "
    "Waiting for qualified opportunity."
)

NO_BET_HIGH_VARIANCE = (
    "No pick. "
    "Key variables unresolved. "
    "Lineups not confirmed. "
    "Variance exceeds acceptable risk. "
    "Kelly discipline enforced. "
    "Standby for next signal."
)


# ─────────────────────────────────────────────────────────
# DATABASE STATE
# ─────────────────────────────────────────────────────────
class DBState:
    def __init__(self):
        self.connected: bool = False
        self.error: str | None = None
        self.pool: asyncpg.Pool | None = None
        self.attempt_count: int = 0
        self.consecutive_failures: int = 0
        self.last_check: float | None = None
        self.last_success: float | None = None

    async def mark_ok(self, pool: asyncpg.Pool):
        self.connected = True
        self.error = None
        self.pool = pool
        self.consecutive_failures = 0
        self.last_success = time.time()
        self.last_check = time.time()

    async def mark_fail(self, error: str):
        self.connected = False
        self.error = error
        self.pool = None
        self.attempt_count += 1
        self.consecutive_failures += 1
        self.last_check = time.time()

    def to_dict(self):
        now = time.time()
        return {
            "connected": self.connected,
            "error": self.error,
            "attempt_count": self.attempt_count,
            "consecutive_failures": self.consecutive_failures,
            "last_check_ago_sec": round(now - self.last_check, 1) if self.last_check else None,
            "last_success_ago_sec": round(now - self.last_success, 1) if self.last_success else None,
        }

db_state = DBState()


# ─────────────────────────────────────────────────────────
# DATABASE HELPERS
# ─────────────────────────────────────────────────────────
async def connect_db() -> bool:
    try:
        if not cfg.DATABASE_URL:
            await db_state.mark_fail("DATABASE_URL ikke satt!")
            return False

        logger.info("[DB] Kobler til Railway PostgreSQL...")
        pool = await asyncpg.create_pool(
            cfg.DATABASE_URL,
            min_size=1,
            max_size=5,
            command_timeout=30,
            ssl="require"
        )

        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")

        await db_state.mark_ok(pool)
        logger.info("[DB] Tilkoblet Railway PostgreSQL!")

        await ensure_tables(pool)
        return True

    except Exception as e:
        err = str(e)[:200]
        await db_state.mark_fail(err)
        logger.warning(f"[DB] Offline — {err}")
        return False


async def ensure_tables(pool: asyncpg.Pool):
    try:
        async with pool.acquire() as conn:
            # Basistabeller
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS bankroll (
                    id SERIAL PRIMARY KEY,
                    amount NUMERIC(10,2) NOT NULL DEFAULT 0,
                    timestamp TIMESTAMPTZ DEFAULT NOW(),
                    note TEXT
                );

                CREATE TABLE IF NOT EXISTS picks (
                    id SERIAL PRIMARY KEY,
                    match TEXT,
                    pick TEXT,
                    odds NUMERIC(5,2),
                    stake NUMERIC(10,2),
                    result TEXT,
                    profit NUMERIC(10,2),
                    telegram_posted BOOLEAN DEFAULT FALSE,
                    timestamp TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS dagens_kamp (
                    id SERIAL PRIMARY KEY,
                    match TEXT,
                    league TEXT,
                    home_team TEXT,
                    away_team TEXT,
                    pick TEXT,
                    odds NUMERIC(5,2),
                    stake NUMERIC(10,2),
                    edge NUMERIC(6,2),
                    ev NUMERIC(6,2),
                    confidence INTEGER,
                    kickoff TIMESTAMPTZ,
                    telegram_posted BOOLEAN DEFAULT FALSE,
                    timestamp TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS daily_summaries (
                    id SERIAL PRIMARY KEY,
                    date DATE,
                    profit NUMERIC(10,2),
                    num_picks INTEGER,
                    trigger_type VARCHAR(50) DEFAULT 'scheduled',
                    match_id TEXT,
                    result TEXT,
                    reason TEXT,
                    timestamp TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS settings (
                    id SERIAL PRIMARY KEY,
                    key TEXT UNIQUE,
                    value TEXT,
                    timestamp TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS odds_snapshots (
                    id SERIAL PRIMARY KEY,
                    league_key TEXT NOT NULL,
                    snapshot_time TIMESTAMPTZ DEFAULT NOW(),
                    data JSONB NOT NULL
                );

                CREATE TABLE IF NOT EXISTS api_calls (
                    id SERIAL PRIMARY KEY,
                    call_date DATE NOT NULL DEFAULT CURRENT_DATE,
                    window_name TEXT NOT NULL,
                    league_key TEXT NOT NULL,
                    status_code INTEGER,
                    called_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS match_odds_history (
                    id SERIAL PRIMARY KEY,
                    match_id TEXT NOT NULL,
                    league_key TEXT NOT NULL,
                    home_team TEXT,
                    away_team TEXT,
                    market_type VARCHAR(30),
                    bookmaker VARCHAR(50),
                    odds FLOAT NOT NULL,
                    snapshot_time TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_match_odds_hist
                    ON match_odds_history(match_id, market_type, bookmaker, snapshot_time DESC);

                CREATE TABLE IF NOT EXISTS xg_cache (
                    id SERIAL PRIMARY KEY,
                    match_id TEXT NOT NULL UNIQUE,
                    home_team TEXT,
                    away_team TEXT,
                    league_name TEXT,
                    xg_data JSONB,
                    cached_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_xg_cache_match ON xg_cache(match_id, cached_at DESC);

                CREATE TABLE IF NOT EXISTS clv_records (
                    id SERIAL PRIMARY KEY,
                    pick_id INTEGER REFERENCES dagens_kamp(id),
                    match TEXT,
                    pick TEXT,
                    odds_taken NUMERIC(5,2),
                    pinnacle_opening NUMERIC(5,2),
                    pinnacle_closing NUMERIC(5,2),
                    clv_pct NUMERIC(6,2),
                    kickoff TIMESTAMPTZ,
                    tracked_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)

            # Migrer dagens_kamp med nye kolonner
            await conn.execute("""
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS league TEXT;
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS home_team TEXT;
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS away_team TEXT;
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS edge NUMERIC(6,2);
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS ev NUMERIC(6,2);
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS confidence INTEGER;
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS kickoff TIMESTAMPTZ;
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS telegram_posted BOOLEAN DEFAULT FALSE;
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS market_type TEXT;
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS score NUMERIC(8,4);
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS bookmaker_count INTEGER;
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS pinnacle_opening NUMERIC(5,2);
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS pinnacle_closing NUMERIC(5,2);
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS clv_pct NUMERIC(6,2);
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS total_scanned INTEGER;
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS atomic_score INTEGER DEFAULT 0;
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS tier VARCHAR(20) DEFAULT 'MONITORED';
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS tier_label VARCHAR(50) DEFAULT '📊 MONITORED';
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS kelly_stake NUMERIC(6,2) DEFAULT 0.00;
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS signals_triggered JSONB DEFAULT '[]';
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS signal_streak_home VARCHAR(30) DEFAULT 'NEUTRAL';
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS signal_streak_away VARCHAR(30) DEFAULT 'NEUTRAL';
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS streak_home_count INTEGER DEFAULT 0;
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS streak_away_count INTEGER DEFAULT 0;
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS market_hint VARCHAR(30);
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS signal_velocity VARCHAR(30);
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS signal_xg VARCHAR(30);
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS signal_weather VARCHAR(30);
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS xg_divergence_home FLOAT;
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS xg_divergence_away FLOAT;

                ALTER TABLE daily_summaries ADD COLUMN IF NOT EXISTS trigger_type VARCHAR(50) DEFAULT 'scheduled';
                ALTER TABLE daily_summaries ADD COLUMN IF NOT EXISTS match_id TEXT;
                ALTER TABLE daily_summaries ADD COLUMN IF NOT EXISTS result TEXT;
                ALTER TABLE daily_summaries ADD COLUMN IF NOT EXISTS reason TEXT;
            """)

            # Migrer picks-tabell — rename bet365_* → soft_*, legg til soft_book
            await conn.execute("""
                DO $$ BEGIN
                    ALTER TABLE picks RENAME COLUMN bet365_edge TO soft_edge;
                EXCEPTION WHEN undefined_column THEN NULL; END $$;
                DO $$ BEGIN
                    ALTER TABLE picks RENAME COLUMN bet365_ev TO soft_ev;
                EXCEPTION WHEN undefined_column THEN NULL; END $$;
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS soft_edge FLOAT;
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS soft_ev FLOAT;
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS soft_book VARCHAR(50) DEFAULT 'unibet';
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS pinnacle_clv FLOAT;
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS benchmark_book VARCHAR(50) DEFAULT 'unibet';
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS clv_reference_book VARCHAR(50) DEFAULT 'pinnacle';
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS clv_missing BOOLEAN DEFAULT false;
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS telegram_posted BOOLEAN DEFAULT FALSE;
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS posted_at TIMESTAMP;
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS scan_session VARCHAR(20);
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS atomic_score INTEGER DEFAULT 0;
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS signal_velocity VARCHAR(20);
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS signal_xg_home FLOAT;
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS signal_xg_away FLOAT;
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS xg_divergence_home FLOAT;
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS xg_divergence_away FLOAT;
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS signals_triggered JSONB DEFAULT '[]';
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS signal_weather VARCHAR(30);
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS signal_referee VARCHAR(30);
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS referee_name VARCHAR(100);
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS wind_speed FLOAT;
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS temperature FLOAT;
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS tier VARCHAR(20) DEFAULT 'MONITORED';
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS tier_label VARCHAR(50) DEFAULT '📊 MONITORED';
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS kelly_multiplier FLOAT DEFAULT 0.0;
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS kelly_stake FLOAT DEFAULT 0.0;
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS referee_matches_count INTEGER DEFAULT 0;
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS signal_streak_home VARCHAR(30) DEFAULT 'NEUTRAL';
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS signal_streak_away VARCHAR(30) DEFAULT 'NEUTRAL';
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS streak_home_count INTEGER DEFAULT 0;
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS streak_away_count INTEGER DEFAULT 0;
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS market_hint VARCHAR(30);
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS market_type_detail VARCHAR(20);
                CREATE INDEX IF NOT EXISTS idx_picks_atomic ON picks(atomic_score DESC);
                CREATE INDEX IF NOT EXISTS idx_picks_weather ON picks(signal_weather);
                CREATE INDEX IF NOT EXISTS idx_picks_created ON picks(created_at DESC);

                ALTER TABLE odds_snapshots ADD COLUMN IF NOT EXISTS prev_odds FLOAT;
                ALTER TABLE odds_snapshots ADD COLUMN IF NOT EXISTS odds_delta FLOAT;
                ALTER TABLE odds_snapshots ADD COLUMN IF NOT EXISTS delta_minutes INTEGER;
                ALTER TABLE odds_snapshots ADD COLUMN IF NOT EXISTS velocity_type VARCHAR(20) DEFAULT 'UNKNOWN';
                ALTER TABLE odds_snapshots ADD COLUMN IF NOT EXISTS xg_data JSONB;
                ALTER TABLE odds_snapshots ADD COLUMN IF NOT EXISTS xg_cached_at TIMESTAMPTZ;
            """)

            # Indeks for snapshot-oppslag
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_odds_snapshots_league_time
                    ON odds_snapshots(league_key, snapshot_time DESC);
                CREATE INDEX IF NOT EXISTS idx_clv_pick_id ON clv_records(pick_id);
                CREATE INDEX IF NOT EXISTS idx_dagens_kamp_kickoff ON dagens_kamp(kickoff);
            """)

            # ── FASE A: Zero-downtime picks_v2 shadow table ───────────────
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS picks_v2 (
                    id BIGSERIAL PRIMARY KEY,
                    match_name VARCHAR(255),
                    home_team VARCHAR(100),
                    away_team VARCHAR(100),
                    league VARCHAR(50),
                    kickoff_time TIMESTAMPTZ,
                    odds DECIMAL(5,2),
                    soft_edge DECIMAL(5,2),
                    soft_ev DECIMAL(5,2),
                    soft_book VARCHAR(50),
                    pinnacle_clv DECIMAL(5,2),
                    atomic_score INTEGER DEFAULT 0,
                    signals_triggered JSONB DEFAULT '[]',
                    signal_velocity VARCHAR(20),
                    signal_xg_home FLOAT,
                    signal_xg_away FLOAT,
                    signal_weather VARCHAR(20),
                    weather_market_impact VARCHAR(30),
                    wind_speed FLOAT,
                    temperature FLOAT,
                    signal_referee VARCHAR(30),
                    referee_name VARCHAR(100),
                    referee_cards_avg FLOAT,
                    referee_home_bias FLOAT,
                    referee_matches_count INTEGER DEFAULT 0,
                    result VARCHAR(10),
                    telegram_posted BOOLEAN DEFAULT FALSE,
                    posted_at TIMESTAMPTZ,
                    scan_session VARCHAR(20),
                    benchmark_book VARCHAR(50),
                    clv_reference_book VARCHAR(50),
                    clv_missing BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    timestamp TIMESTAMPTZ DEFAULT NOW(),
                    tier VARCHAR(20) NOT NULL DEFAULT 'MONITORED',
                    tier_label VARCHAR(50) NOT NULL DEFAULT '📊 MONITORED',
                    kelly_multiplier DECIMAL(3,2) NOT NULL DEFAULT 0.00,
                    kelly_stake DECIMAL(6,2) NOT NULL DEFAULT 0.00,
                    CONSTRAINT chk_tier_values
                        CHECK (tier IN ('ATOMIC', 'EDGE', 'MONITORED')),
                    CONSTRAINT chk_kelly_positive
                        CHECK (kelly_stake >= 0),
                    CONSTRAINT chk_kelly_max
                        CHECK (kelly_stake <= 5.00),
                    CONSTRAINT chk_atomic_score
                        CHECK (atomic_score BETWEEN 0 AND 9)
                );
            """)

            # FASE A-2: Backfill historiske data (idempotent via ON CONFLICT)
            # Bruker kun kolonner som faktisk finnes i picks-tabellen
            # (home_team/away_team/league er ikke i picks, kun i dagens_kamp)
            await conn.execute("""
                INSERT INTO picks_v2 (
                    id, match_name,
                    odds, soft_edge, soft_ev, soft_book,
                    pinnacle_clv, atomic_score, signals_triggered,
                    result, telegram_posted, posted_at, scan_session,
                    benchmark_book, clv_reference_book, clv_missing,
                    created_at, timestamp,
                    tier, tier_label, kelly_multiplier, kelly_stake
                )
                SELECT
                    id,
                    COALESCE(match, ''),
                    odds,
                    soft_edge,
                    soft_ev,
                    soft_book,
                    pinnacle_clv,
                    COALESCE(atomic_score, 0),
                    COALESCE(signals_triggered, '[]'::jsonb),
                    result,
                    COALESCE(telegram_posted, FALSE),
                    posted_at,
                    scan_session,
                    benchmark_book,
                    clv_reference_book,
                    COALESCE(clv_missing, FALSE),
                    COALESCE(timestamp, NOW()),
                    COALESCE(timestamp, NOW()),
                    COALESCE(tier, 'MONITORED'),
                    COALESCE(tier_label, '📊 MONITORED'),
                    COALESCE(kelly_multiplier, 0.00),
                    COALESCE(kelly_stake, 0.00)
                FROM picks
                ORDER BY id
                ON CONFLICT (id) DO NOTHING;
            """)

            # FASE A-3: Sync trigger (ny picks → picks_v2 automatisk)
            # Bruker kun kolonner som eksisterer i picks
            await conn.execute("""
                CREATE OR REPLACE FUNCTION sync_picks_to_v2()
                RETURNS TRIGGER AS $$
                BEGIN
                    INSERT INTO picks_v2 (
                        id, match_name,
                        odds, soft_edge, soft_ev, soft_book, pinnacle_clv,
                        atomic_score, signals_triggered,
                        result, telegram_posted, posted_at, created_at, timestamp,
                        tier, tier_label, kelly_multiplier, kelly_stake
                    ) VALUES (
                        NEW.id,
                        COALESCE(NEW.match, ''),
                        NEW.odds,
                        NEW.soft_edge,
                        NEW.soft_ev,
                        NEW.soft_book,
                        NEW.pinnacle_clv,
                        COALESCE(NEW.atomic_score, 0),
                        COALESCE(NEW.signals_triggered, '[]'::jsonb),
                        NEW.result,
                        COALESCE(NEW.telegram_posted, FALSE),
                        NEW.posted_at,
                        COALESCE(NEW.timestamp, NOW()),
                        COALESCE(NEW.timestamp, NOW()),
                        COALESCE(NEW.tier, 'MONITORED'),
                        COALESCE(NEW.tier_label, '📊 MONITORED'),
                        COALESCE(NEW.kelly_multiplier, 0.00),
                        COALESCE(NEW.kelly_stake, 0.00)
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        result = EXCLUDED.result,
                        telegram_posted = EXCLUDED.telegram_posted,
                        updated_at = NOW();
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;

                DROP TRIGGER IF EXISTS picks_sync_trigger ON picks;
                CREATE TRIGGER picks_sync_trigger
                AFTER INSERT OR UPDATE ON picks
                FOR EACH ROW
                EXECUTE FUNCTION sync_picks_to_v2();
            """)

            # FASE A-2b: Legg til timestamp hvis mangler (idempotent)
            await conn.execute("""
                ALTER TABLE picks_v2
                ADD COLUMN IF NOT EXISTS timestamp TIMESTAMPTZ DEFAULT NOW();
            """)

        # FASE A-4: Indexes CONCURRENTLY (utenfor transaksjon)
        async with pool.acquire() as conn:
            await conn.execute(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_picks_v2_tier "
                "ON picks_v2(tier)"
            )
            await conn.execute(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_picks_v2_created "
                "ON picks_v2(created_at DESC)"
            )
            await conn.execute(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_picks_v2_telegram "
                "ON picks_v2(telegram_posted) WHERE telegram_posted = TRUE"
            )
            await conn.execute(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_picks_v2_atomic "
                "ON picks_v2(atomic_score) WHERE atomic_score >= 1"
            )

        logger.info("[DB] Tabeller OK — picks_v2 shadow table aktiv")
    except Exception as e:
        logger.warning(f"[DB] Tabell-feil: {e}")


async def reconnect_loop():
    delays = [5, 10, 20, 40, 60]
    attempt = 0
    while True:
        try:
            if not db_state.connected:
                delay = delays[min(attempt, len(delays) - 1)]
                await asyncio.sleep(delay)
                logger.info(f"[DB] Reconnect forsøk #{attempt + 1}...")
                success = await connect_db()
                if success:
                    attempt = 0
                else:
                    attempt += 1
            else:
                await asyncio.sleep(30)
                try:
                    async with db_state.pool.acquire() as conn:
                        await conn.fetchval("SELECT 1")
                except Exception as e:
                    logger.warning(f"[DB] Ping feilet: {e}")
                    await db_state.mark_fail(str(e))
                    attempt = 0
        except asyncio.CancelledError:
            logger.info("[DB] Reconnect-loop avsluttet.")
            break
        except Exception as e:
            logger.error(f"[DB] Uventet feil i reconnect-loop: {e}")
            await asyncio.sleep(10)


# ─────────────────────────────────────────────────────────
# API BUDSJETT-GUARD
# ─────────────────────────────────────────────────────────
async def _check_api_budget(conn, window_name: str, leagues: list) -> bool:
    """
    Sjekker om vi har nok API-credits igjen denne måneden.
    Returnerer True hvis vi kan fortsette, False hvis over budsjett.
    """
    try:
        row = await conn.fetchrow("""
            SELECT COUNT(*) as calls_this_month
            FROM api_calls
            WHERE call_date >= DATE_TRUNC('month', CURRENT_DATE)
              AND status_code = 200
        """)
        calls_this_month = row["calls_this_month"] if row else 0
        calls_needed = len(leagues)
        if calls_this_month + calls_needed > API_MONTHLY_BUDGET:
            logger.warning(
                f"[BudsjettGuard] {window_name}: {calls_this_month} brukt + "
                f"{calls_needed} nødvendig > {API_MONTHLY_BUDGET} budsjett — hopper over"
            )
            return False
        logger.info(
            f"[BudsjettGuard] {window_name}: {calls_this_month}/{API_MONTHLY_BUDGET} "
            f"brukt — {calls_needed} calls planlagt"
        )
        return True
    except Exception as e:
        logger.warning(f"[BudsjettGuard] Feil ved budsjettsjekk: {e} — fortsetter")
        return True


async def _log_api_call(conn, window_name: str, league_key: str, status_code: int):
    """Logger et API-kall til api_calls-tabellen."""
    try:
        await conn.execute("""
            INSERT INTO api_calls (window_name, league_key, status_code)
            VALUES ($1, $2, $3)
        """, window_name, league_key, status_code)
    except Exception as e:
        logger.warning(f"[BudsjettGuard] Klarte ikke logge API-kall: {e}")


# ─────────────────────────────────────────────────────────
# ATOMIC SIGNAL ARCHITECTURE — konstanter
# ─────────────────────────────────────────────────────────
ATOMIC_SCORE_MIN         = int(os.getenv("ATOMIC_SCORE_MIN", "1"))
XG_DIVERGENCE_THRESHOLD  = float(os.getenv("XG_DIVERGENCE_THRESHOLD", "0.6"))
VELOCITY_SHARP_DELTA     = float(os.getenv("VELOCITY_SHARP_DELTA", "0.10"))
VELOCITY_SHARP_MINUTES   = int(os.getenv("VELOCITY_SHARP_MINUTES", "60"))
ATOMIC_MODE              = os.getenv("ATOMIC_MODE", "enabled")
LEAGUE_AVERAGE_GOALS     = 1.35


# ─────────────────────────────────────────────────────────
# SIGNAL 1: ODDS VELOCITY
# ─────────────────────────────────────────────────────────
async def calculate_odds_velocity(
    conn,
    match_id: str,
    current_odds: float,
    market_type: str,
    bookmaker: str
) -> dict:
    """
    Beregner odds-bevegelseshastighet fra match_odds_history.
    Returnerer alltid et resultat — feiler aldri stille.
    """
    try:
        if not conn or not match_id:
            return {
                "velocity_type": "NO_HISTORY",
                "odds_delta": 0.0,
                "delta_minutes": 0,
                "atomic_points": 0,
                "reason": "Ingen conn eller match_id"
            }

        row = await conn.fetchrow("""
            SELECT odds, snapshot_time
            FROM match_odds_history
            WHERE match_id = $1
              AND market_type = $2
              AND bookmaker = $3
            ORDER BY snapshot_time DESC
            LIMIT 1
        """, match_id, market_type, bookmaker)

        if row is None:
            return {
                "velocity_type": "NO_HISTORY",
                "odds_delta": 0.0,
                "delta_minutes": 0,
                "atomic_points": 0,
                "reason": "Første snapshot for denne kampen"
            }

        prev_odds = float(row["odds"])
        prev_time = row["snapshot_time"]
        now_utc = datetime.now(timezone.utc)

        odds_delta = abs(current_odds - prev_odds)
        delta_seconds = (now_utc - prev_time).total_seconds()
        delta_minutes = max(1, int(delta_seconds / 60))

        if odds_delta >= VELOCITY_SHARP_DELTA and delta_minutes <= VELOCITY_SHARP_MINUTES:
            velocity_type = "SHARP_MONEY"
            atomic_points = 2
        elif odds_delta >= 0.15 and delta_minutes <= 180:
            velocity_type = "SHARP_MONEY"
            atomic_points = 2
        elif odds_delta >= 0.05 and delta_minutes > 180:
            velocity_type = "PUBLIC_MONEY"
            atomic_points = 0
        elif odds_delta < 0.05:
            velocity_type = "STABLE"
            atomic_points = 0
        else:
            velocity_type = "NEUTRAL"
            atomic_points = 0

        return {
            "velocity_type": velocity_type,
            "odds_delta": round(odds_delta, 4),
            "delta_minutes": delta_minutes,
            "atomic_points": atomic_points,
            "prev_odds": prev_odds,
            "current_odds": current_odds
        }

    except Exception as e:
        return {
            "velocity_type": "ERROR",
            "odds_delta": 0.0,
            "delta_minutes": 0,
            "atomic_points": 0,
            "error": str(e)[:100]
        }


# ─────────────────────────────────────────────────────────
# SIGNAL 2: xG DIVERGENS — med fuzzy matching, cache, rate limit
# ─────────────────────────────────────────────────────────

# Rate limiter: maks 10 kall/minutt = 1 per 6s
_xg_rate_lock = asyncio.Lock()
_xg_last_call_time: float = 0.0

# Ligaer tilgjengelig på gratis plan
_XG_LEAGUE_MAP = {
    "Premier League": "PL",
    "La Liga": "PD",
    "Bundesliga": "BL1",
    "Serie A": "SA",
    "Ligue 1": "FL1",
    "Eredivisie": "DED",
    "Champions League": "CL",
}
# Ligaer IKKE tilgjengelig på gratis plan → legacy gate
_XG_LEAGUE_UNAVAILABLE = {"Europa League"}


_TEAM_ALIASES: dict = {
    # Bundesliga — engelsk → tysk
    "bayern munich": "bayern münchen",
    "cologne": "köln",
    "borussia monchengladbach": "borussia mönchengladbach",
    "monchengladbach": "mönchengladbach",
    "mainz": "mainz",
    "leverkusen": "leverkusen",
    # Serie A
    "ac milan": "milan",
    "inter milan": "inter",
    # La Liga
    "athletic bilbao": "athletic club",
    "atletico madrid": "atlético de madrid",
    "betis": "real betis",
    # EPL
    "spurs": "tottenham",
    "man city": "manchester city",
    "man united": "manchester united",
}


def _normalize_name(name: str) -> str:
    """Normaliserer teamnavn: lowercase, fjerner suffikser, normaliserer umlauts."""
    name = name.lower().strip()
    # Umlaut-normalisering (tysk/norsk)
    name = name.replace("ü", "u").replace("ö", "o").replace("ä", "a").replace("é", "e").replace("ó", "o")
    name = name.replace("münchen", "munich").replace("köln", "cologne")
    # Fjern vanlige suffikser/prefikser
    for token in [" fc", " cf", " afc", " sc", " ac", " bc", " fk", " sk",
                  "fc ", "afc ", "as ", "fk ", "1. ", "sv ", "bv ", "vfl ", "vfb ",
                  " 1910", " 04", " 05", " 1846", " 1899", " 1846"]:
        name = name.replace(token, " ")
    return " ".join(name.split())  # Fjern doble spaces


def _fuzzy_team_match(search: str, api_name: str) -> bool:
    """Fuzzy team name matching — normaliserer navn, sjekker aliases, håndterer umlauts."""
    s = _normalize_name(_TEAM_ALIASES.get(search.lower().strip(), search))
    a = _normalize_name(api_name)
    return s in a or a in s or s[:6] == a[:6]


async def get_xg_divergence(
    home_team: str,
    away_team: str,
    league_name: str,
    api_key: str = None,
    conn=None,
    match_id: str = None,
) -> dict:
    """
    Henter xG-proxy data fra football-data.org.
    Returnerer alltid — feiler aldri stille.
    Sjekker xg_cache FØR API-kall (6 timers TTL).
    Rate limit: 10 kall/minutt = sleep(6) mellom kall.
    Bruker httpx + fuzzy team matching.
    """
    global _xg_last_call_time

    if not api_key:
        return {
            "atomic_points": 0,
            "signal": "XG_UNAVAILABLE",
            "reason": "Mangler FOOTBALL_DATA_API_KEY",
        }

    if league_name in _XG_LEAGUE_UNAVAILABLE:
        return {"atomic_points": 0, "signal": "XG_LEAGUE_UNAVAILABLE"}

    fd_league = _XG_LEAGUE_MAP.get(league_name)
    if not fd_league:
        return {"atomic_points": 0, "signal": "XG_LEAGUE_UNKNOWN"}

    # ── Cache-sjekk ──────────────────────────────────────────
    if conn and match_id:
        try:
            cached = await conn.fetchrow("""
                SELECT xg_data FROM xg_cache
                WHERE match_id = $1
                  AND cached_at > NOW() - INTERVAL '6 hours'
            """, match_id)
            if cached and cached["xg_data"]:
                result = dict(json.loads(cached["xg_data"]))
                result["from_cache"] = True
                return result
        except Exception:
            pass  # Cache miss — fortsett til API

    # ── Rate limit guard ─────────────────────────────────────
    async with _xg_rate_lock:
        now_ts = time.time()
        elapsed = now_ts - _xg_last_call_time
        if elapsed < 6.0:
            await asyncio.sleep(6.0 - elapsed)
        _xg_last_call_time = time.time()

    try:
        async with httpx.AsyncClient(timeout=8) as client:
            resp = await client.get(
                f"https://api.football-data.org/v4/competitions/{fd_league}/matches",
                params={"status": "FINISHED", "limit": 15},
                headers={"X-Auth-Token": api_key},
            )
            if resp.status_code == 429:
                return {"atomic_points": 0, "signal": "XG_RATE_LIMITED"}
            if resp.status_code != 200:
                return {"atomic_points": 0, "signal": "XG_API_ERROR", "http_status": resp.status_code}

            matches = resp.json().get("matches", [])

            home_matches = [
                m for m in matches
                if _fuzzy_team_match(home_team, m.get("homeTeam", {}).get("name", ""))
            ]
            away_matches = [
                m for m in matches
                if _fuzzy_team_match(away_team, m.get("awayTeam", {}).get("name", ""))
            ]

            if len(home_matches) < 3 or len(away_matches) < 3:
                return {
                    "atomic_points": 0,
                    "signal": "XG_INSUFFICIENT_DATA",
                    "home_matches_found": len(home_matches),
                    "away_matches_found": len(away_matches),
                }

            home_goals = [
                m["score"]["fullTime"]["home"]
                for m in home_matches[:6]
                if m.get("score", {}).get("fullTime", {}).get("home") is not None
            ]
            away_goals = [
                m["score"]["fullTime"]["away"]
                for m in away_matches[:6]
                if m.get("score", {}).get("fullTime", {}).get("away") is not None
            ]

            if not home_goals or not away_goals:
                return {"atomic_points": 0, "signal": "XG_NO_SCORES"}

            home_avg = sum(home_goals) / len(home_goals)
            away_avg = sum(away_goals) / len(away_goals)
            home_div = home_avg - LEAGUE_AVERAGE_GOALS
            away_div = away_avg - LEAGUE_AVERAGE_GOALS

            if abs(home_div) > XG_DIVERGENCE_THRESHOLD or abs(away_div) > XG_DIVERGENCE_THRESHOLD:
                atomic_points = 2
                signal = "XG_DIVERGENCE_FOUND"
            else:
                atomic_points = 0
                signal = "XG_NEUTRAL"

            result = {
                "home_goals_avg": round(home_avg, 2),
                "away_goals_avg": round(away_avg, 2),
                "home_divergence": round(home_div, 2),
                "away_divergence": round(away_div, 2),
                "atomic_points": atomic_points,
                "signal": signal,
                "matches_used": min(len(home_goals), len(away_goals)),
                "from_cache": False,
            }

            # ── Lagre i cache ─────────────────────────────────
            if conn and match_id:
                try:
                    await conn.execute("""
                        INSERT INTO xg_cache (match_id, home_team, away_team, league_name, xg_data)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (match_id) DO UPDATE
                          SET xg_data = EXCLUDED.xg_data,
                              cached_at = NOW()
                    """, match_id, home_team, away_team, league_name, json.dumps(result))
                except Exception:
                    pass

            return result

    except httpx.TimeoutException:
        return {"atomic_points": 0, "signal": "XG_TIMEOUT", "reason": "football-data.org timeout"}
    except Exception as e:
        return {"atomic_points": 0, "signal": "XG_ERROR", "error": str(e)[:100]}


# ─────────────────────────────────────────────────────────
# SIGNAL 5: SCORING STREAK
# ─────────────────────────────────────────────────────────
_STREAK_LEAGUE_MAP: dict = {
    "Premier League": "PL",
    "La Liga": "PD",
    "Bundesliga": "BL1",
    "Serie A": "SA",
    "Ligue 1": "FL1",
    "Eredivisie": "DED",
    "Champions League": "CL",
}


async def get_scoring_streak(
    team_name: str,
    league_name: str,
    api_key: str,
) -> dict:
    """
    Signal 5 — Scoring streak.
    Lag som scorer i 5-7 av siste 7 er underestimert i BTTS/Over.
    Returnerer alltid. Feiler aldri stille.
    """
    if not api_key:
        return {"streak_signal": "UNAVAILABLE", "atomic_points": 0, "reason": "No API key"}

    fd_league = _STREAK_LEAGUE_MAP.get(league_name)
    if not fd_league:
        return {"streak_signal": "LEAGUE_UNKNOWN", "atomic_points": 0}

    try:
        if football_limiter:
            await football_limiter.acquire()

        async with httpx.AsyncClient(timeout=8) as client:
            resp = await client.get(
                f"https://api.football-data.org/v4/competitions/{fd_league}/matches",
                params={"status": "FINISHED"},
                headers={"X-Auth-Token": api_key},
            )

        if resp.status_code == 429:
            return {"streak_signal": "RATE_LIMITED", "atomic_points": 0}
        if resp.status_code != 200:
            return {"streak_signal": "API_ERROR", "atomic_points": 0, "http_status": resp.status_code}

        matches = resp.json().get("matches", [])

        # Filtrer kamper for laget
        team_matches = [
            m for m in matches
            if _fuzzy_team_match(team_name, m.get("homeTeam", {}).get("name", ""))
            or _fuzzy_team_match(team_name, m.get("awayTeam", {}).get("name", ""))
        ]

        if len(team_matches) < 5:
            return {
                "streak_signal": "INSUFFICIENT_DATA",
                "atomic_points": 0,
                "matches_found": len(team_matches),
            }

        # Scorer laget i de siste 7 kampene?
        scored = []
        for m in team_matches[:7]:
            score = m.get("score", {}).get("fullTime", {})
            home_goals = score.get("home")
            away_goals = score.get("away")
            if home_goals is None or away_goals is None:
                continue
            is_home = _fuzzy_team_match(team_name, m.get("homeTeam", {}).get("name", ""))
            goals = home_goals if is_home else away_goals
            scored.append(1 if goals > 0 else 0)

        if len(scored) < 5:
            return {"streak_signal": "INSUFFICIENT_DATA", "atomic_points": 0}

        streak = sum(scored)

        if streak >= 7:
            return {
                "streak_signal": "SCORING_7_OF_7",
                "atomic_points": 2,
                "market_hint": "BTTS_YES/OVER_2.5",
                "streak_count": streak,
            }
        elif streak >= 5:
            return {
                "streak_signal": "SCORING_5_OF_7",
                "atomic_points": 1,
                "market_hint": "BTTS_YES",
                "streak_count": streak,
            }
        else:
            return {"streak_signal": "NEUTRAL", "atomic_points": 0, "streak_count": streak}

    except httpx.TimeoutException:
        return {"streak_signal": "TIMEOUT", "atomic_points": 0}
    except Exception as e:
        logger.warning(f"[StreakSignal] Feil: {e}")
        return {"streak_signal": "ERROR", "atomic_points": 0, "error": str(e)[:100]}


# ─────────────────────────────────────────────────────────
# UCL FIXTURE FALLBACK (football-data.org)
# ─────────────────────────────────────────────────────────
async def fetch_ucl_fixtures_football_data() -> list:
    """
    Henter UCL-kamper fra football-data.org (fixture-data, ingen odds).
    Brukes som supplement til Odds API for metadata og xG-enrichment.
    Returnerer liste kompatibel med _analyse_snapshot-format.
    """
    from datetime import timedelta
    fd_key = cfg.FOOTBALL_DATA_API_KEY if cfg else os.getenv("FOOTBALL_DATA_API_KEY", "")
    if not fd_key:
        logger.warning("[UCL-Fixture] FOOTBALL_DATA_API_KEY mangler")
        return []
    today = datetime.now(timezone.utc).date()
    date_from = today.strftime("%Y-%m-%d")
    date_to = (today + timedelta(days=4)).strftime("%Y-%m-%d")
    try:
        if football_limiter:
            await football_limiter.acquire()
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                "https://api.football-data.org/v4/competitions/CL/matches",
                params={"dateFrom": date_from, "dateTo": date_to, "status": "SCHEDULED"},
                headers={"X-Auth-Token": fd_key},
            )
        if resp.status_code != 200:
            logger.warning(f"[UCL-Fixture] football-data.org feilet: {resp.status_code}")
            return []
        matches = resp.json().get("matches", [])
        logger.info(f"[UCL-Fixture] {len(matches)} UCL-kamper fra football-data.org")
        result = []
        for m in matches:
            home = m.get("homeTeam", {}).get("name")
            away = m.get("awayTeam", {}).get("name")
            if not home or not away:
                continue
            result.append({
                "id": f"fd_{m.get('id')}",
                "home_team": home,
                "away_team": away,
                "commence_time": m.get("utcDate", ""),
                "bookmakers": [],  # Ingen odds fra fd.org
                "fd_match_id": m.get("id"),
                "source": "football-data.org",
            })
        return result
    except Exception as e:
        logger.error(f"[UCL-Fixture] Exception: {e}")
        return []


# ─────────────────────────────────────────────────────────
# KELLY CRITERION
# ─────────────────────────────────────────────────────────
def calculate_kelly_stake(
    edge_pct: float,
    odds: float,
    bankroll: float = 1000.0,
    tier: str = "EDGE",
) -> float:
    """
    Kelly Criterion med tier-multiplier.
    Fractional Kelly (25%) for sikkerhet.
    """
    if odds <= 1.0 or edge_pct <= 0:
        return 0.0

    # Full Kelly
    b = odds - 1  # Net odds
    p = (edge_pct / 100) + (1 / odds)
    q = 1 - p
    kelly = (b * p - q) / b

    # Fractional Kelly (25% for safety)
    fractional_kelly = kelly * 0.25

    # Tier multiplier
    if tier == "ATOMIC":
        multiplier = 1.0  # Full fractional
    elif tier == "EDGE":
        multiplier = 0.5  # Half fractional
    else:
        multiplier = 0.0  # No bet

    stake_fraction = fractional_kelly * multiplier
    stake_units = round(stake_fraction * 100, 1)

    # Cap på 5 units for sikkerhet
    return min(stake_units, 5.0)


# ─────────────────────────────────────────────────────────
# SIGNAL 3: ATOMIC SCORE GATE
# ─────────────────────────────────────────────────────────
def calculate_atomic_score(
    velocity_result: dict,
    xg_result: dict,
    soft_edge: float,
    soft_ev: float,
    weather_result: dict = None,
    referee_result: dict = None,
    streak_home_result: dict = None,
    streak_away_result: dict = None,
) -> dict:
    """
    Kombinerer alle 5 atomic signals (velocity, xG, weather, referee, streak).
    ALDRI blokkerende — alltid additivt.
    Gammel gate (SOFT_EDGE_MIN) er alltid siste fallback.
    Maks score: velocity(2) + xG(2) + edge(2) + EV(1) + weather(1) + referee(0) + streak(2) = 10 → capped 9
    """
    weather_result      = weather_result      or {"signal": "WEATHER_UNAVAILABLE", "atomic_points": 0}
    referee_result      = referee_result      or {"signal": "REFEREE_UNAVAILABLE", "atomic_points": 0}
    streak_home_result  = streak_home_result  or {"streak_signal": "UNAVAILABLE", "atomic_points": 0}
    streak_away_result  = streak_away_result  or {"streak_signal": "UNAVAILABLE", "atomic_points": 0}

    atomic_score = 0
    signals_triggered = []

    # Signal 1 — Velocity
    v_points = velocity_result.get("atomic_points", 0)
    atomic_score += v_points
    if v_points > 0:
        signals_triggered.append(velocity_result.get("velocity_type", "VELOCITY"))

    # Signal 2 — xG
    xg_points = xg_result.get("atomic_points", 0)
    atomic_score += xg_points
    if xg_points > 0:
        signals_triggered.append(xg_result.get("signal", "XG"))

    # Signal 3 — Weather
    w_points = weather_result.get("atomic_points", 0)
    atomic_score += w_points
    if w_points > 0:
        signals_triggered.append(weather_result.get("signal", "WEATHER"))

    # Signal 4 — Referee (data-innsamling i v10.1.0 — 0 poeng)
    r_points = referee_result.get("atomic_points", 0)
    atomic_score += r_points
    if r_points > 0:
        signals_triggered.append(referee_result.get("signal", "REFEREE"))

    # Signal 5 — Scoring streak (best av home/away)
    streak_points = max(
        streak_home_result.get("atomic_points", 0),
        streak_away_result.get("atomic_points", 0),
    )
    atomic_score += streak_points
    if streak_points > 0:
        streak_sig = (
            streak_home_result.get("streak_signal")
            if streak_home_result.get("atomic_points", 0) >= streak_away_result.get("atomic_points", 0)
            else streak_away_result.get("streak_signal")
        )
        signals_triggered.append(streak_sig or "STREAK")

    # Cap atomic_score ved 9
    atomic_score = min(atomic_score, 9)

    # market_hint fra streak (prioriter høyeste poeng)
    if streak_home_result.get("atomic_points", 0) >= streak_away_result.get("atomic_points", 0):
        market_hint = streak_home_result.get("market_hint") or streak_away_result.get("market_hint")
    else:
        market_hint = streak_away_result.get("market_hint") or streak_home_result.get("market_hint")

    # Edge-bonus
    if soft_edge >= 0.5:
        atomic_score += 2
        signals_triggered.append("STRONG_EDGE_35PCT")
    elif soft_edge >= 0.5:
        atomic_score += 1
        signals_triggered.append("EDGE_25PCT")

    # EV-bonus
    if soft_ev >= 5.0:
        atomic_score += 1
        signals_triggered.append("STRONG_EV_5PCT")

    xg_available = xg_result.get("signal") not in [
        "XG_UNAVAILABLE", "XG_API_ERROR", "XG_TIMEOUT", "XG_ERROR", "XG_LEAGUE_UNKNOWN"
    ]
    velocity_available = velocity_result.get("velocity_type") not in ["NO_HISTORY", "ERROR"]
    weather_available  = weather_result.get("signal") not in [
        "WEATHER_UNAVAILABLE", "WEATHER_API_ERROR", "WEATHER_TIMEOUT", "WEATHER_ERROR",
        "WEATHER_AUTH_FAIL", "WEATHER_CITY_UNKNOWN",
    ]

    if not xg_available and not velocity_available:
        verdict = "LEGACY_GATE"
        gate_passed = soft_edge >= float(os.getenv("SOFT_EDGE_MIN", "2.0"))
    elif atomic_score >= 1:
        verdict = "ATOMIC_CONFIRMED"
        gate_passed = True
    elif atomic_score >= 1:
        verdict = "ATOMIC_PROBABLE"
        gate_passed = soft_edge >= 0.5
    elif atomic_score >= 1:
        verdict = "WEAK_SIGNAL"
        gate_passed = soft_edge >= 0.5
    else:
        verdict = "NO_SIGNAL"
        gate_passed = False

    # Tier-klassifisering
    if atomic_score >= 5 and soft_edge >= 7:
        tier = "ATOMIC"
        tier_label = "⚡ ATOMIC SIGNAL"
        post_telegram = True
        kelly_multiplier = 1.0
    elif atomic_score >= 2 and soft_edge >= 4:
        tier = "EDGE"
        tier_label = "🎯 EDGE SIGNAL"
        post_telegram = True
        kelly_multiplier = 0.5
    else:
        tier = "MONITORED"
        tier_label = "📊 MONITORED"
        post_telegram = False
        kelly_multiplier = 0.0

    return {
        "atomic_score": atomic_score,
        "verdict": verdict,
        "gate_passed": gate_passed,
        "signals_triggered": signals_triggered,
        "xg_available": xg_available,
        "velocity_available": velocity_available,
        "weather_available": weather_available,
        "tier": tier,
        "tier_label": tier_label,
        "post_telegram": post_telegram,
        "kelly_multiplier": kelly_multiplier,
        "market_hint": market_hint,
        "streak_home_signal": streak_home_result.get("streak_signal", "UNAVAILABLE"),
        "streak_away_signal": streak_away_result.get("streak_signal", "UNAVAILABLE"),
    }


# ─────────────────────────────────────────────────────────
# ODDS CACHING (3-vindu plan)
# ─────────────────────────────────────────────────────────
async def fetch_all_odds(leagues: list = None, window_name: str = "early"):
    """
    3-vindu scan:
      Vindu 1 (Early 07:00 UTC): alle 8 ligaer (8 calls)
      Vindu 2 (Evening 18:00 UTC): topp-4 ligaer (4 calls)
    Totalt: 12 calls/dag × 30 dager = 360 req/mnd (buffer mot 500-limit).
    """
    target_leagues = leagues if leagues is not None else SCAN_LEAGUES
    logger.info(f"[OddsCache] fetch_all_odds startet — vindu={window_name}, ligaer={len(target_leagues)}")

    if not cfg.ODDS_API_KEY:
        logger.warning("[OddsCache] ODDS_API_KEY mangler")
        return

    if not db_state.connected or not db_state.pool:
        logger.warning("[OddsCache] DB offline — kan ikke lagre snapshots")
        return

    snap_time = datetime.now(timezone.utc)
    saved = 0

    async with db_state.pool.acquire() as conn:
        ok = await _check_api_budget(conn, window_name, target_leagues)
        if not ok:
            return

    async with httpx.AsyncClient(timeout=30) as client:
        for league in target_leagues:
            try:
                resp = await client.get(
                    f"https://api.the-odds-api.com/v4/sports/{league['key']}/odds/",
                    params={
                        "apiKey": cfg.ODDS_API_KEY,
                        "regions": "eu",
                        "markets": "totals,spreads,h2h",
                        "oddsFormat": "decimal",
                        "bookmakers": "pinnacle,bet365,betway,unibet,williamhill,bwin,nordicbet,betsson,betfair_ex_eu,sport888",
                    }
                )

                async with db_state.pool.acquire() as conn:
                    await _log_api_call(conn, window_name, league["key"], resp.status_code)

                if resp.status_code != 200:
                    logger.warning(f"[OddsCache] {league['key']}: HTTP {resp.status_code}")
                    continue

                data = resp.json()
                if not isinstance(data, list):
                    continue

                # Filtrer kun kamper innen 24 timer (kun dagens kamper)
                now = datetime.now(timezone.utc)
                filtered_data = []
                for m in data:
                    try:
                        commence = datetime.fromisoformat(
                            m["commence_time"].replace("Z", "+00:00")
                        )
                        hours = (commence - now).total_seconds() / 3600
                        if 1 <= hours <= 24:  # Kun dagens kamper
                            filtered_data.append(m)
                    except:
                        continue
                
                if not filtered_data:
                    logger.info(f"[OddsCache] {league['name']}: Ingen kamper innen 24 timer")
                    continue

                async with db_state.pool.acquire() as conn:
                    await conn.execute("""
                        INSERT INTO odds_snapshots (league_key, snapshot_time, data)
                        VALUES ($1, $2, $3)
                    """, league["key"], snap_time, json.dumps(filtered_data))

                    # Populer match_odds_history for velocity-beregning
                    for m in data:
                        match_id = m.get("id", "")
                        if not match_id:
                            continue
                        for bk in m.get("bookmakers", []):
                            bk_key = bk.get("key", "")
                            for mkt in bk.get("markets", []):
                                mkt_key = mkt.get("key", "")
                                if mkt_key not in ("h2h", "totals"):
                                    continue
                                for outcome in mkt.get("outcomes", []):
                                    price = outcome.get("price")
                                    if not price:
                                        continue
                                    label = outcome.get("name", "")
                                    point = outcome.get("point")
                                    market_label = mkt_key if not point else f"{mkt_key}_{point}"
                                    await conn.execute("""
                                        INSERT INTO match_odds_history
                                            (match_id, league_key, home_team, away_team,
                                             market_type, bookmaker, odds, snapshot_time)
                                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                                    """, match_id, league["key"],
                                        m.get("home_team"), m.get("away_team"),
                                        f"{market_label}_{label}", bk_key,
                                        float(price), snap_time)

                saved += 1
                logger.info(f"[OddsCache] {league['name']}: {len(data)} kamper lagret")

            except Exception as e:
                logger.warning(f"[OddsCache] Feil for {league['key']}: {e}")
                continue

    logger.info(f"[OddsCache] Ferdig — {saved}/{len(target_leagues)} ligaer cachet kl. {snap_time.strftime('%H:%M')} UTC")


async def fetch_top4_odds():
    """Vindu 2 — 18:00 UTC: henter kun topp-4 ligaer (EPL, La Liga, Bundesliga, CL)."""
    await fetch_all_odds(leagues=TOP4_LEAGUES, window_name="evening")


# ─────────────────────────────────────────────────────────
# ANALYSE (leser fra cache, beregner EV/edge via Pinnacle)
# ─────────────────────────────────────────────────────────
def _median(lst: list) -> float:
    s = sorted(lst)
    n = len(s)
    return s[n // 2] if n % 2 == 1 else (s[n // 2 - 1] + s[n // 2]) / 2


def _pinnacle_no_vig(h_odds: float, d_odds: float, a_odds: float):
    """Fjerner vig fra Pinnacle-odds og returnerer sanne sannsynligheter."""
    raw_h = 1 / h_odds
    raw_d = 1 / d_odds
    raw_a = 1 / a_odds
    total = raw_h + raw_d + raw_a
    margin_pct = (total - 1) * 100
    p_h = raw_h / total
    p_d = raw_d / total
    p_a = raw_a / total
    return p_h, p_d, p_a, margin_pct


async def _analyse_snapshot(league: dict, matches: list, now: datetime, conn=None) -> list:
    """
    Analyserer en liste med kamper fra snapshot.
    Bruker Pinnacle som sharp reference.
    SCORE = EV_pct × log(bookmaker_count + 1)
    Atomic signals (velocity + xG) er ADDITIVE — aldri blokkerende.
    """
    candidates = []
    league_pick_count = 0

    for m in matches:
        if league_pick_count >= MAX_PICKS_PER_LEAGUE:
            break

        try:
            commence = datetime.fromisoformat(
                m["commence_time"].replace("Z", "+00:00")
            )
            hours = (commence - now).total_seconds() / 3600
            if not (1 <= hours <= MATCH_HOURS_MAX):
                continue

            bookmakers = m.get("bookmakers", [])
            if not bookmakers:
                continue

            # ── Soft Benchmark (BENCHMARK env var, default: unibet) ──────────
            soft_bk = next(
                (bk for bk in bookmakers if bk.get("key") == BENCHMARK),
                None
            )
            if not soft_bk:
                continue  # FALLBACK: ingen benchmark-linje = ingen pick

            soft_h2h = next(
                (mkt for mkt in soft_bk.get("markets", []) if mkt["key"] == "h2h"),
                None
            )
            if not soft_h2h:
                continue  # Ingen benchmark h2h = ingen pick

            soft_out = {o["name"]: o["price"] for o in soft_h2h.get("outcomes", [])}
            soft_home_price = soft_out.get(m["home_team"])
            soft_away_price = soft_out.get(m["away_team"])
            soft_draw_price = soft_out.get("Draw")

            if not soft_home_price or not soft_away_price or not soft_draw_price:
                continue  # Benchmark mangler outcome = ingen pick

            p_home, p_draw, p_away, soft_margin = _pinnacle_no_vig(soft_home_price, soft_draw_price, soft_away_price)

            # ── Pinnacle: KUN CLV-referanse ──────────────────────────────────
            pinnacle_bk = next(
                (bk for bk in bookmakers if bk.get("key") == "pinnacle"),
                None
            )
            clv_missing = pinnacle_bk is None
            pin_home = pin_draw = pin_away = None
            pin_margin = None
            if pinnacle_bk:
                pin_h2h = next(
                    (mkt for mkt in pinnacle_bk.get("markets", []) if mkt["key"] == "h2h"),
                    None
                )
                if pin_h2h:
                    pin_out = {o["name"]: o["price"] for o in pin_h2h.get("outcomes", [])}
                    pin_home = pin_out.get(m["home_team"])
                    pin_away = pin_out.get(m["away_team"])
                    pin_draw = pin_out.get("Draw")
                    if pin_home and pin_away and pin_draw:
                        _, _, _, pin_margin = _pinnacle_no_vig(pin_home, pin_draw, pin_away)

            # Soft book odds — benchmark OG Pinnacle ekskludert fra betting-targets
            num_bk = len(bookmakers)
            home_list, draw_list, away_list = [], [], []
            over25_list, over35_list = [], []
            spreads_home: dict = {}
            spreads_away: dict = {}

            for bk in bookmakers:
                if bk.get("key") in ("pinnacle", BENCHMARK):
                    continue  # Begge er referanser, ikke betting-targets
                for mkt in bk.get("markets", []):
                    if mkt["key"] == "h2h":
                        outcomes_map = {o["name"]: o["price"] for o in mkt.get("outcomes", [])}
                        if m["home_team"] in outcomes_map:
                            home_list.append(outcomes_map[m["home_team"]])
                        if m["away_team"] in outcomes_map:
                            away_list.append(outcomes_map[m["away_team"]])
                        if "Draw" in outcomes_map:
                            draw_list.append(outcomes_map["Draw"])
                    elif mkt["key"] == "totals":
                        for o in mkt.get("outcomes", []):
                            pt = o.get("point", 0)
                            if abs(pt - 2.5) < 0.1 and o["name"] == "Over":
                                over25_list.append(o["price"])
                            elif abs(pt - 3.5) < 0.1 and o["name"] == "Over":
                                over35_list.append(o["price"])
                    elif mkt["key"] == "spreads":
                        for o in mkt.get("outcomes", []):
                            pt  = round(o.get("point", 0), 1)
                            nm  = o.get("name", "")
                            prc = o.get("price")
                            if not prc:
                                continue
                            if nm == m["home_team"]:
                                spreads_home.setdefault(pt, []).append(prc)
                            elif nm == m["away_team"]:
                                spreads_away.setdefault(pt, []).append(prc)

            if not home_list:
                continue

            # Fase 0: minimum antall bookmakers (Pinnacle inkludert i num_bk)
            if num_bk < MIN_BOOKMAKERS:
                continue

            # Best consensus odds (highest = softest book)
            best_home = max(home_list)
            best_draw = max(draw_list) if draw_list else None
            best_away = max(away_list) if away_list else None

            match_pick_count = 0

            # ── outcomes_to_check: (b365_prob, soft_odds, label, market, pin_ref_odds) ──
            # b365_prob = Bet365 no-vig probability (benchmark)
            # pin_ref_odds = Pinnacle odds same outcome (CLV reference, may be None)
            outcomes_to_check = []

            # H2H — Bet365 no-vig prob som model (p_home/draw/away allerede beregnet)
            if best_home and ODDS_MIN <= best_home <= ODDS_MAX:
                outcomes_to_check.append((p_home, best_home, f"{m['home_team']} vinner", "h2h", pin_home))
            if best_draw and ODDS_MIN <= best_draw <= ODDS_MAX:
                outcomes_to_check.append((p_draw, best_draw, "Uavgjort", "h2h", pin_draw))
            if best_away and ODDS_MIN <= best_away <= ODDS_MAX:
                outcomes_to_check.append((p_away, best_away, f"{m['away_team']} vinner", "h2h", pin_away))

            # Over 2.5 — soft benchmark totals no-vig som model_prob
            if over25_list and ODDS_MIN <= max(over25_list) <= ODDS_MAX:
                soft_totals = next(
                    (mkt for mkt in soft_bk.get("markets", []) if mkt["key"] == "totals"),
                    None
                )
                soft_over25 = soft_under25 = None
                pin_over25_ref = None
                if soft_totals:
                    for o in soft_totals.get("outcomes", []):
                        if abs(o.get("point", 0) - 2.5) < 0.1:
                            if o["name"] == "Over":
                                soft_over25 = o["price"]
                            elif o["name"] == "Under":
                                soft_under25 = o["price"]
                if soft_over25 and soft_under25:
                    raw_o, raw_u = 1 / soft_over25, 1 / soft_under25
                    p_over25 = raw_o / (raw_o + raw_u)
                    if pinnacle_bk:
                        pin_totals = next(
                            (mkt for mkt in pinnacle_bk.get("markets", []) if mkt["key"] == "totals"),
                            None
                        )
                        if pin_totals:
                            for o in pin_totals.get("outcomes", []):
                                if abs(o.get("point", 0) - 2.5) < 0.1 and o["name"] == "Over":
                                    pin_over25_ref = o["price"]
                    outcomes_to_check.append((p_over25, max(over25_list), "Over 2.5 mål", "totals_over25", pin_over25_ref))

            # Over 3.5 — soft benchmark totals no-vig som model_prob
            if over35_list and ODDS_MIN <= max(over35_list) <= ODDS_MAX:
                soft_totals = next(
                    (mkt for mkt in soft_bk.get("markets", []) if mkt["key"] == "totals"),
                    None
                )
                soft_over35 = soft_under35 = None
                pin_over35_ref = None
                if soft_totals:
                    for o in soft_totals.get("outcomes", []):
                        if abs(o.get("point", 0) - 3.5) < 0.1:
                            if o["name"] == "Over":
                                soft_over35 = o["price"]
                            elif o["name"] == "Under":
                                soft_under35 = o["price"]
                if soft_over35 and soft_under35:
                    raw_o, raw_u = 1 / soft_over35, 1 / soft_under35
                    p_over35 = raw_o / (raw_o + raw_u)
                    if pinnacle_bk:
                        pin_totals = next(
                            (mkt for mkt in pinnacle_bk.get("markets", []) if mkt["key"] == "totals"),
                            None
                        )
                        if pin_totals:
                            for o in pin_totals.get("outcomes", []):
                                if abs(o.get("point", 0) - 3.5) < 0.1 and o["name"] == "Over":
                                    pin_over35_ref = o["price"]
                    outcomes_to_check.append((p_over35, max(over35_list), "Over 3.5 mål", "totals_over35", pin_over35_ref))

            # Spreads — soft benchmark spreads no-vig som model_prob
            b365_spreads = next(
                (mkt for mkt in soft_bk.get("markets", []) if mkt["key"] == "spreads"),
                None
            )
            pin_spreads = next(
                (mkt for mkt in pinnacle_bk.get("markets", []) if mkt["key"] == "spreads"),
                None
            ) if pinnacle_bk else None

            if b365_spreads and (spreads_home or spreads_away):
                soft_sp_map: dict = {}
                for o in b365_spreads.get("outcomes", []):
                    pt  = round(o.get("point", 0), 1)
                    nm  = o.get("name", "")
                    prc = o.get("price")
                    if not prc:
                        continue
                    soft_sp_map.setdefault(pt, {})
                    if nm == m["home_team"]:
                        soft_sp_map[pt]["home"] = prc
                    elif nm == m["away_team"]:
                        soft_sp_map[pt]["away"] = prc

                # Pinnacle spreads CLV reference map
                pin_sp_ref_map: dict = {}
                if pin_spreads:
                    for o in pin_spreads.get("outcomes", []):
                        pt  = round(o.get("point", 0), 1)
                        nm  = o.get("name", "")
                        prc = o.get("price")
                        if not prc:
                            continue
                        pin_sp_ref_map.setdefault(pt, {})
                        if nm == m["home_team"]:
                            pin_sp_ref_map[pt]["home"] = prc
                        elif nm == m["away_team"]:
                            pin_sp_ref_map[pt]["away"] = prc

                for pt, soft_sides in soft_sp_map.items():
                    b365_sp_home = soft_sides.get("home")
                    b365_sp_away = soft_sides.get("away")
                    if not b365_sp_home or not b365_sp_away:
                        continue
                    raw_h = 1 / b365_sp_home
                    raw_a = 1 / b365_sp_away
                    total = raw_h + raw_a
                    p_sp_home = raw_h / total
                    p_sp_away = raw_a / total

                    pin_sp_h = pin_sp_ref_map.get(pt, {}).get("home")
                    pin_sp_a = pin_sp_ref_map.get(-pt, {}).get("away")

                    best_sp_home = max(spreads_home[pt]) if pt in spreads_home else None
                    best_sp_away = max(spreads_away.get(-pt, [])) if -pt in spreads_away else None

                    label_pt = f"+{pt}" if pt > 0 else str(pt)
                    if best_sp_home and ODDS_MIN <= best_sp_home <= ODDS_MAX:
                        outcomes_to_check.append((
                            p_sp_home, best_sp_home,
                            f"{m['home_team']} handicap {label_pt}", "spreads", pin_sp_h
                        ))
                    neg_pt = -pt
                    neg_label = f"+{neg_pt}" if neg_pt > 0 else str(neg_pt)
                    if best_sp_away and ODDS_MIN <= best_sp_away <= ODDS_MAX:
                        outcomes_to_check.append((
                            p_sp_away, best_sp_away,
                            f"{m['away_team']} handicap {neg_label}", "spreads", pin_sp_a
                        ))

            # ── Evaluerings-loop: BENCHMARK (soft book) som EV/edge-referanse ─
            match_id = m.get("id", "")

            for soft_model_prob, target_odds, pick_label, market_type, pin_ref_odds in outcomes_to_check:
                if match_pick_count >= MAX_PICKS_PER_MATCH:
                    break

                target_prob = 1 / target_odds
                soft_ev   = round((soft_model_prob * target_odds - 1) * 100, 2)
                soft_edge = round((soft_model_prob - target_prob) * 100, 2)
                soft_fair = round(1 / soft_model_prob, 3) if soft_model_prob > 0 else None

                # Dual Benchmark gate (legacy — alltid aktiv)
                if soft_ev < SOFT_EV_MIN:
                    continue
                if soft_edge < SOFT_EDGE_MIN:
                    continue
                if 75 < CONFIDENCE_MIN:
                    continue

                # ── Atomic Signals (ADDITIVE — aldri blokkerende) ────────────
                velocity_result = await calculate_odds_velocity(
                    conn, match_id, target_odds, market_type, BENCHMARK
                )
                # xG — rate-limited (football_limiter)
                if football_limiter:
                    await football_limiter.acquire()
                xg_result = await get_xg_divergence(
                    m["home_team"], m["away_team"], league["name"],
                    cfg.FOOTBALL_DATA_API_KEY,
                    conn=conn,
                    match_id=match_id,
                )
                # Signal 3 — Weather (rate-limited + circuit breaker)
                weather_result = {"signal": "WEATHER_UNAVAILABLE", "atomic_points": 0}
                if WeatherSignal and cfg.OPENWEATHER_API_KEY:
                    try:
                        if weather_limiter:
                            await weather_limiter.acquire()
                        kickoff_dt = datetime.fromisoformat(
                            m["commence_time"].replace("Z", "+00:00")
                        )
                        async with WeatherSignal(cfg.OPENWEATHER_API_KEY) as ws:
                            weather_result = await ws.get_signal(m["home_team"], kickoff_dt)
                    except Exception as _we:
                        logger.warning(f"[WeatherSignal] Exception: {_we}")

                # Signal 4 — Referee (rate-limited + circuit breaker)
                referee_result = {"signal": "REFEREE_UNAVAILABLE", "atomic_points": 0}
                if RefereeSignal and cfg.FOOTBALL_DATA_API_KEY:
                    try:
                        if football_limiter:
                            await football_limiter.acquire()
                        _ref_fallback = {"signal": "REFEREE_UNAVAILABLE", "atomic_points": 0}
                        if referee_breaker:
                            @referee_breaker.protect(fallback=_ref_fallback)
                            async def _get_referee():
                                async with RefereeSignal(cfg.FOOTBALL_DATA_API_KEY) as rs:
                                    return await rs.get_signal(
                                        m["home_team"], m["away_team"], league["name"]
                                    )
                            referee_result = await _get_referee()
                        else:
                            async with RefereeSignal(cfg.FOOTBALL_DATA_API_KEY) as rs:
                                referee_result = await rs.get_signal(
                                    m["home_team"], m["away_team"], league["name"]
                                )
                    except Exception as _re:
                        logger.warning(f"[RefereeSignal] Exception: {_re}")

                # Signal 5 — Scoring streak (home + away)
                streak_home_result = {"streak_signal": "UNAVAILABLE", "atomic_points": 0}
                streak_away_result = {"streak_signal": "UNAVAILABLE", "atomic_points": 0}
                if cfg.FOOTBALL_DATA_API_KEY:
                    try:
                        if football_limiter:
                            await football_limiter.acquire()
                        streak_home_result = await get_scoring_streak(
                            m["home_team"], league["name"], cfg.FOOTBALL_DATA_API_KEY
                        )
                    except Exception as _she:
                        logger.warning(f"[StreakSignal] home feil: {_she}")
                    try:
                        if football_limiter:
                            await football_limiter.acquire()
                        streak_away_result = await get_scoring_streak(
                            m["away_team"], league["name"], cfg.FOOTBALL_DATA_API_KEY
                        )
                    except Exception as _sae:
                        logger.warning(f"[StreakSignal] away feil: {_sae}")

                atomic_result = calculate_atomic_score(
                    velocity_result, xg_result, soft_edge, soft_ev,
                    weather_result=weather_result,
                    referee_result=referee_result,
                    streak_home_result=streak_home_result,
                    streak_away_result=streak_away_result,
                )
                # ────────────────────────────────────────────────────────────

                # Pinnacle CLV referanse
                pinnacle_clv = None
                if pin_ref_odds and soft_model_prob > 0:
                    pinnacle_clv = round((soft_model_prob - 1 / pin_ref_odds) * 100, 2)
                outcome_clv_missing = clv_missing or pin_ref_odds is None

                score = round(soft_ev * math.log(num_bk + 1), 4)

                candidates.append({
                    "league_key": league["key"],
                    "league": league["name"],
                    "league_flag": league["flag"],
                    "home_team": m["home_team"],
                    "away_team": m["away_team"],
                    "match_id": match_id,
                    "commence_time": m["commence_time"],
                    "hours_to_kickoff": round(hours, 1),
                    "pick": pick_label,
                    "odds": target_odds,
                    "market_type": market_type,
                    "model_prob": round(soft_model_prob * 100, 2),
                    "market_prob": round(target_prob * 100, 2),
                    "edge": soft_edge,
                    "ev": soft_ev,
                    "soft_edge": soft_edge,
                    "soft_ev": soft_ev,
                    "soft_book": BENCHMARK,
                    "pinnacle_clv": pinnacle_clv,
                    "clv_missing": outcome_clv_missing,
                    "benchmark_book": BENCHMARK,
                    "clv_reference_book": "pinnacle",
                    "score": score,
                    "num_bookmakers": num_bk,
                    "pinnacle_opening": round(pin_ref_odds, 2) if pin_ref_odds else None,
                    "pinnacle_fair_odds": soft_fair,
                    "pinnacle_margin": round(pin_margin, 2) if pin_margin else None,
                    # Atomic Signal fields
                    "atomic_score": atomic_result["atomic_score"],
                    "atomic_verdict": atomic_result["verdict"],
                    "atomic_gate_passed": atomic_result["gate_passed"],
                    "signals_triggered": atomic_result["signals_triggered"],
                    "signal_velocity": velocity_result.get("velocity_type"),
                    "signal_xg": xg_result.get("signal"),
                    "xg_divergence_home": xg_result.get("home_divergence"),
                    "xg_divergence_away": xg_result.get("away_divergence"),
                    "signal_weather": weather_result.get("signal"),
                    "signal_referee": referee_result.get("signal"),
                    "referee_name": referee_result.get("referee_name"),
                    "wind_speed": weather_result.get("wind_ms"),
                    "temperature": weather_result.get("temperature_c"),
                    "signal_streak_home": atomic_result["streak_home_signal"],
                    "signal_streak_away": atomic_result["streak_away_signal"],
                    "market_hint": atomic_result["market_hint"],
                    # Tier + Kelly (Decimal engine hvis tilgjengelig)
                    "tier": atomic_result["tier"],
                    "tier_label": atomic_result["tier_label"],
                    "kelly_multiplier": atomic_result["kelly_multiplier"],
                    "kelly_stake": float(
                        _kelly_engine.calculate(soft_edge, target_odds, atomic_result["tier"]).stake_units
                    ) if _kelly_engine else calculate_kelly_stake(
                        soft_edge, target_odds, tier=atomic_result["tier"]
                    ),
                    "post_telegram": atomic_result["post_telegram"],
                })
                match_pick_count += 1
                league_pick_count += 1

        except Exception as e:
            logger.warning(f"[Analyse] Match-feil i {league['key']}: {e}")
            continue

    return candidates


async def run_analysis():
    """
    Kjører kl. 07:05, 14:05, 20:00 UTC.
    Leser siste snapshot fra DB, analyserer, lagrer picks med dedup + poster til Telegram.
    """
    logger.info("[Analyse] run_analysis startet")

    if not db_state.connected or not db_state.pool:
        logger.warning("[Analyse] DB offline — hopper over")
        return

    now = datetime.now(timezone.utc)
    candidates = []
    total_scanned = 0

    async with db_state.pool.acquire() as conn:
        for league in SCAN_LEAGUES:
            try:
                row = await conn.fetchrow("""
                    SELECT data FROM odds_snapshots
                    WHERE league_key = $1
                    ORDER BY snapshot_time DESC
                    LIMIT 1
                """, league["key"])

                if not row:
                    logger.info(f"[Analyse] Ingen snapshot for {league['key']}")
                    continue

                matches = json.loads(row["data"])
                total_scanned += len(matches)
                picks = await _analyse_snapshot(league, matches, now, conn=conn)
                candidates.extend(picks)

            except Exception as e:
                logger.warning(f"[Analyse] Feil for {league['key']}: {e}")
                continue

    if not candidates:
        logger.info("[Analyse] Ingen kvalifiserte picks denne runden")
        return

    # Sorter: today-filter (kickoff 0-12h) prioriteres, deretter SCORE desc
    def _sort_key(x):
        hours = x.get("hours_to_kickoff", 999)
        today_priority = 1 if 0 <= hours <= 12 else 0
        return (today_priority, x["score"])

    candidates.sort(key=_sort_key, reverse=True)
    logger.info(f"[Analyse] {len(candidates)} kvalifiserte picks fra {total_scanned} kamper")

    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    newly_inserted = []

    async with db_state.pool.acquire() as conn:
        daily_posted = await conn.fetchval("""
            SELECT COUNT(*) FROM dagens_kamp
            WHERE telegram_posted = TRUE AND timestamp >= $1
        """, today_start)

        for pick in candidates:
            kickoff_dt = datetime.fromisoformat(pick["commence_time"].replace("Z", "+00:00"))

            exists = await conn.fetchval("""
                SELECT 1 FROM dagens_kamp
                WHERE home_team = $1 AND away_team = $2 AND kickoff = $3 AND pick = $4
                LIMIT 1
            """, pick["home_team"], pick["away_team"], kickoff_dt, pick["pick"])

            if exists:
                continue

            # Parse streak count fra signal string (e.g. "SCORING_5_OF_7" → 5)
            def _parse_streak_count(sig: str) -> int:
                if sig and sig.startswith("SCORING_"):
                    try:
                        return int(sig.split("_")[1])
                    except (IndexError, ValueError):
                        pass
                return 0

            streak_h_count = _parse_streak_count(pick.get("signal_streak_home", ""))
            streak_a_count = _parse_streak_count(pick.get("signal_streak_away", ""))

            row_id = await conn.fetchval("""
                INSERT INTO dagens_kamp
                    (match, league, home_team, away_team, pick, odds, stake,
                     edge, ev, confidence, kickoff, telegram_posted,
                     market_type, score, bookmaker_count, pinnacle_opening, total_scanned,
                     atomic_score, tier, tier_label, kelly_stake,
                     signals_triggered, signal_streak_home, signal_streak_away,
                     streak_home_count, streak_away_count, market_hint,
                     signal_velocity, signal_xg, signal_weather,
                     xg_divergence_home, xg_divergence_away)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,FALSE,$12,$13,$14,$15,$16,
                        $17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31)
                RETURNING id
            """,
                f"{pick['home_team']} vs {pick['away_team']}",
                f"{pick['league_flag']} {pick['league']}",
                pick["home_team"],
                pick["away_team"],
                pick["pick"],
                pick["odds"],
                5.0,
                pick["edge"],
                pick["ev"],
                75,
                kickoff_dt,
                pick["market_type"],
                pick["score"],
                pick["num_bookmakers"],
                pick.get("pinnacle_opening"),
                total_scanned,
                pick.get("atomic_score", 0),
                pick.get("tier", "MONITORED"),
                pick.get("tier_label", "📊 MONITORED"),
                pick.get("kelly_stake", 0.0),
                json.dumps(pick.get("signals_triggered", [])),
                pick.get("signal_streak_home", "NEUTRAL"),
                pick.get("signal_streak_away", "NEUTRAL"),
                streak_h_count,
                streak_a_count,
                pick.get("market_hint"),
                pick.get("signal_velocity"),
                pick.get("signal_xg"),
                pick.get("signal_weather"),
                pick.get("xg_divergence_home"),
                pick.get("xg_divergence_away"),
            )
            newly_inserted.append({"id": row_id, **pick, "total_scanned": total_scanned})
            logger.info(f"[Analyse] Ny pick (id={row_id}): {pick['pick']} @ {pick['odds']} SCORE={pick['score']}")

    if not newly_inserted:
        logger.info("[Analyse] Ingen nye picks (alle allerede i DB)")
        return

    if not cfg.TELEGRAM_TOKEN or not cfg.TELEGRAM_CHAT_ID:
        logger.warning("[Analyse] TELEGRAM mangler — lagret men ikke postet")
        return

    # STEG 7: Kun EDGE og ATOMIC postes til Telegram — MONITORED logges alltid til DB
    postable = [
        p for p in newly_inserted
        if p.get("post_telegram", False)
        and float(p.get("edge") or 0) >= 6
        and p.get("tier") in ("ATOMIC", "EDGE")
    ]
    posts_left = max(0, DAILY_POST_LIMIT - int(daily_posted))
    monitored_count = len(newly_inserted) - len(postable)
    if monitored_count > 0:
        logger.info(f"[Analyse] {monitored_count} MONITORED picks lagret i DB (ingen Telegram-posting)")

    rank = 1
    for pick in postable[:posts_left]:
        try:
            message = _format_pick_message(pick, rank=rank, total_scanned=total_scanned)
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(
                    f"https://api.telegram.org/bot{cfg.TELEGRAM_TOKEN}/sendMessage",
                    json={"chat_id": cfg.TELEGRAM_CHAT_ID, "text": message},
                )
            if resp.status_code == 200:
                async with db_state.pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE dagens_kamp SET telegram_posted = TRUE WHERE id = $1",
                        pick["id"]
                    )
                logger.info(f"[Analyse] Postet til Telegram: {pick['pick']} — {pick['home_team']} vs {pick['away_team']} [{pick.get('tier','?')}]")
            else:
                logger.error(f"[Analyse] Telegram feil {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            logger.exception(f"[Analyse] Feil ved posting id={pick['id']}: {e}")
        rank += 1

    skipped = len(postable) - posts_left
    if skipped > 0:
        logger.info(f"[Analyse] {skipped} picks lagret men ikke postet (grense {DAILY_POST_LIMIT} nådd)")


# ─────────────────────────────────────────────────────────
# CLV TRACKING
# ─────────────────────────────────────────────────────────
async def _log_notion_pick(pick: dict):
    """Logger en kvalifisert pick til Notion MATCH_PREDICTIONS database."""
    if not cfg.NOTION_TOKEN or not cfg.NOTION_DB_ID:
        logger.warning("[Notion] NOTION_TOKEN eller NOTION_DATABASE_ID mangler")
        return
    try:
        kickoff = pick.get("kickoff") or pick.get("commence_time", "")
        if hasattr(kickoff, "isoformat"):
            kickoff_str = kickoff.date().isoformat()
        elif isinstance(kickoff, str):
            kickoff_str = kickoff[:10]
        else:
            kickoff_str = datetime.now(timezone.utc).date().isoformat()

        league_name = pick.get("league", "")
        if pick.get("league_flag"):
            league_name = league_name.replace(pick["league_flag"], "").strip()

        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                "https://api.notion.com/v1/pages",
                headers={
                    "Authorization": f"Bearer {cfg.NOTION_TOKEN}",
                    "Notion-Version": "2022-06-28",
                    "Content-Type": "application/json",
                },
                json={
                    "parent": {"database_id": cfg.NOTION_DB_ID},
                    "properties": {
                        # Only properties that exist in the schema (verified 2026-03-12):
                        # Name(title), Liga(select→[La Liga,Serie A]), Hjemmelag/Bortelag(rich_text),
                        # Kickoff(date), Pick(rich_text), Odds(number), Edge(rich_text),
                        # EV(rich_text), Confidence(number), Stake(rich_text),
                        # Status(select→[PENDING, NO BET])
                        "Name":       {"title": [{"text": {"content": f"{pick.get('home_team')} vs {pick.get('away_team')}"}}]},
                        "Liga":       {"select": {"name": league_name}} if league_name else {},
                        "Hjemmelag":  {"rich_text": [{"text": {"content": pick.get("home_team", "")}}]},
                        "Bortelag":   {"rich_text": [{"text": {"content": pick.get("away_team", "")}}]},
                        "Kickoff":    {"date": {"start": kickoff_str}},
                        "Pick":       {"rich_text": [{"text": {"content": pick.get("pick", "")}}]},
                        "Odds":       {"number": float(pick.get("odds") or 0)},
                        "Edge":       {"rich_text": [{"text": {"content": f"+{pick.get('edge', 0):.2f}%"}}]},
                        "EV":         {"rich_text": [{"text": {"content": f"+{pick.get('ev', 0):.2f}%"}}]},
                        "Confidence": {"number": float(pick.get("confidence") or 0)},
                        "Stake":      {"rich_text": [{"text": {"content": "5.0"}}]},
                        "Status":     {"select": {"name": "PENDING"}},
                        # NOTE: "QUALIFIED" is not a valid Status option.
                        # NOTE: "Market" and "Reason" properties do not exist in schema — omitted.
                    },
                },
            )
        if resp.status_code == 200:
            notion_id = resp.json().get("id", "?")
            logger.info(f"[Notion] Pick logget OK: {notion_id}")
        else:
            logger.warning(f"[Notion] Feil {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        logger.warning(f"[Notion] Exception: {e}")


async def pre_kickoff_check():
    """
    Kjøres inne i track_clv-jobben (hvert 30. minutt).
    Finner kamper med kickoff 60-150 min fremover, analyserer fra cache,
    logger alle resultater til daily_summaries og poster kvalifiserte picks.
    Kaller ALDRI Odds API — leser kun fra odds_snapshots.
    """
    try:
        if not db_state.connected or not db_state.pool:
            return

        now = datetime.now(timezone.utc)
        window_start = now + timedelta(minutes=60)
        window_end   = now + timedelta(minutes=150)
        stale_cutoff = now - timedelta(hours=3)
        today_date   = now.date()

        # Finn ferske snapshots innenfor tidsvinduet
        eligible_matches = []

        async with db_state.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT league_key, data, snapshot_time
                FROM odds_snapshots
                WHERE snapshot_time >= $1
                ORDER BY league_key, snapshot_time DESC
            """, stale_cutoff)

        # Grupper: siste snapshot per liga
        latest_by_league = {}
        for row in rows:
            if row["league_key"] not in latest_by_league:
                latest_by_league[row["league_key"]] = row

        for league_key, row in latest_by_league.items():
            league = next((l for l in SCAN_LEAGUES if l["key"] == league_key), None)
            if not league:
                continue
            try:
                matches = json.loads(row["data"])
            except Exception:
                continue
            for m in matches:
                try:
                    commence = datetime.fromisoformat(
                        m["commence_time"].replace("Z", "+00:00")
                    )
                    if window_start <= commence <= window_end:
                        match_id = f"{m['home_team']}|{m['away_team']}|{m['commence_time'][:10]}"
                        eligible_matches.append({
                            "league": league,
                            "match": m,
                            "match_id": match_id,
                            "snapshot_time": row["snapshot_time"],
                        })
                except Exception:
                    continue

        if not eligible_matches:
            return

        logger.info(f"[PreKickoff] {len(eligible_matches)} kamper i 60-150 min vindu")

        # Sjekk deduplication: hvilke match_ids er allerede behandlet i dag
        async with db_state.pool.acquire() as conn:
            existing = await conn.fetch("""
                SELECT match_id FROM daily_summaries
                WHERE trigger_type = 'pre_kickoff'
                  AND date = $1
            """, today_date)
        already_done = {r["match_id"] for r in existing}

        to_process = [e for e in eligible_matches if e["match_id"] not in already_done]

        if not to_process:
            logger.info("[PreKickoff] Alle kamper allerede behandlet i dag")
            return

        logger.info(f"[PreKickoff] Analyserer {len(to_process)} nye kamper")

        for entry in to_process:
            league  = entry["league"]
            m       = entry["match"]
            match_id = entry["match_id"]
            result_label = "NO_BET"
            reason_text  = "Ingen picks kvalifiserte (EV < 3%)"

            try:
                picks = await _analyse_snapshot(league, [m], now, conn=None)

                if picks:
                    best = picks[0]
                    result_label = "QUALIFIED"
                    reason_text  = f"EV={best['ev']}% Edge={best['edge']}% SCORE={best['score']:.2f}"

                    # Lagre i dagens_kamp
                    kickoff_dt = datetime.fromisoformat(
                        m["commence_time"].replace("Z", "+00:00")
                    )
                    async with db_state.pool.acquire() as conn:
                        row_id = await conn.fetchval("""
                            INSERT INTO dagens_kamp
                                (match, league, home_team, away_team, pick, odds, stake,
                                 edge, ev, confidence, kickoff, telegram_posted,
                                 market_type, score, bookmaker_count, pinnacle_opening)
                            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,FALSE,$12,$13,$14,$15)
                            ON CONFLICT DO NOTHING
                            RETURNING id
                        """,
                            f"{m['home_team']} vs {m['away_team']}",
                            f"{league['flag']} {league['name']}",
                            m["home_team"], m["away_team"],
                            best["pick"], best["odds"], 5.0,
                            best["edge"], best["ev"], 75, kickoff_dt,
                            best["market_type"], best["score"],
                            best["num_bookmakers"],
                            best.get("pinnacle_opening"),
                        )

                    if row_id:
                        best["id"] = row_id
                        # Notion
                        best_with_kickoff = {**best, "kickoff": kickoff_dt}
                        await _log_notion_pick(best_with_kickoff)

                        # Telegram
                        if cfg.TELEGRAM_TOKEN and cfg.TELEGRAM_CHAT_ID:
                            msg = _format_pick_message(best_with_kickoff, rank=1)
                            async with httpx.AsyncClient(timeout=15) as client:
                                tresp = await client.post(
                                    f"https://api.telegram.org/bot{cfg.TELEGRAM_TOKEN}/sendMessage",
                                    json={"chat_id": cfg.TELEGRAM_CHAT_ID, "text": msg},
                                )
                            if tresp.status_code == 200:
                                async with db_state.pool.acquire() as conn:
                                    await conn.execute(
                                        "UPDATE dagens_kamp SET telegram_posted=TRUE WHERE id=$1",
                                        row_id
                                    )
                                logger.info(f"[PreKickoff] Postet til Telegram: {best['pick']} — {m['home_team']} vs {m['away_team']}")
                else:
                    picks_ev = []
                    all_raw = await _analyse_snapshot(league, [m], now, conn=None)
                    reason_text = "EV < 3% mot Pinnacle fair odds for alle outcomes"

            except Exception as e:
                result_label = "ERROR"
                reason_text  = str(e)[:200]
                logger.warning(f"[PreKickoff] Feil for {match_id}: {e}")

            # Logg alltid til daily_summaries
            try:
                async with db_state.pool.acquire() as conn:
                    await conn.execute("""
                        INSERT INTO daily_summaries
                            (date, trigger_type, match_id, result, reason, num_picks, profit)
                        VALUES ($1, 'pre_kickoff', $2, $3, $4, $5, 0)
                    """, today_date, match_id, result_label, reason_text,
                        1 if result_label == "QUALIFIED" else 0)
            except Exception as e:
                logger.warning(f"[PreKickoff] daily_summaries insert feil: {e}")

        logger.info(f"[PreKickoff] Ferdig — {len(to_process)} kamper behandlet")

    except Exception as e:
        logger.error(f"[PreKickoff] Kritisk feil (CLV-tracker upåvirket): {e}")
        try:
            if db_state.connected and db_state.pool:
                async with db_state.pool.acquire() as conn:
                    await conn.execute("""
                        INSERT INTO daily_summaries
                            (date, trigger_type, match_id, result, reason, num_picks, profit)
                        VALUES ($1, 'pre_kickoff_error', 'SYSTEM', 'ERROR', $2, 0, 0)
                    """, datetime.now(timezone.utc).date(), str(e)[:200])
        except Exception:
            pass


async def track_clv():
    """
    Kjører hvert 30. minutt.
    For picks der kickoff er passert (kamp ferdig), henter Pinnacle-sluttodds
    og beregner CLV = (odds_taken / pinnacle_closing - 1) × 100.
    """
    await pre_kickoff_check()

    if not db_state.connected or not db_state.pool:
        return

    now = datetime.now(timezone.utc)
    cutoff_start = now - timedelta(hours=4)
    cutoff_end = now - timedelta(minutes=90)

    async with db_state.pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT dk.id, dk.match, dk.pick, dk.odds, dk.kickoff,
                   dk.home_team, dk.away_team, dk.league, dk.pinnacle_opening
            FROM dagens_kamp dk
            LEFT JOIN clv_records cr ON cr.pick_id = dk.id
            WHERE dk.kickoff BETWEEN $1 AND $2
              AND dk.pinnacle_opening IS NOT NULL
              AND cr.id IS NULL
        """, cutoff_start, cutoff_end)

    if not rows:
        return

    logger.info(f"[CLV] Tracker {len(rows)} picks for CLV")

    if not cfg.ODDS_API_KEY:
        return

    async with httpx.AsyncClient(timeout=20) as client:
        for row in rows:
            try:
                league_key = None
                for lg in SCAN_LEAGUES:
                    if lg["name"] in (row["league"] or ""):
                        league_key = lg["key"]
                        break
                if not league_key:
                    continue

                resp = await client.get(
                    f"https://api.the-odds-api.com/v4/sports/{league_key}/odds/",
                    params={
                        "apiKey": cfg.ODDS_API_KEY,
                        "regions": "eu",
                        "markets": "h2h",
                        "oddsFormat": "decimal",
                        "bookmakers": "pinnacle",
                    }
                )
                if resp.status_code != 200:
                    continue

                matches = resp.json()
                match_data = next(
                    (m for m in matches
                     if m.get("home_team") == row["home_team"]
                     and m.get("away_team") == row["away_team"]),
                    None
                )
                if not match_data:
                    continue

                pin_bk = next(
                    (bk for bk in match_data.get("bookmakers", []) if bk.get("key") == "pinnacle"),
                    None
                )
                if not pin_bk:
                    continue

                pin_h2h = next(
                    (mkt for mkt in pin_bk.get("markets", []) if mkt["key"] == "h2h"),
                    None
                )
                if not pin_h2h:
                    continue

                pick_name = row["pick"] or ""
                pin_closing = None
                for o in pin_h2h.get("outcomes", []):
                    if o["name"] in pick_name or pick_name in o["name"]:
                        pin_closing = o["price"]
                        break

                if not pin_closing:
                    continue

                odds_taken = float(row["odds"])
                pin_opening = float(row["pinnacle_opening"])
                clv_pct = round((odds_taken / pin_closing - 1) * 100, 2)

                async with db_state.pool.acquire() as conn:
                    await conn.execute("""
                        INSERT INTO clv_records
                            (pick_id, match, pick, odds_taken, pinnacle_opening, pinnacle_closing, clv_pct, kickoff)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    """,
                        row["id"],
                        row["match"],
                        row["pick"],
                        odds_taken,
                        pin_opening,
                        pin_closing,
                        clv_pct,
                        row["kickoff"],
                    )
                    await conn.execute(
                        "UPDATE dagens_kamp SET pinnacle_closing = $1, clv_pct = $2 WHERE id = $3",
                        pin_closing, clv_pct, row["id"]
                    )

                logger.info(f"[CLV] Pick id={row['id']}: CLV={clv_pct:+.1f}% (tatt {odds_taken} / closing {pin_closing})")

            except Exception as e:
                logger.warning(f"[CLV] Feil for pick id={row['id']}: {e}")
                continue


async def post_clv_rapport_telegram():
    """
    Kjører mandag kl. 08:00 UTC.
    Poster ukentlig CLV-rapport til Telegram.
    """
    logger.info("[CLV] post_clv_rapport_telegram startet")

    if not cfg.TELEGRAM_TOKEN or not cfg.TELEGRAM_CHAT_ID:
        return

    if not db_state.connected or not db_state.pool:
        return

    week_ago = datetime.now(timezone.utc) - timedelta(days=7)

    async with db_state.pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT clv_pct, odds_taken, pinnacle_closing, match, pick
            FROM clv_records
            WHERE tracked_at >= $1
            ORDER BY tracked_at DESC
        """, week_ago)

    if not rows:
        logger.info("[CLV] Ingen CLV-data denne uken")
        return

    clv_values = [float(r["clv_pct"]) for r in rows if r["clv_pct"] is not None]
    if not clv_values:
        return

    clv_snitt = round(sum(clv_values) / len(clv_values), 2)
    clv_positive = sum(1 for c in clv_values if c > 0)
    clv_rate = round(clv_positive / len(clv_values) * 100, 1)

    lines = [
        "📊 SESOMNOD CLV-RAPPORT",
        f"Uke {datetime.now(timezone.utc).isocalendar()[1]}",
        "━━━━━━━━━━━━━━━━━━━━━",
        "",
        f"Antall picks:      {len(clv_values)}",
        f"CLV-snitt:         {clv_snitt:+.2f}%",
        f"Positiv CLV-rate:  {clv_rate}%",
        "",
        "━━━━━━━━━━━━━━━━━━━━━",
        "Siste picks:",
    ]
    for r in rows[:5]:
        clv = float(r["clv_pct"]) if r["clv_pct"] else 0
        sign = "+" if clv >= 0 else ""
        lines.append(f"  {r['pick'][:20]:<20} CLV {sign}{clv:.1f}%")

    lines += ["", "SesomNod Engine — Beating the close."]

    message = "\n".join(lines)

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"https://api.telegram.org/bot{cfg.TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": cfg.TELEGRAM_CHAT_ID, "text": message},
            )
        if resp.status_code == 200:
            logger.info(f"[CLV] CLV-rapport postet (snitt={clv_snitt:+.2f}%)")
        else:
            logger.error(f"[CLV] Telegram feil {resp.status_code}")
    except Exception as e:
        logger.exception(f"[CLV] Feil ved posting: {e}")


# ─────────────────────────────────────────────────────────
# TELEGRAM MESSAGE FORMATTER
# ─────────────────────────────────────────────────────────
def _tg_bar(pct: float, w: int = 10) -> str:
    f = round(max(0, min(w, pct / 100 * w)))
    return "█" * f + "░" * (w - f)


def _get_scorers(home: str, away: str) -> list:
    """
    Returns top scorers for BOTH home and away team (max 2 each).
    Output: interleaved list [home1, away1, home2, away2], max 4 items.

    Strategy:
      Pass 1 - Competition scorer lists (limit=10 = free tier max)
               Tries 10 competition codes until both teams have >= 1 scorer.
      Pass 2 - Team roster fallback for any team still at 0 scorers.
               Searches by team name, fetches squad, filters attackers.
      Pass 3 - Honest "not available" — never shows wrong team.

    Team matching: uses token intersection ignoring noise words.
    Handles "Brighton & Hove Albion" vs "Brighton and Hove Albion" correctly.
    Uses urllib.parse.quote (not requests.utils.quote which does not exist).
    Uses correct football-data.org position strings (e.g. "Centre-Forward").
    """
    import os as _os
    import urllib.parse as _ul
    import requests as _req

    _key = cfg.FOOTBALL_DATA_API_KEY
    if not _key:
        return []

    _H = {"X-Auth-Token": _key}

    _NOISE = {
        "fc", "afc", "sc", "cf", "ac", "as", "bv", "sv",
        "the", "and", "de", "van", "&",
        "hove",
        "hotspur",
        "sporting",
        "athletic",
        "cp",
        "1903", "1905", "1907", "1908", "1899",
        "09", "04", "05",
    }

    def _tok(name: str) -> set:
        cleaned = name.lower().replace("&", " ").replace(".", " ")
        return set(cleaned.split()) - _NOISE

    def _match(pick_name: str, api_name: str) -> bool:
        pt = _tok(pick_name)
        at = _tok(api_name)
        shared = pt & at
        if not shared:
            return False
        _GENERIC = {
            "city", "united", "albion", "rovers", "wanderers",
            "town", "county", "athletic", "athletics",
        }
        meaningful = shared - _GENERIC
        return len(meaningful) >= 1

    _ATTACK = {
        "Centre-Forward",
        "Second Striker",
        "Left Winger",
        "Right Winger",
        "Attacking Midfield",
        "Left Midfield",
        "Right Midfield",
    }

    home_s: list = []
    away_s: list = []

    _COMPS = ["PL", "PD", "BL1", "SA", "FL1", "CL", "EL", "ECL", "DED", "PPL"]
    for comp in _COMPS:
        if len(home_s) >= 2 and len(away_s) >= 2:
            break
        try:
            r = _req.get(
                f"https://api.football-data.org/v4/competitions/{comp}"
                f"/scorers?limit=10",
                headers=_H, timeout=6)
            if r.status_code == 429:
                break
            if r.status_code != 200:
                continue
            for s in r.json().get("scorers", []):
                api_team = s.get("team", {}).get("name", "")
                name     = s.get("player", {}).get("name", "")
                goals    = int(s.get("numberOfGoals") or 0)
                is_home  = _match(home, api_team)
                is_away  = _match(away, api_team)
                if is_home and not is_away and len(home_s) < 2:
                    home_s.append({
                        "name": name, "team": home,
                        "goals": goals, "prob": min(65, 12 + goals * 3),
                    })
                elif is_away and not is_home and len(away_s) < 2:
                    away_s.append({
                        "name": name, "team": away,
                        "goals": goals, "prob": min(65, 12 + goals * 3),
                    })
        except _req.RequestException:
            continue

    def _roster(team_name: str) -> list:
        try:
            encoded = _ul.quote(team_name)
            r = _req.get(
                f"https://api.football-data.org/v4/teams?name={encoded}",
                headers=_H, timeout=6)
            if r.status_code != 200:
                return []
            teams = r.json().get("teams", [])
            if not teams:
                return []
            matched = next(
                (t for t in teams if _match(team_name, t.get("name", ""))),
                teams[0]
            )
            if not matched:
                return []
            r2 = _req.get(
                f"https://api.football-data.org/v4/teams/{matched['id']}",
                headers=_H, timeout=6)
            if r2.status_code != 200:
                return []
            squad = r2.json().get("squad", [])
            attackers = [
                p for p in squad
                if p.get("position", "") in _ATTACK
            ]
            return [
                {"name": p.get("name", ""), "team": team_name,
                 "goals": 0, "prob": 18}
                for p in attackers[:2]
            ]
        except _req.RequestException:
            return []

    if len(home_s) < 1:
        home_s = _roster(home)
    if len(away_s) < 1:
        away_s = _roster(away)

    if not home_s:
        home_s = [{"name": "Scorer ikke tilgjengelig",
                   "team": home, "goals": 0, "prob": 0}]
    if not away_s:
        away_s = [{"name": "Scorer ikke tilgjengelig",
                   "team": away, "goals": 0, "prob": 0}]

    home_s.sort(key=lambda x: x["goals"], reverse=True)
    away_s.sort(key=lambda x: x["goals"], reverse=True)

    out = []
    for i in range(2):
        if i < len(home_s):
            out.append(home_s[i])
        if i < len(away_s):
            out.append(away_s[i])

    return out[:4]

def _build_shap_block(xgb: dict | None) -> str:
    """Build SHAP explainability section for Telegram message."""
    if not xgb:
        return ""
    if not xgb.get("model_available"):
        return ""
    shap_top3 = xgb.get("shap_top3") or []
    if not shap_top3:
        return ""
    outcome_labels = {
        "HOME_WIN": "Hjemmeseier",
        "DRAW": "Uavgjort",
        "AWAY_WIN": "Borteseier",
    }
    predicted = outcome_labels.get(
        xgb.get("predicted_outcome", ""),
        xgb.get("predicted_outcome", "—"),
    )
    conf = xgb.get("confidence", 0)
    lines = [
        "──────────────────────────────",
        "🧠 AI-ANALYSE (XGBoost)",
        f"Prediksjon: {predicted} | Sikkerhet: {conf:.0%}",
        "",
        "Topp signaler:",
    ]
    for i, s in enumerate(shap_top3[:3], 1):
        arrow = "+" if s.get("direction") == "positiv" else "-"
        lines.append(
            f"  {i}. {arrow} {s.get('label', s.get('feature'))} "
            f"= {s.get('value')} ({s.get('shap_value'):+.3f})"
        )
    lines.append("")
    return "\n".join(lines) + "\n"


def _build_dc_kelly_block(pick: dict) -> str:
    """Build Dixon-Coles + Kelly section for Telegram message. Returns empty string if data unavailable."""
    dc = pick.get("dixon_coles")
    kelly = pick.get("kelly")
    if not dc:
        return ""
    if dc.get("fallback_used"):
        return ""
    block = (
        f"──────────────────────────────\n"
        f"🧮 DIXON-COLES MODEL\n"
        f"──────────────────────────────\n"
        f"Home: {dc['home_win_prob']:.0%} | Draw: {dc['draw_prob']:.0%} | Away: {dc['away_win_prob']:.0%}\n"
        f"BTTS: {dc['btts_prob']:.0%}\n"
    )
    if kelly and kelly.get("kelly_tier") == "UNVERIFIED":
        block += (
            f"⚠️ KELLY: IKKE TILGJENGELIG\n"
            f"Modellen mangler historisk data for disse lagene.\n"
            f"Ingen stake-anbefaling gis.\n"
        )
    elif kelly and kelly.get("is_value_bet"):
        block += (
            f"💰 Kelly: {kelly['recommended_stake_pct']:.1f}u (Half-Kelly) | "
            f"Edge: +{kelly['edge_pct']:.1f}% | {kelly['kelly_tier']}\n"
        )
    elif kelly:
        block += f"⚠️ No value at current odds (edge: {kelly['edge_pct']:.1f}%)\n"
    block += "\n"
    return block


def build_telegram_message(pick: dict, rank: int = 1, total_scanned: int = 0) -> str:
    home   = pick.get("home_team") or pick.get("match", "?").split(" vs ")[0]
    away   = pick.get("away_team") or (pick.get("match", "?").split(" vs ")[-1])
    if pick.get("league_flag") and pick.get("league"):
        league = f"{pick['league_flag']} {pick['league']}"
    else:
        league = pick.get("league", "Football")

    # Kickoff formatting
    kickoff = pick.get("kickoff") or pick.get("commence_time")
    if kickoff:
        if isinstance(kickoff, str):
            kickoff = datetime.fromisoformat(kickoff.replace("Z", "+00:00"))
        cet = kickoff + timedelta(hours=1)
        ko = cet.strftime("%-d. %b %H:%M CET")
    else:
        ko = str(pick.get("match_date") or pick.get("kickoff_cet", "?"))

    odds   = float(pick.get("odds") or pick.get("our_odds") or 0)
    edge   = float(pick.get("edge") or pick.get("soft_edge") or 0)
    ev     = float(pick.get("ev") or pick.get("ev_percent") or 0)
    score  = float(pick.get("score") or pick.get("atomic_score") or 0)
    books  = pick.get("num_bookmakers") or pick.get("bookmakers") or 1
    market_raw = pick.get("market_type") or "h2h"
    market = {"h2h": "1X2", "totals_over25": "Over 2.5", "totals_over35": "Over 3.5"}.get(market_raw, market_raw.upper())
    pick_label = pick.get("pick") or pick.get("pick_label") or f"{home} vinner"
    scan   = pick.get("total_scanned") or total_scanned or 0

    # Try real Pinnacle odds first
    pinnacle_raw = pick.get("pinnacle_h2h")
    if pinnacle_raw:
        import json as _json
        try:
            if isinstance(pinnacle_raw, str):
                p_odds = _json.loads(pinnacle_raw)
            else:
                p_odds = pinnacle_raw
            h = float(p_odds.get("home", 0))
            a = float(p_odds.get("away", 0))
            d = float(p_odds.get("draw", 0))
            if h > 1.0 and a > 1.0 and d > 1.0:
                total = 1/h + 1/d + 1/a
                fair_home = (1/h) / total
                fair_away = (1/a) / total
                xg_h = round(fair_home * 2.7, 2)
                xg_a = round(fair_away * 2.7, 2)
            else:
                xg_h = float(pick.get("signal_xg_home") or pick.get("xg_divergence_home") or 1.3)
                xg_a = float(pick.get("signal_xg_away") or pick.get("xg_divergence_away") or 1.1)
        except Exception:
            xg_h = float(pick.get("signal_xg_home") or 1.3)
            xg_a = float(pick.get("signal_xg_away") or 1.1)
    else:
        xg_h = float(pick.get("signal_xg_home") or pick.get("xg_divergence_home") or 1.3)
        xg_a = float(pick.get("signal_xg_away") or pick.get("xg_divergence_away") or 1.1)
    lam  = max(0.5, min(6.0, xg_h + xg_a))

    def _pcdf(n: int, l: float) -> float:
        t, term = 0.0, math.exp(-l)
        for k in range(n + 1):
            t += term
            term *= l / (k + 1)
        return t

    def pover(n: int) -> int:
        return round((1 - _pcdf(n, lam)) * 100)

    ph   = round(max(5, min(85, (xg_h / lam) * 70)))
    pa   = round(max(5, min(85, (xg_a / lam) * 60)))
    pd   = max(5, 100 - ph - pa)
    btts = round(max(15, min(85, (1 - math.exp(-xg_h)) * (1 - math.exp(-xg_a)) * 100)))

    omega = int(pick.get("omega_score") or 0)
    tier  = ("⚡ BRUTAL EDGE" if omega >= 72 else
             "💪 STRONG EDGE" if omega >= 55 else
             "👁 MONITORED"   if omega >= 40 else "📊 ANALYSE")

    scorers = _get_scorers(home, away)
    if scorers:
        scorer_block = ""
        medals = ["🥇", "🥈", "🥉", "🎖"]
        for i, s in enumerate(scorers[:4]):
            scorer_block += f"{medals[i]} {s['name']} ({s['team']}): ~{s['prob']}% — {s['goals']} mål sesongen\n"
    else:
        scorer_block = (
            f"📊 Scorer-data lastes ved kampstart\n"
            f"xG Hjem ({home[:15]}): {xg_h:.1f} — høy scoringsforventning\n"
            f"xG Borte ({away[:15]}): {xg_a:.1f}\n"
        )

    return (
        f"⚡ SESOMNOD ENGINE · {ko}\n"
        f"Football Decision Intelligence\n"
        f"──────────────────────────────\n\n"
        f"🏆 {league}\n\n"
        f"🎯 {home} VS {away}\n"
        f"🕐 {ko}\n\n"
        f"──────────────────────────────\n"
        f"📊 SANNSYNLIGHETER\n"
        f"──────────────────────────────\n"
        f"1️⃣  {home[:22]}: {ph}%\n"
        f"    {_tg_bar(ph)}\n"
        f"🤝  Uavgjort: {pd}%\n"
        f"    {_tg_bar(pd)}\n"
        f"2️⃣  {away[:22]}: {pa}%\n"
        f"    {_tg_bar(pa)}\n\n"
        f"──────────────────────────────\n"
        f"⚽ MÅL (xG {xg_h:.1f} vs {xg_a:.1f})\n"
        f"──────────────────────────────\n"
        f"Over 1.5: {pover(1)}%  {_tg_bar(pover(1))}\n"
        f"Over 2.5: {pover(2)}%  {_tg_bar(pover(2))}\n"
        f"Over 3.5: {pover(3)}%  {_tg_bar(pover(3))}\n"
        f"Over 4.5: {pover(4)}%  {_tg_bar(pover(4))}\n\n"
        f"──────────────────────────────\n"
        f"🔴 BTTS — Begge lag scorer\n"
        f"──────────────────────────────\n"
        f"JA:  {btts}%  {_tg_bar(btts)}\n"
        f"NEI: {100-btts}%  {_tg_bar(100-btts)}\n\n"
        f"──────────────────────────────\n"
        f"🎯 SCORER-PREDIKSJON\n"
        f"──────────────────────────────\n"
        f"{scorer_block}\n"
        f"──────────────────────────────\n"
        f"📈 EDGE ANALYSE\n"
        f"──────────────────────────────\n"
        f"Market: {market} | Odds: {odds}\n"
        f"Edge: +{edge:.2f}% | EV: +{ev:.2f}%\n"
        f"Score: {score:.2f} | Books: {books}\n"
        f"Scan: {scan} kamper analysert\n\n"
        + _build_dc_kelly_block(pick) +
        f"──────────────────────────────\n"
        f"🎯 MODEL DECISION  {tier}\n"
        f"──────────────────────────────\n"
        f"PICK: {pick_label}\n"
        f"ODDS: @ {odds} | RANK: #{rank} av dagen\n\n"
        + _build_shap_block(pick.get("xgboost")) +
        f"You don't get picks. You get control. ⚡\n"
        f"SesomNod gir deg kontroll og innsikt — ikke tilfeldigheter. Trenger du hjelp med spillavhengighet? Ring Hjelpelinjen gratis på 800 800 40."
    )


def _format_pick_message(pick: dict, rank: int = 1, total_scanned: int = 0) -> str:
    return build_telegram_message(pick, rank=rank, total_scanned=total_scanned)


# ─────────────────────────────────────────────────────────
# TIER TELEGRAM FORMATTER (UTF-16 safe, zoneinfo CET/CEST)
# ─────────────────────────────────────────────────────────
def format_telegram_pick(pick: dict) -> str:
    """
    Formaterer Telegram-melding.
    Telegram teller Unicode codepoints, ikke bytes.
    Garantert ≤200 codepoints.
    Trunkerer lagnavn til 12 tegn.
    """
    home = str(pick.get("home_team", ""))[:12]
    away = str(pick.get("away_team", ""))[:12]
    edge = round(float(pick.get("soft_edge", 0)), 1)
    score = pick.get("atomic_score", 0)
    odds = pick.get("odds", 0)
    kelly = pick.get("kelly_stake", 0)
    tier = pick.get("tier", "MONITORED")

    # Timezone-korrekt CET/CEST
    try:
        import zoneinfo
        kickoff_raw = pick.get("kickoff_time") or pick.get("commence_time", "")
        tz = zoneinfo.ZoneInfo("Europe/Oslo")
        dt = datetime.fromisoformat(str(kickoff_raw).replace("Z", "+00:00"))
        dt_local = dt.astimezone(tz)
        time_str = dt_local.strftime("%H:%M")
        tz_name = "CET" if dt_local.utcoffset().seconds == 3600 else "CEST"
    except Exception:
        kickoff_raw = pick.get("kickoff_time") or pick.get("commence_time", "")
        time_str = str(kickoff_raw)[:5] if kickoff_raw else "--:--"
        tz_name = "UTC"

    if tier == "ATOMIC":
        msg = (
            f"⚡ ATOMIC | {home} v {away} | "
            f"+{edge}% | {score}/9 signals | "
            f"BACK @{odds} ({kelly}u) | "
            f"{time_str} {tz_name}"
        )
    else:
        msg = (
            f"🎯 EDGE | {home} v {away} | "
            f"+{edge}% | {score}/9 signals | "
            f"BACK @{odds} ({kelly}u) | "
            f"{time_str} {tz_name}"
        )

    # Trunker til 200 Unicode codepoints (ikke bytes)
    if len(msg) > 200:
        msg = msg[:197] + "..."
    return msg


# ─────────────────────────────────────────────────────────
# LEGACY JOBS (beholdes for bakoverkompatibilitet)
# ─────────────────────────────────────────────────────────
async def post_dagens_kamp_telegram():
    """Kjører kl. 09:00 UTC — poster upostede picks fra dagens_kamp."""
    logger.info("[Scheduler] post_dagens_kamp_telegram startet")

    if not cfg.TELEGRAM_TOKEN or not cfg.TELEGRAM_CHAT_ID:
        return
    if not db_state.connected or not db_state.pool:
        return

    try:
        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

        async with db_state.pool.acquire() as conn:
            # Count picks already posted today (by kickoff window so date-of-insert doesn't matter)
            daily_posted = await conn.fetchval("""
                SELECT COUNT(*) FROM dagens_kamp
                WHERE telegram_posted = TRUE
                  AND kickoff BETWEEN $1 AND $1 + INTERVAL '24 hours'
            """, today_start)

            posts_left = max(0, DAILY_POST_LIMIT - int(daily_posted))
            if posts_left == 0:
                logger.info(f"[Scheduler] Daglig grense ({DAILY_POST_LIMIT}) nådd")
                return

            # Fetch unposted picks with kickoff today (within ±3h → +36h window)
            rows = await conn.fetch("""
                SELECT * FROM dagens_kamp
                WHERE telegram_posted = FALSE
                  AND kickoff BETWEEN NOW() - INTERVAL '3 hours' AND NOW() + INTERVAL '36 hours'
                ORDER BY score DESC NULLS LAST, ev DESC NULLS LAST
                LIMIT $1
            """, posts_left)

        if not rows:
            logger.info("[Scheduler] Ingen upostede picks i dag")
            return

        rank = 1
        for row in rows:
            pick_data = dict(row)
            try:
                message = _format_pick_message(pick_data, rank=rank)
                async with httpx.AsyncClient(timeout=15) as client:
                    resp = await client.post(
                        f"https://api.telegram.org/bot{cfg.TELEGRAM_TOKEN}/sendMessage",
                        json={"chat_id": cfg.TELEGRAM_CHAT_ID, "text": message},
                    )
                if resp.status_code == 200:
                    async with db_state.pool.acquire() as conn:
                        await conn.execute(
                            "UPDATE dagens_kamp SET telegram_posted = TRUE WHERE id = $1",
                            pick_data["id"]
                        )
                    logger.info(f"[Scheduler] Postet (09:00): {pick_data.get('pick')} — {pick_data.get('match')}")
                else:
                    logger.error(f"[Scheduler] Telegram feil {resp.status_code}: {resp.text[:200]}")
            except Exception as e:
                logger.exception(f"[Scheduler] Feil pick id={pick_data.get('id')}: {e}")
            rank += 1

    except Exception as e:
        logger.exception(f"[Scheduler] Uventet feil: {e}")


async def _check_live_results():
    """
    Runs every 15 minutes 24/7.
    Fetches FINISHED matches from football-data.org, updates result/score in
    dagens_kamp, fires MiroFish /close-clv, and logs result to Notion.
    Never raises — all errors are logged and swallowed.
    """
    FOOTBALL_API_KEY = cfg.FOOTBALL_DATA_API_KEY
    MIROFISH_URL = "https://mirofish-service-production.up.railway.app"

    if not FOOTBALL_API_KEY:
        logger.info("[Results] SKIP: No FOOTBALL_DATA_API_KEY")
        return

    if not db_state.connected or not db_state.pool:
        logger.info("[Results] SKIP: DB offline")
        return

    try:
        today = datetime.now(timezone.utc).date().isoformat()

        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(
                "https://api.football-data.org/v4/matches",
                headers={"X-Auth-Token": FOOTBALL_API_KEY},
                params={"dateFrom": today, "dateTo": today, "status": "FINISHED"},
            )

        if r.status_code != 200:
            logger.warning(f"[Results] football-data.org returned {r.status_code}: {r.text[:150]}")
            return

        finished = r.json().get("matches", [])
        if not finished:
            logger.info("[Results] No finished matches today")
            return

        logger.info(f"[Results] {len(finished)} finished matches from football-data.org")

        # Ensure new columns exist (idempotent — ADD COLUMN IF NOT EXISTS)
        async with db_state.pool.acquire() as conn:
            await conn.execute("""
                ALTER TABLE dagens_kamp
                ADD COLUMN IF NOT EXISTS home_score INTEGER,
                ADD COLUMN IF NOT EXISTS away_score INTEGER,
                ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ
            """)
            pending = await conn.fetch(
                """SELECT id, match, home_team, away_team, pick, odds, kickoff
                   FROM dagens_kamp
                   WHERE result IS NULL
                   AND kickoff::date = CURRENT_DATE"""
            )

        if not pending:
            logger.info("[Results] No pending picks today")
            return

        logger.info(f"[Results] {len(pending)} pending picks to check")
        mirofish_needed = False

        for pick in pending:
            pick_home = str(pick["home_team"] or "").lower().strip()
            pick_away = str(pick["away_team"] or "").lower().strip()

            # Match against football-data.org results (full name then shortName)
            matched = None
            for attempt in ("name", "shortName"):
                for m in finished:
                    api_home = m.get("homeTeam", {}).get(attempt, "").lower().strip()
                    api_away = m.get("awayTeam", {}).get(attempt, "").lower().strip()
                    if (pick_home in api_home or api_home in pick_home) and \
                       (pick_away in api_away or api_away in pick_away):
                        matched = m
                        break
                if matched:
                    break

            if not matched:
                logger.info(f"[Results] No match found for {pick['home_team']} vs {pick['away_team']}")
                continue

            score = matched.get("score", {}).get("fullTime", {})
            home_score = score.get("home")
            away_score = score.get("away")
            if home_score is None or away_score is None:
                continue

            result_str = f"{home_score}-{away_score}"
            total_goals = home_score + away_score
            btts = home_score > 0 and away_score > 0

            # Determine outcome from the `pick` field
            pick_type = str(pick["pick"] or "").lower().strip()
            pick_won = None
            if pick_type == "draw":
                pick_won = home_score == away_score
            elif pick_type in ("home", "home win", "1"):
                pick_won = home_score > away_score
            elif pick_type in ("away", "away win", "2"):
                pick_won = away_score > home_score
            elif "over 3.5" in pick_type:
                pick_won = total_goals > 3
            elif "over 2.5" in pick_type or "over2.5" in pick_type:
                pick_won = total_goals > 2
            elif "over 1.5" in pick_type:
                pick_won = total_goals > 1
            elif "over 0.5" in pick_type:
                pick_won = total_goals > 0
            elif "btts no" in pick_type:
                pick_won = not btts
            elif "btts" in pick_type:
                pick_won = btts
            elif "under 2.5" in pick_type:
                pick_won = total_goals < 3
            elif "under 3.5" in pick_type:
                pick_won = total_goals < 4

            outcome_str = "WIN" if pick_won else "LOSS" if pick_won is False else "VOID"

            async with db_state.pool.acquire() as conn:
                await conn.execute(
                    """UPDATE dagens_kamp
                       SET result=$1, home_score=$2, away_score=$3, updated_at=NOW()
                       WHERE id=$4""",
                    outcome_str, home_score, away_score, pick["id"],
                )

            logger.info(
                f"[Results] {pick['home_team']} vs {pick['away_team']}: "
                f"{result_str} → {outcome_str} (pick: {pick['pick']})"
            )
            mirofish_needed = True

            # Log to Notion (fire-and-forget)
            if pick_won is not None:
                pick_dict = dict(pick)
                pick_dict["match_name"] = pick_dict.get("match", "")
                asyncio.create_task(_log_result_to_notion(pick_dict, outcome_str, None))

        # Trigger MiroFish close-clv once per run if any results were written
        if mirofish_needed:
            try:
                async with httpx.AsyncClient(timeout=10) as client:
                    await client.post(f"{MIROFISH_URL}/close-clv")
                logger.info("[Results] MiroFish /close-clv triggered")
            except Exception as mf_err:
                logger.warning(f"[Results] MiroFish error (non-fatal): {mf_err}")

    except Exception as e:
        logger.warning(f"[Results] Top-level exception: {e}")


# ─────────────────────────────────────────────────────────
# LIFESPAN
# ─────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("=" * 60)
    logger.info(f"  SesomNod Engine v9.0 CLV EDITION starter...")
    logger.info(f"  Ligaer:  {len(SCAN_LEAGUES)}")
    logger.info(f"  Miljø:   {cfg.ENVIRONMENT}")
    logger.info(f"  Port:    {cfg.PORT}")
    logger.info(f"  DB:      {'Satt' if cfg.DATABASE_URL else 'MANGLER'}")
    logger.info("=" * 60)

    await connect_db()
    reconnect_task = asyncio.create_task(reconnect_loop())

    scheduler = AsyncIOScheduler()

    # ── 3-VINDU SCAN ──────────────────────────────────────────────
    # Vindu 1 (Early 07:00 UTC): alle 8 ligaer — 8 API calls
    scheduler.add_job(
        fetch_all_odds,
        trigger=CronTrigger(hour=7, minute=0, timezone="UTC"),
        id="fetch_early_all",
        kwargs={"leagues": None, "window_name": "early"},
        misfire_grace_time=300,
        replace_existing=True,
    )
    # Vindu 1 analyse — 07:05 UTC
    scheduler.add_job(
        run_analysis,
        trigger=CronTrigger(hour=7, minute=5, timezone="UTC"),
        id="run_analysis_early",
        misfire_grace_time=300,
        replace_existing=True,
    )

    # Vindu 2 (Evening 18:00 UTC): topp-4 ligaer — 4 API calls
    scheduler.add_job(
        fetch_top4_odds,
        trigger=CronTrigger(hour=18, minute=0, timezone="UTC"),
        id="fetch_evening_top4",
        misfire_grace_time=300,
        replace_existing=True,
    )
    # Vindu 2 analyse — 18:05 UTC
    scheduler.add_job(
        run_analysis,
        trigger=CronTrigger(hour=18, minute=5, timezone="UTC"),
        id="run_analysis_evening",
        misfire_grace_time=300,
        replace_existing=True,
    )

    # Vindu 3 (Pre-kickoff 20:00 UTC): analyse fra eksisterende cache — 0 API calls
    scheduler.add_job(
        run_analysis,
        trigger=CronTrigger(hour=20, minute=0, timezone="UTC"),
        id="run_analysis_prekickoff",
        misfire_grace_time=300,
        replace_existing=True,
    )
    # ──────────────────────────────────────────────────────────────

    # Post 09:00 UTC
    scheduler.add_job(
        post_dagens_kamp_telegram,
        trigger=CronTrigger(hour=9, minute=0, timezone="UTC"),
        id="post_dagens_kamp",
        misfire_grace_time=300,
        replace_existing=True,
    )

    # CLV tracking hvert 30. minutt
    scheduler.add_job(
        track_clv,
        trigger=CronTrigger(minute="*/30", timezone="UTC"),
        id="track_clv",
        misfire_grace_time=120,
        replace_existing=True,
    )

    # Ukentlig CLV-rapport mandag 08:00 UTC
    scheduler.add_job(
        post_clv_rapport_telegram,
        trigger=CronTrigger(day_of_week="mon", hour=8, minute=0, timezone="UTC"),
        id="clv_rapport",
        misfire_grace_time=600,
        replace_existing=True,
    )

    # Live results hvert 15. minutt 24/7
    scheduler.add_job(
        _check_live_results,
        "interval",
        minutes=15,
        id="live_results",
        max_instances=1,
        coalesce=True,
    )

    scheduler.start()
    logger.info(
        "[Scheduler] Startet — "
        "3-vindu: Early 07:00 (8L) | Evening 18:00 (4L) | Pre-KO 20:00 (cache) | "
        "Post: 09:00 | CLV: 30min | CLV-rapport: man 08:00 UTC"
    )

    if db_state.connected:
        logger.info("[APP] SesomNod Engine KLAR! (FULL MODE)")
        try:
            async with db_state.pool.acquire() as conn:
                await conn.execute("""
                    ALTER TABLE picks ADD COLUMN IF NOT EXISTS result       VARCHAR(10) DEFAULT 'PENDING';
                    ALTER TABLE picks ADD COLUMN IF NOT EXISTS closing_odds FLOAT;
                    ALTER TABLE picks ADD COLUMN IF NOT EXISTS clv          FLOAT;
                    ALTER TABLE picks ADD COLUMN IF NOT EXISTS league       VARCHAR(60);
                """)
            await conn.execute("""
                    ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS home_score   INTEGER;
                    ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS away_score   INTEGER;
                    ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS updated_at   TIMESTAMPTZ;
                    ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS pinnacle_h2h TEXT;
                """)
            logger.info("[Blokk2] picks-kolonner migrert (result, closing_odds, clv, league)")
        except Exception as _e:
            logger.warning(f"[Blokk2] Migrasjon feilet (ikke kritisk): {_e}")

        # ml_models table for XGBoost persistence
        try:
            async with db_state.pool.acquire() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS ml_models (
                        model_name VARCHAR(100) PRIMARY KEY,
                        model_data BYTEA NOT NULL,
                        accuracy FLOAT,
                        training_samples INTEGER,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
            logger.info("[ML] ml_models table ready.")
        except Exception as _e:
            logger.warning(f"[ML] ml_models migration failed: {_e}")

        # Backtest tables
        try:
            async with db_state.pool.acquire() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS backtest_results (
                        id SERIAL PRIMARY KEY,
                        run_at TIMESTAMP DEFAULT NOW(),
                        league VARCHAR(100),
                        season VARCHAR(20),
                        total_matches INTEGER,
                        qualified_picks INTEGER,
                        hit_rate FLOAT,
                        roi_pct FLOAT,
                        avg_clv FLOAT,
                        avg_brier FLOAT,
                        max_drawdown_pct FLOAT,
                        total_profit_units FLOAT,
                        parameters JSONB
                    )
                """)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS backtest_picks (
                        id SERIAL PRIMARY KEY,
                        backtest_run_id INTEGER REFERENCES backtest_results(id),
                        match_date DATE,
                        home_team VARCHAR(100),
                        away_team VARCHAR(100),
                        league VARCHAR(100),
                        predicted_outcome VARCHAR(20),
                        actual_outcome VARCHAR(20),
                        predicted_prob FLOAT,
                        closing_odds FLOAT,
                        clv FLOAT,
                        brier_contribution FLOAT,
                        profit_units FLOAT,
                        cumulative_profit FLOAT,
                        was_correct BOOLEAN
                    )
                """)
            logger.info("[Backtest] Tables ready.")
        except Exception as _e:
            logger.warning(f"[Backtest] Migration failed: {_e}")

        # Load XGBoost model in background (does not block startup)
        async def _load_xgb():
            try:
                await asyncio.sleep(3)  # let DB pool stabilize
                from services.xgboost_model import ensure_model_loaded
                await ensure_model_loaded(db_state.pool)
            except Exception as e:
                logger.warning(f"[XGB] Startup load failed (non-critical): {e}")
        asyncio.create_task(_load_xgb())

        # Run backtest in background
        async def _run_backtest():
            try:
                await asyncio.sleep(10)  # wait for pool + XGB
                from services.football_data_fetcher import get_historical_data
                from services.backtest_engine import run_backtest
                from services.metrics import update_backtest_metrics
                df = await asyncio.to_thread(get_historical_data)
                summary = await asyncio.to_thread(run_backtest, df, "Top 5 Leagues")
                update_backtest_metrics(summary)
                async with db_state.pool.acquire() as conn:
                    run_id = await conn.fetchval("""
                        INSERT INTO backtest_results (
                            league, season, total_matches, qualified_picks,
                            hit_rate, roi_pct, avg_clv, avg_brier,
                            max_drawdown_pct, total_profit_units, parameters
                        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                        RETURNING id
                    """,
                        "Top 5 Leagues", "2023-2025",
                        summary.total_matches_scanned,
                        summary.qualified_picks,
                        summary.hit_rate,
                        summary.roi_pct,
                        summary.avg_clv,
                        summary.avg_brier,
                        summary.max_drawdown_pct,
                        summary.total_profit_units,
                        '{"edge_threshold":0.06,"half_kelly":true,"cap":0.10}',
                    )
                    if summary.picks:
                        await conn.executemany("""
                            INSERT INTO backtest_picks (
                                backtest_run_id, match_date, home_team, away_team,
                                league, predicted_outcome, actual_outcome,
                                predicted_prob, closing_odds, clv,
                                brier_contribution, profit_units,
                                cumulative_profit, was_correct
                            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
                        """, [
                            (run_id, p.match_date, p.home_team, p.away_team,
                             p.league, p.predicted_outcome, p.actual_outcome,
                             p.predicted_prob, p.closing_odds, p.clv,
                             p.brier_contribution, p.profit_units,
                             p.cumulative_profit, p.was_correct)
                            for p in summary.picks
                        ])
                logger.info(
                    "[Backtest] Done: %d picks | hit=%.1f%% | ROI=%.1f%% | CLV=%.1f%%",
                    summary.qualified_picks, summary.hit_rate * 100,
                    summary.roi_pct, summary.avg_clv,
                )
            except Exception as e:
                logger.error("[Backtest] Startup task failed: %s", e, exc_info=True)
        asyncio.create_task(_run_backtest())
    else:
        logger.info("[APP] SesomNod Engine KLAR! (OFFLINE MODE)")

    yield

    scheduler.shutdown(wait=False)
    logger.info("[Scheduler] Stoppet")
    reconnect_task.cancel()
    try:
        await reconnect_task
    except asyncio.CancelledError:
        pass
    if db_state.pool:
        await db_state.pool.close()
    logger.info("[APP] SesomNod Engine avslutter.")


# ─────────────────────────────────────────────────────────
# APP
# ─────────────────────────────────────────────────────────
app = FastAPI(
    title="SesomNod Engine API",
    version="10.0.1",
    lifespan=lifespan,
)

try:
    from prometheus_fastapi_instrumentator import Instrumentator
    Instrumentator().instrument(app).expose(app, endpoint="/metrics", include_in_schema=False)
    logger.info("[Metrics] Prometheus /metrics endpoint mounted.")
except ImportError:
    logger.warning("[Metrics] prometheus-fastapi-instrumentator not available.")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    try:
        return JSONResponse(status_code=200, content={
            "status": "online",
            "service": cfg.SERVICE_NAME,
            "version": "10.2.0-btts",
            "db": db_state.to_dict(),
            "env": cfg.ENVIRONMENT,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
    except Exception:
        return JSONResponse(status_code=200, content={"status": "ok"})


@app.get("/")
async def root():
    return {
        "service": cfg.SERVICE_NAME,
        "version": "10.2.0-btts",
        "status": "online",
        "db_connected": db_state.connected,
        "leagues": len(SCAN_LEAGUES),
        "endpoints": ["/health", "/picks", "/bankroll", "/dagens-kamp",
                      "/scan-alle-kamper", "/clv", "/docs"],
    }


@app.get("/bankroll")
async def get_bankroll():
    if not db_state.connected or not db_state.pool:
        return JSONResponse(status_code=503, content={"status": "offline", "data": [], "error": "Database ikke tilgjengelig"})
    try:
        async with db_state.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM bankroll ORDER BY timestamp DESC LIMIT 100")
        return {"status": "ok", "data": [dict(r) for r in rows], "count": len(rows)}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e)[:200]})


@app.get("/backtest/latest")
async def get_backtest_latest():
    """Latest backtest results with Phase 1 gate evaluation."""
    if not db_state.connected or not db_state.pool:
        return JSONResponse(status_code=503, content={"error": "DB offline"})
    try:
        async with db_state.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM backtest_results ORDER BY run_at DESC LIMIT 1")
        if not row:
            return {"error": "No backtest results yet", "status": 404}
        r = dict(row)
        r["run_at"] = r["run_at"].isoformat() if r.get("run_at") else None
        hit_ok = (r.get("hit_rate") or 0) > 0.55
        clv_ok = (r.get("avg_clv") or 0) > 2.0
        brier_ok = (r.get("avg_brier") or 1) < 0.25
        dd_ok = (r.get("max_drawdown_pct") or 100) < 20.0
        r["phase1_gate"] = {
            "hit_rate_ok": hit_ok, "clv_ok": clv_ok,
            "brier_ok": brier_ok, "drawdown_ok": dd_ok,
            "gate_passed": all([hit_ok, clv_ok, brier_ok, dd_ok]),
        }
        return r
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)[:200]})


@app.get("/backtest/picks")
async def get_backtest_picks(limit: int = 50, offset: int = 0):
    """Paginated backtest pick history."""
    if not db_state.connected or not db_state.pool:
        return JSONResponse(status_code=503, content={"error": "DB offline"})
    try:
        async with db_state.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM backtest_picks ORDER BY match_date DESC LIMIT $1 OFFSET $2",
                limit, offset,
            )
            total = await conn.fetchval("SELECT COUNT(*) FROM backtest_picks")
        return {"total": total, "limit": limit, "offset": offset, "picks": [dict(r) for r in rows]}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)[:200]})


@app.get("/dashboard/stats")
async def get_dashboard_stats():
    """Public dashboard: live Phase 0 + backtest + Phase 1 gate status."""
    if not db_state.connected or not db_state.pool:
        return JSONResponse(status_code=503, content={"error": "DB offline"})
    live_data = {"phase0_picks": 0, "hit_rate": 0.0, "avg_clv": 0.0, "profit_units": 0.0}
    backtest_data = {"hit_rate": 0.0, "roi_pct": 0.0, "avg_clv": 0.0, "qualified_picks": 0}
    try:
        async with db_state.pool.acquire() as conn:
            live_row = await conn.fetchrow("""
                SELECT
                    COUNT(*) FILTER (WHERE result IN ('WIN','LOSS')) AS settled,
                    COUNT(*) FILTER (WHERE result = 'WIN') AS wins,
                    COALESCE(AVG(clv), 0) AS avg_clv
                FROM dagens_kamp
                WHERE tier IN ('ATOMIC','EDGE') AND result IS NOT NULL
            """)
            if live_row and live_row["settled"] and live_row["settled"] > 0:
                s, w = live_row["settled"], live_row["wins"] or 0
                live_data = {
                    "phase0_picks": int(s),
                    "hit_rate": round(w / s, 4) if s > 0 else 0.0,
                    "avg_clv": round(float(live_row["avg_clv"]), 2),
                    "profit_units": 0.0,
                }
            bt_row = await conn.fetchrow("""
                SELECT hit_rate, roi_pct, avg_clv, qualified_picks, avg_brier, max_drawdown_pct
                FROM backtest_results ORDER BY run_at DESC LIMIT 1
            """)
            if bt_row:
                backtest_data = {
                    "hit_rate": round(bt_row["hit_rate"] or 0, 4),
                    "roi_pct": round(bt_row["roi_pct"] or 0, 2),
                    "avg_clv": round(bt_row["avg_clv"] or 0, 2),
                    "qualified_picks": bt_row["qualified_picks"] or 0,
                    "avg_brier": round(bt_row["avg_brier"] or 0, 4),
                }
        hit_ok = live_data["hit_rate"] > 0.55
        clv_ok = live_data["avg_clv"] > 2.0
        return {
            "live": live_data, "backtest": backtest_data,
            "phase1_gate": {"hit_rate_ok": hit_ok, "clv_ok": clv_ok, "gate_passed": all([hit_ok, clv_ok])},
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)[:200]})



def enrich_pick(pick: dict) -> dict:
    import math, json as _json
    xg_home = max(0.0, float(
        pick.get("signal_xg_home") or pick.get("xg_home") or pick.get("xg_divergence_home") or 0
    ))
    xg_away = max(0.0, float(
        pick.get("signal_xg_away") or pick.get("xg_away") or pick.get("xg_divergence_away") or 0
    ))

    # When real xG is missing, derive from actual Pinnacle h/d/a odds stored per match
    if xg_home == 0 or xg_away == 0:
        try:
            h2h_raw = pick.get("pinnacle_h2h")
            h_odds = d_odds = a_odds = 0.0
            if h2h_raw:
                parsed = _json.loads(h2h_raw) if isinstance(h2h_raw, str) else h2h_raw
                h_odds = float(parsed.get("home") or 0)
                d_odds = float(parsed.get("draw") or 0)
                a_odds = float(parsed.get("away") or 0)

            if h_odds > 1.0 and a_odds > 1.0 and d_odds > 1.0:
                # Remove vig → fair probabilities
                total_implied = (1/h_odds) + (1/d_odds) + (1/a_odds)
                fair_home = (1/h_odds) / total_implied
                fair_away = (1/a_odds) / total_implied
                # Scale to goal expectation (avg ~2.7 goals per match)
                xg_home = round(fair_home * 2.7, 2)
                xg_away = round(fair_away * 2.7, 2)
            else:
                # Last resort: use pick odds + edge to infer one-sided strength
                pick_odds = float(pick.get("odds") or 2.5)
                edge_val  = float(pick.get("edge") or 0)
                implied   = min(0.85, (1.0 / max(1.01, pick_odds)) + edge_val)
                xg_home   = round(implied * 2.7, 2)
                xg_away   = round(max(0.5, (1.0 - implied - 0.25) * 2.7), 2)
        except Exception:
            xg_home = 1.3
            xg_away = 1.1
    lam = max(0.5, min(6.0, xg_home + xg_away))
    def pcdf(n, l):
        t, term = 0.0, math.exp(-l)
        for k in range(n + 1):
            t += term
            term *= l / (k + 1)
        return t
    def pover(n): return round((1 - pcdf(n, lam)) * 100)
    def pbtts():
        ph = 1 - math.exp(-max(0.01, xg_home))
        pa = 1 - math.exp(-max(0.01, xg_away))
        return round(max(0, min(99, (ph * pa - ph * pa * 0.08 * (xg_home * xg_away * 0.13)) * 100)))
    atomic = int(pick.get("atomic_score") or 4)
    soft = float(pick.get("soft_edge") or pick.get("edge") or 0)
    raw = (min(10,max(0,5+(xg_home-xg_away)*2.5))*2.3 + 5*1.8 +
           min(10,max(0,atomic*1.1))*1.5 + 5*1.2 + 5*0.9 + 9*0.7 +
           min(10,max(0,soft*0.8))*3.1)
    omega = round(min(100, max(0, (raw / 115.0) * 100)))
    db_tier = pick.get("omega_tier")  # from DB (aliased as omega_tier in SELECT)
    omega_tier = ("BRUTAL" if omega>=72 else "STRONG" if omega>=55 else "MONITORED" if omega>=40 else "SKIP")
    tier = db_tier if db_tier in ("ATOMIC", "EDGE", "MONITORED") else omega_tier
    hw = round(max(5, min(85, (xg_home/lam)*70)))
    aw = round(max(5, min(85, (xg_away/lam)*60)))
    btts = pbtts()
    if not pick.get("home_team") or not pick.get("away_team"):
        parts = str(pick.get("match_name") or "Hjemme vs Borte").split(" vs ")
        pick["home_team"] = parts[0].strip() if parts else "Hjemmelag"
        pick["away_team"] = parts[1].strip() if len(parts) > 1 else "Bortelag"
    smart = []
    if soft >= 6.0:
        smart.append({"market": str(pick.get("market_type") or "Pick"),
            "selection": str(pick.get("market_type") or "Pick"),
            "our_prob": round(50+soft), "market_implied_prob": 50,
            "value_gap_percent": round(soft,1),
            "unibet_odds": float(pick.get("our_odds") or 2.0),
            "edge_label": "Sharp Edge" if soft>=15 else "Value Edge"})
    market_label = str(pick.get("market_hint") or pick.get("market_type") or "Pick")
    our_odds_val  = float(pick.get("odds") or 2.0)
    ev_val        = float(pick.get("ev") or soft or 0)
    pick.update({"omega_score":omega,"omega_tier":tier,"tier":tier,
        "ev": round(ev_val, 2),
        "xg_home":round(xg_home,1),"xg_away":round(xg_away,1),"lambda":round(lam,1),
        "btts_yes":btts,"btts_no":100-btts,"btts_is_smart_bet":btts>=60,"btts_value_gap":0.0,
        "over_05":pover(0),"over_15":pover(1),"over_25":pover(2),
        "over_35":pover(3),"over_45":pover(4),"under_25":100-pover(2),
        "home_win_prob":hw,"draw_prob":max(5,100-hw-aw),"away_win_prob":aw,
        "home_odds":round(100/max(1,hw),2),"draw_odds":round(100/max(1,100-hw-aw),2),"away_odds":round(100/max(1,aw),2),
        "first_goal_home":max(0,round(xg_home/lam*75)),"first_goal_away":max(0,round(xg_away/lam*65)),
        "first_goal_none_ht":15,
        "form_home":list(pick.get("form_home") or ["W","D","W","D","W"]),
        "form_away":list(pick.get("form_away") or ["W","D","W","D","W"]),
        "smart_bets":smart,"is_completed":bool(pick.get("is_completed") or False),
        "kickoff_cet":str(pick.get("kickoff_time") or pick.get("match_date") or pick.get("kickoff_cet") or "18:45"),
        "our_pick":market_label+" @ "+str(round(our_odds_val,2))})
    return pick

async def enrich_picks_with_dc(picks: list[dict]) -> list[dict]:
    """
    Enrich picks with Dixon-Coles probabilities and Kelly stake recommendations.
    Runs after initial enrich_pick() processing. Never raises.
    """
    try:
        from services.dixon_coles_engine import get_dixon_coles_probs
        from services.kelly_calculator import calculate_kelly
    except ImportError as e:
        logger.warning("[DC] Failed to import Dixon-Coles/Kelly services: %s", e)
        for pick in picks:
            pick["dixon_coles"] = None
            pick["kelly"] = None
        return picks

    for pick in picks:
        try:
            home = pick.get("home_team", "")
            away = pick.get("away_team", "")
            odds = float(pick.get("odds") or 2.0)

            # Market home probability from existing Poisson-based calc
            hw_pct = float(pick.get("home_win_prob") or 40)
            market_home_prob = hw_pct / 100.0

            dc_result = await get_dixon_coles_probs(home, away, market_home_prob)
            pick["dixon_coles"] = dc_result.to_dict()

            # Determine which probability to use for Kelly based on the pick type
            pick_label = str(pick.get("our_pick") or pick.get("market_hint") or "").lower()
            if "away" in pick_label or "borte" in pick_label:
                model_prob = dc_result.away_win_prob
            elif "draw" in pick_label or "uavgjort" in pick_label:
                model_prob = dc_result.draw_prob
            else:
                model_prob = dc_result.home_win_prob

            kelly_result = calculate_kelly(
                model_prob=model_prob,
                decimal_odds=odds,
                model_verified=not dc_result.fallback_used,
            )
            pick["kelly"] = kelly_result.to_dict()

        except Exception as e:
            logger.warning("[DC] Failed for %s vs %s: %s", pick.get("home_team"), pick.get("away_team"), e)
            pick["dixon_coles"] = None
            pick["kelly"] = None

        # XGBoost prediction
        try:
            from services.pick_feature_extractor import extract_features_for_pick
            from services.xgboost_model import predict_match
            features = extract_features_for_pick(
                home_team=pick.get("home_team", ""),
                away_team=pick.get("away_team", ""),
            )
            xgb_result = predict_match(**features)
            pick["xgboost"] = xgb_result.to_dict()
        except Exception as xgb_err:
            logger.warning("[XGB] Failed for %s vs %s: %s", pick.get("home_team"), pick.get("away_team"), xgb_err)
            pick["xgboost"] = None

    return picks


_live_cache: dict = {"data": {}, "fetched_at": 0.0}

async def fetch_live_scores() -> dict:
    """Henter live scores fra football-data.org. Caches i 60s."""
    now = time.time()
    if now - _live_cache["fetched_at"] < 60:
        return _live_cache["data"]
    fd_key = os.environ.get("FOOTBALL_DATA_API_KEY", "")
    if not fd_key:
        return {}
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.get(
                "https://api.football-data.org/v4/matches",
                headers={"X-Auth-Token": fd_key},
                params={"status": "LIVE,IN_PLAY,PAUSED,HALFTIME"},
            )
        if r.status_code != 200:
            return {}
        scores: dict = {}
        for m in r.json().get("matches", []):
            home = m["homeTeam"]["name"]
            away = m["awayTeam"]["name"]
            sc = m.get("score", {})
            ft = sc.get("fullTime", {}) or {}
            ht = sc.get("halfTime", {}) or {}
            h_goals = ft.get("home") if ft.get("home") is not None else ht.get("home") or 0
            a_goals = ft.get("away") if ft.get("away") is not None else ht.get("away") or 0
            minute = m.get("minute") or ""
            scores[f"{home} vs {away}"] = {
                "score": f"{h_goals}-{a_goals}",
                "minute": minute,
                "status": m.get("status", ""),
            }
            # Also try short name variants
            scores[f"{m['homeTeam'].get('shortName', home)} vs {m['awayTeam'].get('shortName', away)}"] = scores[f"{home} vs {away}"]
        _live_cache["data"] = scores
        _live_cache["fetched_at"] = now
        logger.info(f"[live-scores] {len(scores)} live kamper hentet")
        return scores
    except Exception as e:
        logger.warning(f"[live-scores] feil: {e}")
        return {}


@app.get("/picks")
async def get_picks():
    if not db_state.connected or not db_state.pool:
        return JSONResponse(status_code=503, content={"status": "offline", "data": [], "error": "Database ikke tilgjengelig"})
    try:
        async with db_state.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    id,
                    match                 AS match_name,
                    home_team,
                    away_team,
                    odds,
                    edge,
                    ev,
                    atomic_score,
                    tier                  AS omega_tier,
                    market_hint,
                    league,
                    kickoff               AS kickoff_time,
                    timestamp,
                    confidence,
                    signal_xg,
                    xg_divergence_home,
                    xg_divergence_away,
                    result,
                    closing_odds,
                    clv,
                    pinnacle_h2h
                FROM dagens_kamp
                WHERE kickoff > NOW() - INTERVAL '1 hour'
                  AND kickoff <= NOW() + INTERVAL '36 hours'
                ORDER BY kickoff ASC
                LIMIT 100
                """
            )
        enriched = [enrich_pick(dict(r)) for r in rows]
        enriched = await enrich_picks_with_dc(enriched)
        live_scores = await fetch_live_scores()
        for pick in enriched:
            match_key = pick.get("match_name", "")
            live = live_scores.get(match_key, {})
            pick["live_score"] = live.get("score") if live else None
            pick["minute"] = live.get("minute") if live else None
            pick["is_live"] = bool(live)
        return {"status": "ok", "data": enriched, "count": len(enriched)}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e)[:200]})


@app.get("/picks/analysis/{match_id}")
async def get_pick_analysis(match_id: int):
    """Detailed Dixon-Coles + Kelly analysis for a specific match."""
    if not db_state.connected or not db_state.pool:
        return JSONResponse(status_code=503, content={"status": "offline", "error": "Database ikke tilgjengelig"})
    try:
        async with db_state.pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT id, match AS match_name, home_team, away_team, odds, edge, ev,
                          atomic_score, tier AS omega_tier, market_hint, league,
                          kickoff AS kickoff_time, confidence, signal_xg,
                          xg_divergence_home, xg_divergence_away, pinnacle_h2h
                   FROM dagens_kamp WHERE id = $1""",
                match_id,
            )
        if not row:
            return JSONResponse(status_code=404, content={"status": "error", "error": f"Match id={match_id} not found"})

        pick = enrich_pick(dict(row))

        try:
            from services.dixon_coles_engine import get_dixon_coles_probs
            from services.kelly_calculator import calculate_kelly

            hw_pct = float(pick.get("home_win_prob") or 40)
            market_home_prob = hw_pct / 100.0
            dc_result = await get_dixon_coles_probs(pick["home_team"], pick["away_team"], market_home_prob)
            odds = float(pick.get("odds") or 2.0)
            pick_label = str(pick.get("our_pick") or "").lower()
            if "away" in pick_label or "borte" in pick_label:
                model_prob = dc_result.away_win_prob
            elif "draw" in pick_label or "uavgjort" in pick_label:
                model_prob = dc_result.draw_prob
            else:
                model_prob = dc_result.home_win_prob
            kelly_result = calculate_kelly(
                model_prob=model_prob,
                decimal_odds=odds,
                model_verified=not dc_result.fallback_used,
            )

            return {
                "status": "ok",
                "match_id": match_id,
                "home_team": pick["home_team"],
                "away_team": pick["away_team"],
                "odds": odds,
                "dixon_coles": dc_result.to_dict(),
                "kelly": kelly_result.to_dict(),
            }
        except ImportError as e:
            return JSONResponse(status_code=500, content={"status": "error", "error": f"Dixon-Coles services not available: {e}"})

    except Exception as e:
        logger.exception("[/picks/analysis] Error for match %s", match_id)
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e)[:200]})


@app.get("/dagens-kamp")
async def get_dagens_kamp():
    if not db_state.connected or not db_state.pool:
        return JSONResponse(status_code=503, content={"status": "offline", "data": [], "error": "Database ikke tilgjengelig"})
    try:
        async with db_state.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM dagens_kamp ORDER BY timestamp DESC LIMIT 50"
            )
        return {"status": "ok", "data": [dict(r) for r in rows], "count": len(rows)}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e)[:200]})


@app.get("/clv")
async def get_clv():
    """Henter siste CLV-records med statistikk."""
    if not db_state.connected or not db_state.pool:
        return JSONResponse(status_code=503, content={"status": "offline", "error": "Database ikke tilgjengelig"})
    try:
        async with db_state.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM clv_records ORDER BY tracked_at DESC LIMIT 100"
            )
            stats = await conn.fetchrow("""
                SELECT
                    COUNT(*) as total,
                    ROUND(AVG(clv_pct)::numeric, 2) as avg_clv,
                    COUNT(CASE WHEN clv_pct > 0 THEN 1 END) as positive_clv
                FROM clv_records
                WHERE tracked_at >= NOW() - INTERVAL '30 days'
            """)
        return {
            "status": "ok",
            "stats_30d": dict(stats) if stats else {},
            "records": [dict(r) for r in rows],
            "count": len(rows),
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e)[:200]})


@app.get("/scan-alle-kamper")
async def scan_alle_kamper():
    """Trigger manuell fetch + analyse og returner resultater."""
    if not cfg.ODDS_API_KEY:
        return JSONResponse(status_code=503, content={"status": "error", "error": "ODDS_API_KEY mangler"})
    try:
        # Hent fersk data
        await fetch_all_odds()

        if not db_state.connected or not db_state.pool:
            return JSONResponse(status_code=503, content={"status": "error", "error": "DB offline"})

        now = datetime.now(timezone.utc)
        candidates = []
        total_scanned = 0

        async with db_state.pool.acquire() as conn:
            for league in SCAN_LEAGUES:
                row = await conn.fetchrow("""
                    SELECT data FROM odds_snapshots
                    WHERE league_key = $1
                    ORDER BY snapshot_time DESC LIMIT 1
                """, league["key"])
                if not row:
                    continue
                matches = json.loads(row["data"])
                total_scanned += len(matches)
                picks = await _analyse_snapshot(league, matches, now, conn=conn)
                candidates.extend(picks)

        candidates.sort(key=lambda x: x["score"], reverse=True)

        return {
            "status": "ok",
            "scanned_leagues": len(SCAN_LEAGUES),
            "total_matches_scanned": total_scanned,
            "filters": {
                "ev_min_pct": EV_MIN,
                "pinnacle_edge_min_pct": PINNACLE_EDGE_MIN,
                "odds_range": [ODDS_MIN, ODDS_MAX],
                "max_hours": MATCH_HOURS_MAX,
            },
            "qualified_picks": len(candidates),
            "best_pick": candidates[0] if candidates else None,
            "all_picks": candidates[:20],
            "timestamp": now.isoformat(),
        }
    except Exception as e:
        logger.exception(f"[/scan-alle-kamper] Feil: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e)[:200]})


@app.post("/fetch-odds")
async def trigger_fetch_odds():
    """Manuell trigger for odds-fetching — returnerer faktiske resultater."""
    if not cfg.ODDS_API_KEY:
        return JSONResponse(status_code=503, content={"status": "error", "error": "ODDS_API_KEY mangler"})
    if not db_state.connected or not db_state.pool:
        return JSONResponse(status_code=503, content={"status": "error", "error": "DB offline"})

    snap_time = datetime.now(timezone.utc)
    results = []

    async with httpx.AsyncClient(timeout=30) as client:
        for league in SCAN_LEAGUES:
            try:
                resp = await client.get(
                    f"https://api.the-odds-api.com/v4/sports/{league['key']}/odds/",
                    params={
                        "apiKey": cfg.ODDS_API_KEY,
                        "regions": "eu",
                        "markets": "totals,spreads,h2h",
                        "oddsFormat": "decimal",
                        "bookmakers": "pinnacle,bet365,betway,unibet,williamhill,bwin,nordicbet,betsson,betfair_ex_eu,sport888",
                    }
                )
                remaining = int(resp.headers.get("x-requests-remaining", -1))
                used = int(resp.headers.get("x-requests-used", -1))

                if resp.status_code != 200:
                    results.append({"league": league["name"], "status": f"HTTP {resp.status_code}", "matches": 0})
                    continue

                data = resp.json()
                if not isinstance(data, list):
                    results.append({"league": league["name"], "status": "bad_data", "matches": 0})
                    continue

                async with db_state.pool.acquire() as conn:
                    await conn.execute("""
                        INSERT INTO odds_snapshots (league_key, snapshot_time, data)
                        VALUES ($1, $2, $3)
                    """, league["key"], snap_time, json.dumps(data))

                results.append({
                    "league": league["name"],
                    "status": "ok",
                    "matches": len(data),
                    "api_remaining": remaining,
                    "api_used": used,
                })
                logger.info(f"[OddsCache] {league['name']}: {len(data)} kamper — {remaining} req igjen")

            except Exception as e:
                results.append({"league": league["name"], "status": f"error: {str(e)[:80]}", "matches": 0})

    total_matches = sum(r["matches"] for r in results)
    api_remaining = next((r["api_remaining"] for r in results if r.get("api_remaining", -1) >= 0), None)
    api_used = next((r["api_used"] for r in results if r.get("api_used", -1) >= 0), None)

    return {
        "status": "ok",
        "snapshot_time": snap_time.isoformat(),
        "leagues_fetched": sum(1 for r in results if r["status"] == "ok"),
        "total_matches": total_matches,
        "api_remaining": api_remaining,
        "api_used": api_used,
        "details": results,
    }


@app.post("/update-pinnacle")
async def update_pinnacle_h2h():
    """Fetch Pinnacle H2H odds and write to dagens_kamp.pinnacle_h2h for today's picks."""
    if not cfg.ODDS_API_KEY:
        return JSONResponse(status_code=503, content={"status": "error", "error": "ODDS_API_KEY mangler"})
    if not db_state.connected or not db_state.pool:
        return JSONResponse(status_code=503, content={"status": "error", "error": "DB offline"})

    all_matches = []
    async with httpx.AsyncClient(timeout=30) as client:
        for league in SCAN_LEAGUES:
            try:
                resp = await client.get(
                    f"https://api.the-odds-api.com/v4/sports/{league['key']}/odds/",
                    params={
                        "apiKey": cfg.ODDS_API_KEY,
                        "regions": "eu",
                        "bookmakers": "pinnacle",
                        "markets": "h2h",
                        "oddsFormat": "decimal",
                    }
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, list):
                        all_matches.extend(data)
            except Exception as e:
                logger.warning(f"[Pinnacle] Skip {league['name']}: {e}")

    logger.info(f"[Pinnacle] Fetched {len(all_matches)} matches from Odds API")

    updated = []
    not_found = []
    async with db_state.pool.acquire() as conn:
        picks = await conn.fetch(
            "SELECT id, home_team, away_team FROM dagens_kamp WHERE kickoff::date = CURRENT_DATE"
        )
        for pick in picks:
            ht = pick["home_team"].lower().strip()
            at = pick["away_team"].lower().strip()
            matched = None
            for m in all_matches:
                mh = m.get("home_team", "").lower().strip()
                ma = m.get("away_team", "").lower().strip()
                if (ht in mh or mh in ht) and (at in ma or ma in at):
                    matched = m
                    break
            if not matched:
                not_found.append(f"{pick['home_team']} vs {pick['away_team']}")
                continue
            for bm in matched.get("bookmakers", []):
                if bm["key"] == "pinnacle":
                    outcomes = bm["markets"][0]["outcomes"]
                    odds_dict = {}
                    for o in outcomes:
                        n = o["name"].lower()
                        if n == matched["home_team"].lower():
                            odds_dict["home"] = o["price"]
                        elif n == matched["away_team"].lower():
                            odds_dict["away"] = o["price"]
                        else:
                            odds_dict["draw"] = o["price"]
                    await conn.execute(
                        "UPDATE dagens_kamp SET pinnacle_h2h=$1 WHERE id=$2",
                        json.dumps(odds_dict), pick["id"]
                    )
                    updated.append({"match": f"{pick['home_team']} vs {pick['away_team']}", "odds": odds_dict})
                    break

    return {"status": "ok", "updated": len(updated), "not_found": len(not_found), "details": updated, "missing": not_found}


@app.post("/run-analysis")
async def trigger_run_analysis():
    """Manuell trigger for analyse — returnerer kvalifiserte picks med SCORE-rangering."""
    if not db_state.connected or not db_state.pool:
        return JSONResponse(status_code=503, content={"status": "error", "error": "DB offline"})

    now = datetime.now(timezone.utc)
    candidates = []
    total_scanned = 0

    async with db_state.pool.acquire() as conn:
        for league in SCAN_LEAGUES:
            try:
                row = await conn.fetchrow("""
                    SELECT data, snapshot_time FROM odds_snapshots
                    WHERE league_key = $1
                    ORDER BY snapshot_time DESC LIMIT 1
                """, league["key"])
                if not row:
                    continue
                matches = json.loads(row["data"])
                total_scanned += len(matches)
                picks = await _analyse_snapshot(league, matches, now, conn=conn)
                candidates.extend(picks)
            except Exception as e:
                logger.warning(f"[API/run-analysis] {league['key']}: {e}")

    candidates.sort(key=lambda x: x["score"], reverse=True)

    formatted = []
    for i, p in enumerate(candidates, 1):
        ev = p["ev"]
        conf_label = "HIGH" if ev >= 12 else ("MEDIUM" if ev >= 9 else "LOW")
        formatted.append({
            "rank": i,
            "match": f"{p['home_team']} vs {p['away_team']}",
            "league": f"{p['league_flag']} {p['league']}",
            "market": p["market_type"],
            "pick": p["pick"],
            "odds": p["odds"],
            "ev_pct": p["ev"],
            "edge_pct": p["edge"],
            "score": p["score"],
            "confidence": conf_label,
            "bookmakers": p["num_bookmakers"],
            "hours_to_kickoff": p["hours_to_kickoff"],
            "pinnacle_opening": p.get("pinnacle_opening"),
        })

    return {
        "status": "ok",
        "timestamp": now.isoformat(),
        "total_matches_scanned": total_scanned,
        "qualified_picks": len(formatted),
        "picks": formatted,
    }


@app.post("/test-telegram")
async def test_telegram():
    """Sender en diagnostisk testmelding til Telegram. Bekrefter at bot-token og chat_id fungerer."""
    if not cfg.TELEGRAM_TOKEN or not cfg.TELEGRAM_CHAT_ID:
        return JSONResponse(status_code=503, content={"status": "error", "error": "TELEGRAM ikke konfigurert"})
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    message = (
        "SesomNod diagnostics — bot alive\n"
        f"Datetime: {now_str}\n"
        "Telegram pipeline: ACTIVE\n"
        "Status: All systems operational"
    )
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"https://api.telegram.org/bot{cfg.TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": cfg.TELEGRAM_CHAT_ID, "text": message},
            )
        return {
            "status": "sent" if resp.status_code == 200 else "failed",
            "telegram_http": resp.status_code,
            "message": message,
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e)[:200]})


async def _log_pick_to_mirofish(pick_row: dict) -> dict:
    """
    Auto-log a pick to MiroFish for CLV tracking.
    Called after every successful Telegram post.
    NEVER raises — MiroFish failure never breaks Telegram posting.
    """
    MIROFISH_URL = "https://mirofish-service-production.up.railway.app"
    try:
        home_raw = str(pick_row.get("home_team") or "unknown")
        away_raw = str(pick_row.get("away_team") or "unknown")

        kickoff_raw = pick_row.get("kickoff")
        if hasattr(kickoff_raw, "strftime"):
            if kickoff_raw.tzinfo is None:
                kickoff_raw = kickoff_raw.replace(tzinfo=timezone.utc)
            kickoff_iso = kickoff_raw.isoformat()
            date_str = kickoff_raw.strftime("%Y%m%d")
        else:
            kickoff_iso = str(kickoff_raw)
            try:
                dt = datetime.fromisoformat(str(kickoff_raw).replace("Z", "+00:00"))
                date_str = dt.strftime("%Y%m%d")
            except Exception:
                date_str = datetime.now(timezone.utc).strftime("%Y%m%d")

        home_slug = home_raw.lower().replace(" ", "-")
        away_slug = away_raw.lower().replace(" ", "-")
        pick_id = f"{home_slug}-{away_slug}-{date_str}"

        our_odds_raw = pick_row.get("odds")
        if our_odds_raw is None or float(our_odds_raw) <= 1.0:
            return {"logged": False, "reason": "no_valid_odds_field"}
        our_odds = float(our_odds_raw)

        edge_val = float(pick_row.get("edge") or 0.0)
        match_name = pick_row.get("match") or f"{home_raw} vs {away_raw}"

        payload = {
            "pick_id": pick_id,
            "match": match_name,
            "home_team": home_raw,
            "away_team": away_raw,
            "our_odds": our_odds,
            "kickoff": kickoff_iso,
            "edge_at_pick": edge_val,
            "market": "h2h",
        }

        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(f"{MIROFISH_URL}/track", json=payload)
            if r.status_code == 200:
                logger.info(f"[MiroFish] Logged: {pick_id} | odds={our_odds}")
                return {"logged": True, "pick_id": pick_id}
            elif r.status_code == 400 and "already tracked" in r.text:
                logger.info(f"[MiroFish] Already tracked: {pick_id}")
                return {"logged": False, "reason": "already_tracked"}
            else:
                logger.warning(f"[MiroFish] HTTP {r.status_code}: {r.text[:200]}")
                return {"logged": False, "reason": f"http_{r.status_code}"}

    except Exception as e:
        logger.warning(f"[MiroFish] Exception: {e}")
        return {"logged": False, "reason": str(e)}


@app.post("/post-telegram")
async def trigger_post_telegram():
    """Poster upostede ATOMIC/EDGE picks (edge >= 6%) til Telegram, inntil daglig grense."""
    if not cfg.TELEGRAM_TOKEN or not cfg.TELEGRAM_CHAT_ID:
        return JSONResponse(status_code=503, content={"status": "error", "error": "TELEGRAM ikke konfigurert"})
    if not db_state.connected or not db_state.pool:
        return JSONResponse(status_code=503, content={"status": "error", "error": "DB offline"})

    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    async with db_state.pool.acquire() as conn:
        # Count picks already posted today (by kickoff so insert date doesn't matter)
        daily_posted = await conn.fetchval("""
            SELECT COUNT(*) FROM dagens_kamp
            WHERE telegram_posted = TRUE
              AND kickoff BETWEEN $1 AND $1 + INTERVAL '24 hours'
        """, today_start)

        if int(daily_posted) >= DAILY_POST_LIMIT:
            return {"status": "skipped", "reason": f"Daglig grense ({DAILY_POST_LIMIT}) nådd", "posted_today": int(daily_posted)}

        # Fix A+B: fetch ALL qualified picks (edge >= 6%, ATOMIC/EDGE tier only)
        rows = await conn.fetch("""
            SELECT * FROM dagens_kamp
            WHERE telegram_posted = FALSE
              AND kickoff BETWEEN NOW() - INTERVAL '3 hours' AND NOW() + INTERVAL '36 hours'
              AND edge >= 0.06
              AND tier IN ('ATOMIC', 'EDGE')
            ORDER BY score DESC NULLS LAST, ev DESC NULLS LAST
        """)

    # Fix C: zero qualified picks → explicit status with dynamic reason, no post
    if not rows:
        async with db_state.pool.acquire() as conn:
            already_posted_qualified = await conn.fetchval("""
                SELECT COUNT(*) FROM dagens_kamp
                WHERE telegram_posted = TRUE
                  AND kickoff BETWEEN NOW() - INTERVAL '3 hours' AND NOW() + INTERVAL '36 hours'
                  AND edge >= 0.06
                  AND tier IN ('ATOMIC', 'EDGE')
            """)
        if int(already_posted_qualified) > 0:
            reason = "all qualified picks already posted today"
        else:
            reason = "no picks meet quality threshold (EDGE/ATOMIC + edge >= 6%)"
        return {"status": "no_qualified_picks", "reason": reason}

    already_posted = int(daily_posted)
    remaining_slots = DAILY_POST_LIMIT - already_posted
    results = []

    for row in rows[:remaining_slots]:
        pick_data = dict(row)
        rank = already_posted + len(results) + 1
        message = _format_pick_message(pick_data, rank=rank)
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(
                    f"https://api.telegram.org/bot{cfg.TELEGRAM_TOKEN}/sendMessage",
                    json={"chat_id": cfg.TELEGRAM_CHAT_ID, "text": message},
                )
            if resp.status_code == 200:
                async with db_state.pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE dagens_kamp SET telegram_posted = TRUE WHERE id = $1",
                        pick_data["id"]
                    )
                logger.info(f"[/post-telegram] Postet pick id={pick_data['id']}: {pick_data.get('pick')}")
                mirofish_result = await _log_pick_to_mirofish(pick_data)
                results.append({
                    "status": "posted",
                    "pick_id": pick_data["id"],
                    "pick": pick_data.get("pick"),
                    "match": pick_data.get("match"),
                    "odds": float(pick_data.get("odds") or 0),
                    "ev": float(pick_data.get("ev") or 0),
                    "edge": float(pick_data.get("edge") or 0),
                    "tier": pick_data.get("tier"),
                    "telegram_status": resp.status_code,
                    "mirofish": mirofish_result,
                })
            else:
                logger.warning(f"[/post-telegram] Telegram feil {resp.status_code} for pick id={pick_data['id']}")
                results.append({"status": "error", "pick_id": pick_data["id"], "telegram_status": resp.status_code, "detail": resp.text[:200]})
        except Exception as e:
            logger.exception(f"[/post-telegram] Feil for pick id={pick_data.get('id')}: {e}")
            results.append({"status": "error", "pick_id": pick_data.get("id"), "error": str(e)[:200]})

    return {
        "status": "done",
        "posted_count": len([r for r in results if r.get("status") == "posted"]),
        "results": results,
    }


@app.post("/check-results-now")
async def check_results_now():
    """Manual trigger for _check_live_results — for testing and on-demand result checks."""
    await _check_live_results()
    return {"status": "done", "message": "check_live_results executed"}


@app.post("/log-results")
async def log_results_manual(results: list[dict]):
    async with db_state.pool.acquire() as conn:
        logged = []
        for p in results:
            hs = int(p["home_score"])
            aws = int(p["away_score"])
            pick_type = str(p.get("pick", "")).lower().strip()
            total_goals = hs + aws
            btts = hs > 0 and aws > 0

            # Compute WIN/LOSS/VOID from pick + score
            pick_won = None
            if pick_type in ("draw", "uavgjort"):
                pick_won = hs == aws
            elif pick_type in ("home", "home win", "1", "hjemme"):
                pick_won = hs > aws
            elif pick_type in ("away", "away win", "2", "borte"):
                pick_won = aws > hs
            elif "over 3.5" in pick_type:
                pick_won = total_goals > 3
            elif "over 2.5" in pick_type:
                pick_won = total_goals > 2
            elif "over 1.5" in pick_type:
                pick_won = total_goals > 1
            elif "over 0.5" in pick_type:
                pick_won = total_goals > 0
            elif "btts no" in pick_type:
                pick_won = not btts
            elif "btts" in pick_type:
                pick_won = btts
            elif "under 2.5" in pick_type:
                pick_won = total_goals < 3
            elif "under 3.5" in pick_type:
                pick_won = total_goals < 4
            outcome = "WIN" if pick_won else "LOSS" if pick_won is False else "VOID"

            row = await conn.fetchrow("""
                SELECT id FROM dagens_kamp
                WHERE home_team ILIKE $1 AND away_team ILIKE $2
            """, f"%{p['home']}%", f"%{p['away']}%")
            if row:
                await conn.execute("""
                    UPDATE dagens_kamp
                    SET result=$1, home_score=$2, away_score=$3, updated_at=NOW()
                    WHERE id=$4
                """, outcome, hs, aws, row["id"])
                logged.append(f"UPDATED: {p['home']} vs {p['away']} → {outcome}")
            else:
                await conn.execute("""
                    INSERT INTO dagens_kamp
                    (match, home_team, away_team, pick, odds, edge, tier,
                     kickoff, telegram_posted, result, home_score, away_score)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8::timestamptz,TRUE,$9,$10,$11)
                """,
                    f"{p['home']} vs {p['away']}",
                    p["home"], p["away"],
                    p.get("pick",""),
                    float(p.get("odds", 3.5)),
                    15.0, "EDGE",
                    datetime(2026, 3, 31, 20, 0, 0, tzinfo=timezone.utc),
                    outcome, hs, aws
                )
                logged.append(f"INSERTED: {p['home']} vs {p['away']} → {outcome}")
        total = await conn.fetchval("SELECT COUNT(*) FROM dagens_kamp WHERE result IS NOT NULL")
        return {"logged": logged, "total_settled": total, "phase0": f"{total}/30"}


@app.post("/send-message")
async def send_custom_message(body: dict):
    """Send a custom text message to Telegram channel."""
    if not cfg.TELEGRAM_TOKEN or not cfg.TELEGRAM_CHAT_ID:
        return JSONResponse(status_code=503, content={"error": "TELEGRAM ikke konfigurert"})
    text = body.get("text", "")
    if not text:
        return {"status": "error", "message": "No text provided"}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"https://api.telegram.org/bot{cfg.TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": cfg.TELEGRAM_CHAT_ID, "text": text},
            )
        return {"status": "sent" if resp.status_code == 200 else "failed",
                "telegram_http": resp.status_code}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)[:200]})


@app.post("/force-telegram")
async def force_telegram():
    """Force a Telegram post bypassing daily limit. For testing only."""
    if not cfg.TELEGRAM_TOKEN or not cfg.TELEGRAM_CHAT_ID:
        return JSONResponse(status_code=503, content={"status": "error", "error": "TELEGRAM ikke konfigurert"})
    if not db_state.connected or not db_state.pool:
        return JSONResponse(status_code=503, content={"status": "error", "error": "DB offline"})
    try:
        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        async with db_state.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM dagens_kamp
                WHERE timestamp >= $1
                ORDER BY score DESC NULLS LAST, ev DESC NULLS LAST
                LIMIT 1
            """, today_start)
        if not row:
            return {"status": "no_picks", "message": "Ingen picks i dag — kjør /run-analysis først"}
        pick_data = dict(row)
        message = build_telegram_message(pick_data, rank=1)
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"https://api.telegram.org/bot{cfg.TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": cfg.TELEGRAM_CHAT_ID, "text": message},
            )
        return {
            "status": "sent" if resp.status_code == 200 else "failed",
            "telegram_http": resp.status_code,
            "pick": pick_data.get("match"),
            "message_preview": message[:200],
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e)[:200]})


@app.post("/notion-update")
async def notion_update(payload: dict):
    """Finn en side i Notion MATCH_PREDICTIONS og oppdater status."""
    if not cfg.NOTION_TOKEN or not cfg.NOTION_DB_ID:
        return JSONResponse(status_code=503, content={"status": "error", "error": "Notion ikke konfigurert"})
    match_name = payload.get("match")
    new_status = payload.get("status", "NO BET")
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            # Søk etter siden i databasen
            search_resp = await client.post(
                f"https://api.notion.com/v1/databases/{cfg.NOTION_DB_ID}/query",
                headers={
                    "Authorization": f"Bearer {cfg.NOTION_TOKEN}",
                    "Notion-Version": "2022-06-28",
                    "Content-Type": "application/json",
                },
                json={"filter": {"property": "Name", "title": {"equals": match_name}}},
            )
            if search_resp.status_code != 200:
                return JSONResponse(status_code=502, content={"status": "error", "notion_error": search_resp.text[:200]})
            results = search_resp.json().get("results", [])
            if not results:
                return {"status": "not_found", "match": match_name}
            page_id = results[0]["id"]
            # Oppdater status
            update_resp = await client.patch(
                f"https://api.notion.com/v1/pages/{page_id}",
                headers={
                    "Authorization": f"Bearer {cfg.NOTION_TOKEN}",
                    "Notion-Version": "2022-06-28",
                    "Content-Type": "application/json",
                },
                json={"properties": {"Status": {"select": {"name": new_status}}}},
            )
            if update_resp.status_code == 200:
                return {"status": "updated", "page_id": page_id, "match": match_name, "new_status": new_status}
            return JSONResponse(status_code=502, content={"status": "error", "notion_error": update_resp.text[:200]})
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e)[:300]})


@app.get("/notion-list-dbs")
async def notion_list_dbs():
    """Lister alle Notion-databaser tilgjengelig med NOTION_TOKEN — brukes for å finne MODEL_CHANGELOG ID."""
    if not cfg.NOTION_TOKEN:
        return JSONResponse(status_code=503, content={"status": "error", "error": "NOTION_TOKEN mangler"})
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                "https://api.notion.com/v1/search",
                headers={
                    "Authorization": f"Bearer {cfg.NOTION_TOKEN}",
                    "Notion-Version": "2022-06-28",
                    "Content-Type": "application/json",
                },
                json={"filter": {"value": "database", "property": "object"}},
            )
            if resp.status_code != 200:
                return JSONResponse(status_code=502, content={"status": "error", "notion_error": resp.text[:300]})
            results = resp.json().get("results", [])
            databases = []
            for db in results:
                title = ""
                title_list = db.get("title", [])
                if title_list:
                    title = title_list[0].get("plain_text", "")
                databases.append({"id": db.get("id"), "title": title})
            return {"status": "ok", "count": len(databases), "databases": databases}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e)[:300]})


@app.post("/notion-changelog")
async def notion_changelog(payload: dict):
    """
    Logger en endring til Notion MODEL_CHANGELOG database.
    Payload: { "db_id": "...", "dato": "...", "versjon": "...", "endring": "...", "begrunnelse": "..." }
    Hvis db_id mangler, søker automatisk etter databasen med navn 'MODEL_CHANGELOG'.
    """
    if not cfg.NOTION_TOKEN:
        return JSONResponse(status_code=503, content={"status": "error", "error": "NOTION_TOKEN mangler"})

    db_id = payload.get("db_id") or os.getenv("NOTION_CHANGELOG_DB_ID", "")
    dato = payload.get("dato", "")
    versjon = payload.get("versjon", "")
    endring = payload.get("endring", "")
    begrunnelse = payload.get("begrunnelse", "")

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            # Finn db_id automatisk hvis ikke oppgitt
            if not db_id:
                search_resp = await client.post(
                    "https://api.notion.com/v1/search",
                    headers={
                        "Authorization": f"Bearer {cfg.NOTION_TOKEN}",
                        "Notion-Version": "2022-06-28",
                        "Content-Type": "application/json",
                    },
                    json={"query": "MODEL_CHANGELOG", "filter": {"value": "database", "property": "object"}},
                )
                if search_resp.status_code != 200:
                    return JSONResponse(status_code=502, content={"status": "error", "search_error": search_resp.text[:300]})
                results = search_resp.json().get("results", [])
                for db in results:
                    title_list = db.get("title", [])
                    title = title_list[0].get("plain_text", "") if title_list else ""
                    if "MODEL_CHANGELOG" in title.upper() or "CHANGELOG" in title.upper():
                        db_id = db.get("id")
                        break
                if not db_id:
                    return {"status": "db_not_found", "message": "Fant ikke MODEL_CHANGELOG database — oppgi db_id manuelt", "databases_searched": [db.get("title", [{}])[0].get("plain_text","") for db in results if db.get("title")]}

            # Lag ny side i databasen
            page_resp = await client.post(
                "https://api.notion.com/v1/pages",
                headers={
                    "Authorization": f"Bearer {cfg.NOTION_TOKEN}",
                    "Notion-Version": "2022-06-28",
                    "Content-Type": "application/json",
                },
                json={
                    "parent": {"database_id": db_id},
                    "properties": {
                        "Name": {"title": [{"text": {"content": f"{versjon} — {endring}"}}]},
                        "Dato": {"date": {"start": dato}} if dato else {},
                        "Versjon": {"rich_text": [{"text": {"content": versjon}}]},
                        "Endring": {"rich_text": [{"text": {"content": endring}}]},
                        "Begrunnelse": {"rich_text": [{"text": {"content": begrunnelse}}]},
                    },
                },
            )
            if page_resp.status_code == 200:
                page_id = page_resp.json().get("id", "?")
                return {"status": "logged", "db_id": db_id, "page_id": page_id, "versjon": versjon}
            # Fallback: prøv med kun Name-property (ukjent schema)
            page_resp2 = await client.post(
                "https://api.notion.com/v1/pages",
                headers={
                    "Authorization": f"Bearer {cfg.NOTION_TOKEN}",
                    "Notion-Version": "2022-06-28",
                    "Content-Type": "application/json",
                },
                json={
                    "parent": {"database_id": db_id},
                    "properties": {
                        "Name": {"title": [{"text": {"content": f"{versjon} | {dato} | {endring}"}}]},
                    },
                    "children": [
                        {"object": "block", "type": "paragraph", "paragraph": {"rich_text": [{"type": "text", "text": {"content": f"Versjon: {versjon}\nDato: {dato}\nEndring: {endring}\nBegrunnelse: {begrunnelse}"}}]}}
                    ],
                },
            )
            if page_resp2.status_code == 200:
                page_id = page_resp2.json().get("id", "?")
                return {"status": "logged_fallback", "db_id": db_id, "page_id": page_id}
            return JSONResponse(status_code=502, content={"status": "error", "notion_error": page_resp.text[:400], "db_id": db_id})
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e)[:300]})


@app.get("/snapshot-bookmakers")
async def snapshot_bookmakers(league: str = "soccer_epl"):
    """Viser bookmaker-nøkler i siste snapshot for en liga — brukes til å bekrefte bet365-tilstedeværelse."""
    if not db_state.connected or not db_state.pool:
        return JSONResponse(status_code=503, content={"status": "error", "error": "DB offline"})
    try:
        async with db_state.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT data, snapshot_time FROM odds_snapshots WHERE league_key=$1 ORDER BY snapshot_time DESC LIMIT 1",
                league
            )
        if not row:
            return {"status": "no_data", "league": league}
        matches = json.loads(row["data"])
        result = []
        for m in matches[:3]:
            bk_keys = [bk.get("key") for bk in m.get("bookmakers", [])]
            result.append({
                "match": f"{m.get('home_team')} vs {m.get('away_team')}",
                "bookmakers": bk_keys,
                "bet365_present": "bet365" in bk_keys,
                "pinnacle_present": "pinnacle" in bk_keys,
                "count": len(bk_keys),
            })
        return {"status": "ok", "league": league, "snapshot_time": str(row["snapshot_time"]), "sample": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e)[:300]})


@app.post("/send-welcome")
async def send_welcome():
    """Sender og pinner SesomNod Engine-velkomstmelding i Telegram-kanalen."""
    if not cfg.TELEGRAM_TOKEN or not cfg.TELEGRAM_CHAT_ID:
        return JSONResponse(status_code=503, content={"status": "error", "error": "TELEGRAM ikke konfigurert"})
    welcome_text = (
        "Welcome to SesomNod Engine.\n\n"
        "We do not guess. We calculate.\n\n"
        "You will receive statistically verified picks with documented edge against "
        "market benchmarks. Quality over quantity — three validated opportunities "
        "outperform thirty forced guesses.\n\n"
        "Every pick includes pre-match odds, EV calculations, and post-result CLV "
        "analysis. We post only when mathematics demands it. Expect discipline, not "
        "daily noise.\n\n"
        "— SesomNod Engine"
    )
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            send_resp = await client.post(
                f"https://api.telegram.org/bot{cfg.TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": cfg.TELEGRAM_CHAT_ID, "text": welcome_text, "parse_mode": "HTML"},
            )
            if send_resp.status_code != 200:
                return JSONResponse(status_code=502, content={"status": "error", "detail": send_resp.text[:200]})
            msg_id = send_resp.json().get("result", {}).get("message_id")
            pin_resp = await client.post(
                f"https://api.telegram.org/bot{cfg.TELEGRAM_TOKEN}/pinChatMessage",
                json={"chat_id": cfg.TELEGRAM_CHAT_ID, "message_id": msg_id, "disable_notification": True},
            )
        return {
            "status": "ok",
            "message_id": msg_id,
            "pinned": pin_resp.status_code == 200,
            "pin_detail": pin_resp.json() if pin_resp.status_code != 200 else "ok",
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e)[:300]})


@app.post("/add-pick")
async def add_pick(payload: dict):
    """Inserter en pick direkte i dagens_kamp og logger til Notion. Inkluderer Dual Benchmark-felt."""
    if not db_state.connected or not db_state.pool:
        return JSONResponse(status_code=503, content={"status": "error", "error": "DB offline"})
    try:
        kickoff_dt = datetime.fromisoformat(payload["kickoff"])
        async with db_state.pool.acquire() as conn:
            row_id = await conn.fetchval("""
                INSERT INTO dagens_kamp
                    (match, league, home_team, away_team, pick, odds, stake,
                     edge, ev, confidence, kickoff, telegram_posted,
                     market_type, score, bookmaker_count, pinnacle_opening)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,FALSE,$12,$13,$14,$15)
                RETURNING id
            """,
                f"{payload['home_team']} vs {payload['away_team']}",
                payload.get("league", ""),
                payload["home_team"],
                payload["away_team"],
                payload.get("pick", payload["home_team"] + " vinner"),
                float(payload["odds"]),
                5.0,
                float(payload.get("soft_edge", payload.get("edge", 0))),
                float(payload.get("soft_ev", payload.get("ev_pct", 0))),
                int(payload.get("confidence", 75)),
                kickoff_dt,
                payload.get("market_type", "h2h"),
                float(payload.get("score", 0)) if payload.get("score") else None,
                int(payload.get("bookmaker_count", 0)) if payload.get("bookmaker_count") else None,
                float(payload.get("pinnacle_opening", 0)) if payload.get("pinnacle_opening") else None,
            )
        pick_data = {**payload, "id": row_id, "kickoff": kickoff_dt}
        await _log_notion_pick(pick_data)
        return {
            "status": "ok", "id": row_id,
            "match": f"{payload['home_team']} vs {payload['away_team']}",
            "soft_edge": payload.get("soft_edge"),
            "soft_ev": payload.get("soft_ev"),
            "soft_book": payload.get("soft_book", BENCHMARK),
            "pinnacle_clv": payload.get("pinnacle_clv"),
            "clv_missing": payload.get("clv_missing", False),
            "benchmark_book": BENCHMARK,
        }
    except Exception as e:
        logger.exception(f"[/add-pick] Feil: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e)[:300]})


@app.get("/db-schema")
async def db_schema(table: str = "picks"):
    """Viser alle kolonner i en tabell — brukes til å verifisere migrasjoner."""
    if not db_state.connected or not db_state.pool:
        return JSONResponse(status_code=503, content={"status": "error", "error": "DB offline"})
    try:
        async with db_state.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT column_name, data_type, column_default, is_nullable
                FROM information_schema.columns
                WHERE table_name = $1
                ORDER BY column_name
            """, table)
            cols = [dict(r) for r in rows]
            names = [r["column_name"] for r in cols]

            # Picks-spesifikk sjekk
            check = {}
            if table == "picks":
                check = {
                    "telegram_posted": "telegram_posted" in names,
                    "posted_at": "posted_at" in names,
                    "scan_session": "scan_session" in names,
                    "soft_edge": "soft_edge" in names,
                    "soft_ev": "soft_ev" in names,
                }

            # Teller poster
            count_row = await conn.fetchrow(f"SELECT COUNT(*) AS total FROM {table}")
            total = count_row["total"] if count_row else 0

            posted_count = None
            if table == "picks" and "telegram_posted" in names:
                p = await conn.fetchrow(
                    "SELECT SUM(CASE WHEN telegram_posted THEN 1 ELSE 0 END) AS posted FROM picks"
                )
                posted_count = p["posted"] if p else 0

        return {
            "table": table,
            "column_count": len(cols),
            "columns": cols,
            "check": check,
            "total_rows": total,
            "telegram_posted_rows": posted_count,
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e)[:300]})


@app.get("/db/retry")
async def db_retry():
    logger.info("[DB] Manuell retry...")
    if db_state.pool:
        try:
            await db_state.pool.close()
        except Exception:
            pass
    await db_state.mark_fail("Manuell retry")
    success = await connect_db()
    return {"triggered": True, "success": success, "db": db_state.to_dict()}


@app.get("/status")
async def status():
    now = datetime.now(timezone.utc)

    # API-kall og siste snapshot fra DB
    api_calls_month = 0
    last_snapshot_iso = None
    last_fetch_ago_sec = None
    try:
        if db_state.connected and db_state.pool:
            async with db_state.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT COUNT(*) AS cnt FROM api_calls "
                    "WHERE call_date >= DATE_TRUNC('month', CURRENT_DATE)"
                )
                api_calls_month = row["cnt"] if row else 0

                snap_row = await conn.fetchrow(
                    "SELECT MAX(snapshot_time) AS last_snap FROM odds_snapshots"
                )
                if snap_row and snap_row["last_snap"]:
                    last_snapshot_iso = snap_row["last_snap"].isoformat()
                    last_fetch_ago_sec = round((now - snap_row["last_snap"]).total_seconds())
    except Exception:
        pass

    return {
        "service": cfg.SERVICE_NAME,
        "version": "10.2.0-btts",
        "db": db_state.to_dict(),
        "config": {
            "database_url_set": bool(cfg.DATABASE_URL),
            "telegram_set": bool(cfg.TELEGRAM_TOKEN),
            "odds_api_set": bool(cfg.ODDS_API_KEY),
            "notion_token_set": bool(cfg.NOTION_TOKEN),
            "notion_db_set": bool(cfg.NOTION_DB_ID),
            "port": cfg.PORT,
            "environment": cfg.ENVIRONMENT,
        },
        "scanner": {
            "leagues": len(SCAN_LEAGUES),
            "top4_leagues": len(TOP4_LEAGUES),
            "ev_min": EV_MIN,
            "edge_min": EDGE_MIN,
            "confidence_min": CONFIDENCE_MIN,
            "min_bookmakers": MIN_BOOKMAKERS,
            "odds_min": ODDS_MIN,
            "odds_max": ODDS_MAX,
            "benchmark": BENCHMARK,
            "soft_edge_min": SOFT_EDGE_MIN,
            "soft_ev_min": SOFT_EV_MIN,
            "pinnacle_clv_track": PINNACLE_CLV_TRACK,
            "pinnacle_margin_max": PINNACLE_MARGIN_MAX,
            "daily_post_limit": DAILY_POST_LIMIT,
        },
        "scheduler": {
            "windows": [
                {"name": "early",    "trigger": "07:00 UTC", "leagues": len(SCAN_LEAGUES),  "type": "fetch+analyse"},
                {"name": "evening",  "trigger": "18:00 UTC", "leagues": len(TOP4_LEAGUES),  "type": "fetch+analyse"},
                {"name": "prekickoff", "trigger": "20:00 UTC", "leagues": 0, "type": "analyse_cache"},
            ],
            "api_budget_monthly": API_MONTHLY_BUDGET,
            "api_calls_this_month": api_calls_month,
            "api_calls_remaining": API_MONTHLY_BUDGET - api_calls_month,
        },
        "atomic": {
            "mode": ATOMIC_MODE,
            "atomic_score_min": ATOMIC_SCORE_MIN,
            "xg_divergence_threshold": XG_DIVERGENCE_THRESHOLD,
            "velocity_sharp_delta": VELOCITY_SHARP_DELTA,
            "velocity_sharp_minutes": VELOCITY_SHARP_MINUTES,
            "football_data_api_set": bool(cfg.FOOTBALL_DATA_API_KEY),
            "openweather_api_set": bool(cfg.OPENWEATHER_API_KEY),
            "signals_available": _SIGNALS_AVAILABLE,
            "xg_status": "XG_PENDING_API_KEY" if not cfg.FOOTBALL_DATA_API_KEY else "XG_ACTIVE",
            "velocity_status": "VELOCITY_LOGGING_MODE",
            "weather_status": "WEATHER_UNAVAILABLE" if not cfg.OPENWEATHER_API_KEY else ("WEATHER_ACTIVE" if _SIGNALS_AVAILABLE else "WEATHER_IMPORT_FAIL"),
            "referee_status": "REFEREE_DATA_COLLECTION_v10.1.0",
        },
        "last_fetch": {
            "snapshot_time": last_snapshot_iso,
            "last_fetch_ago_sec": last_fetch_ago_sec,
        },
        "timestamp": now.isoformat(),
    }


@app.get("/admin/test-streak")
async def admin_test_streak(
    team: str = "Arsenal",
    league: str = "Premier League",
):
    """
    STEG 10 — Test Signal 5 scoring streak.
    ?team=Arsenal&league=Premier+League
    """
    if not cfg.FOOTBALL_DATA_API_KEY:
        return JSONResponse(status_code=400, content={"error": "FOOTBALL_DATA_API_KEY mangler"})
    result = await get_scoring_streak(team, league, cfg.FOOTBALL_DATA_API_KEY)

    # Test calculate_atomic_score med streak +2
    dummy_velocity = {"atomic_points": 2, "velocity_type": "SHARP_MONEY"}
    dummy_xg       = {"atomic_points": 2, "signal": "XG_DIVERGENCE"}
    dummy_weather  = {"atomic_points": 1, "signal": "FAVORABLE"}
    dummy_ref      = {"atomic_points": 0, "signal": "NEUTRAL_REF"}
    streak_home    = result
    streak_away    = {"streak_signal": "NEUTRAL", "atomic_points": 0}
    atomic = calculate_atomic_score(
        dummy_velocity, dummy_xg, 3.5, 5.0,
        weather_result=dummy_weather,
        referee_result=dummy_ref,
        streak_home_result=streak_home,
        streak_away_result=streak_away,
    )

    return {
        "team": team,
        "league": league,
        "streak_result": result,
        "atomic_score_with_streak": atomic["atomic_score"],
        "tier": atomic["tier"],
        "market_hint": atomic["market_hint"],
        "signals_triggered": atomic["signals_triggered"],
        "streak_home_signal": atomic["streak_home_signal"],
    }


@app.post("/admin/fix-null-teams")
async def admin_fix_null_teams():
    """STEG 2 — Fiks NULL home_team/away_team fra match_name."""
    if not db_state.connected or not db_state.pool:
        return JSONResponse(status_code=503, content={"error": "DB offline"})
    async with db_state.pool.acquire() as conn:
        null_picks_before = await conn.fetchval("""
            SELECT COUNT(*) FROM picks
            WHERE (home_team IS NULL OR home_team = '')
            AND match_name LIKE '% vs %'
        """)
        null_dk_before = await conn.fetchval("""
            SELECT COUNT(*) FROM dagens_kamp
            WHERE (home_team IS NULL OR home_team = '')
            AND match LIKE '% vs %'
        """)
        await conn.execute("""
            UPDATE picks SET
                home_team = TRIM(SPLIT_PART(match_name, ' vs ', 1)),
                away_team = TRIM(SPLIT_PART(match_name, ' vs ', 2))
            WHERE (home_team IS NULL OR home_team = '')
            AND match_name LIKE '% vs %'
            AND SPLIT_PART(match_name, ' vs ', 2) != ''
        """)
        await conn.execute("""
            UPDATE dagens_kamp SET
                home_team = TRIM(SPLIT_PART(match, ' vs ', 1)),
                away_team = TRIM(SPLIT_PART(match, ' vs ', 2))
            WHERE (home_team IS NULL OR home_team = '')
            AND match LIKE '% vs %'
            AND SPLIT_PART(match, ' vs ', 2) != ''
        """)
        null_picks_after = await conn.fetchval("""
            SELECT COUNT(*) FROM picks WHERE home_team IS NULL OR home_team = ''
        """)
        null_dk_after = await conn.fetchval("""
            SELECT COUNT(*) FROM dagens_kamp WHERE home_team IS NULL OR home_team = ''
        """)
        sample = await conn.fetch("""
            SELECT id, match_name, home_team, away_team, atomic_score, soft_edge, tier
            FROM picks ORDER BY created_at DESC LIMIT 5
        """)
        # streak columns check
        streak_cols = await conn.fetch("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'picks' AND column_name LIKE '%streak%'
        """)
    return {
        "null_picks_before": null_picks_before,
        "null_dagenskamp_before": null_dk_before,
        "null_picks_after": null_picks_after,
        "null_dagenskamp_after": null_dk_after,
        "picks_sample": [dict(r) for r in sample],
        "streak_columns": [r["column_name"] for r in streak_cols],
    }


@app.get("/admin/fase0-kartlegg")
async def admin_fase0_kartlegg():
    """FASE 0 — Kartlegg faktisk DB-data for v11.0 planlegging."""
    if not db_state.connected or not db_state.pool:
        return JSONResponse(status_code=503, content={"error": "DB offline"})

    result = {}

    async with db_state.pool.acquire() as conn:
        # A) Picks kolonner (kjør FØR alt annet)
        cols = await conn.fetch("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'picks'
            ORDER BY ordinal_position
        """)
        result["picks_columns"] = [{"col": r["column_name"], "type": r["data_type"]} for r in cols]
        pick_col_names = [r["column_name"] for r in cols]

        # A) Siste pick
        last_pick = await conn.fetchrow("SELECT * FROM picks ORDER BY created_at DESC LIMIT 1")
        result["A_last_pick"] = dict(last_pick) if last_pick else None

        # B/D) odds_snapshots kolonner
        snap_cols = await conn.fetch("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'odds_snapshots'
            ORDER BY ordinal_position
        """)
        result["odds_snapshots_columns"] = [{"col": r["column_name"], "type": r["data_type"]} for r in snap_cols]

        # B) Snapshot data-struktur — vis ett eksempel event som JSON fragment
        snap_row = await conn.fetchrow("""
            SELECT league_key, snapshot_time,
                   LEFT(data::text, 2000) AS data_preview
            FROM odds_snapshots
            ORDER BY snapshot_time DESC LIMIT 1
        """)
        result["F_snapshot_sample"] = dict(snap_row) if snap_row else None

        # B) Hent distinkte market keys fra snapshot JSONB
        try:
            snap_markets = await conn.fetch("""
                SELECT DISTINCT mk->>'key' AS market_key, bk->>'key' AS bookmaker
                FROM odds_snapshots,
                     jsonb_array_elements(data::jsonb) AS ev,
                     jsonb_array_elements(ev->'bookmakers') AS bk,
                     jsonb_array_elements(bk->'markets') AS mk
                ORDER BY market_key, bookmaker
                LIMIT 40
            """)
            result["B_market_keys"] = [dict(r) for r in snap_markets]
        except Exception as e:
            result["B_market_keys_error"] = str(e)

        # D) BTTS i feed
        try:
            btts = await conn.fetch("""
                SELECT DISTINCT mk->>'key' AS market_key
                FROM odds_snapshots,
                     jsonb_array_elements(data::jsonb) AS ev,
                     jsonb_array_elements(ev->'bookmakers') AS bk,
                     jsonb_array_elements(bk->'markets') AS mk
                WHERE (mk->>'key') ILIKE '%btts%'
                   OR (mk->>'key') ILIKE '%both%'
                LIMIT 5
            """)
            result["D_btts_in_feed"] = [dict(r) for r in btts]
        except Exception as e:
            result["D_btts_error"] = str(e)

        # E) xG i picks
        xg_data = await conn.fetch("""
            SELECT match_name, signal_xg_home, signal_xg_away,
                   xg_divergence_home, xg_divergence_away
            FROM picks WHERE signal_xg_home IS NOT NULL LIMIT 3
        """)
        result["E_xg_data"] = [dict(r) for r in xg_data]

        # market_type_detail distinkte verdier (erstatter market_type)
        if "market_type_detail" in pick_col_names:
            mt = await conn.fetch("""
                SELECT DISTINCT market_type_detail, COUNT(*) AS cnt
                FROM picks GROUP BY market_type_detail
            """)
            result["picks_market_type_detail"] = [dict(r) for r in mt]

    return result


@app.post("/admin/fix-picks-v2-schema")
async def admin_fix_picks_v2_schema():
    """Legger til manglende kolonner i picks_v2 (idempotent)."""
    if not db_state.connected or not db_state.pool:
        return JSONResponse(status_code=503, content={"error": "DB offline"})
    try:
        async with db_state.pool.acquire() as conn:
            await conn.execute("""
                ALTER TABLE picks_v2
                ADD COLUMN IF NOT EXISTS timestamp TIMESTAMPTZ DEFAULT NOW();
            """)
            cols = await conn.fetch(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'picks_v2' ORDER BY column_name"
            )
        return {"status": "OK", "picks_v2_columns": [r["column_name"] for r in cols]}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)[:300]})


@app.post("/admin/backfill-picks-v2")
async def admin_backfill_picks_v2():
    """
    Kjører backfill fra picks → picks_v2 manuelt.
    Idempotent via ON CONFLICT DO NOTHING.
    Bruker kun kolonner som faktisk finnes i picks.
    """
    if not db_state.connected or not db_state.pool:
        return JSONResponse(status_code=503, content={"error": "DB offline"})

    try:
        async with db_state.pool.acquire() as conn:
            before = await conn.fetchval("SELECT COUNT(*) FROM picks_v2")
            await conn.execute("""
                INSERT INTO picks_v2 (
                    id, match_name,
                    odds, soft_edge, soft_ev, soft_book,
                    pinnacle_clv, atomic_score, signals_triggered,
                    result, telegram_posted, posted_at, scan_session,
                    benchmark_book, clv_reference_book, clv_missing,
                    created_at, timestamp,
                    tier, tier_label, kelly_multiplier, kelly_stake
                )
                SELECT
                    id,
                    COALESCE(match, ''),
                    odds,
                    soft_edge,
                    soft_ev,
                    soft_book,
                    pinnacle_clv,
                    COALESCE(atomic_score, 0),
                    COALESCE(signals_triggered, '[]'::jsonb),
                    result,
                    COALESCE(telegram_posted, FALSE),
                    posted_at,
                    scan_session,
                    benchmark_book,
                    clv_reference_book,
                    COALESCE(clv_missing, FALSE),
                    COALESCE(timestamp, NOW()),
                    COALESCE(timestamp, NOW()),
                    COALESCE(tier, 'MONITORED'),
                    COALESCE(tier_label, '📊 MONITORED'),
                    COALESCE(kelly_multiplier, 0.00),
                    COALESCE(kelly_stake, 0.00)
                FROM picks
                ORDER BY id
                ON CONFLICT (id) DO NOTHING
            """)
            after = await conn.fetchval("SELECT COUNT(*) FROM picks_v2")
            old_count = await conn.fetchval("SELECT COUNT(*) FROM picks")

        return {
            "status": "BACKFILL_OK",
            "picks_v2_before": before,
            "picks_v2_after": after,
            "picks_count": old_count,
            "inserted": after - before,
            "match": old_count == after,
        }
    except Exception as e:
        logger.error(f"[Admin] backfill-picks-v2 feilet: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)[:300]})


@app.post("/admin/clean-old-picks")
async def admin_clean_old_picks():
    """Sletter fullførte og gamle picks fra databasen. Beholder kun picks fra de siste 2 dagene."""
    if not db_state.connected or not db_state.pool:
        return JSONResponse(status_code=503, content={"error": "DB offline"})
    try:
        async with db_state.pool.acquire() as conn:
            # Delete from picks table: older than 2 days
            deleted_picks = await conn.fetchval(
                "SELECT COUNT(*) FROM picks WHERE timestamp < NOW() - INTERVAL '2 days'"
            )
            await conn.execute(
                "DELETE FROM picks WHERE timestamp < NOW() - INTERVAL '2 days'"
            )
            # Delete from dagens_kamp: completed matches or older than 2 days
            deleted_dk = await conn.fetchval("""
                SELECT COUNT(*) FROM dagens_kamp
                WHERE kickoff < NOW() - INTERVAL '4 hours'
                   OR kickoff < CURRENT_DATE
            """)
            await conn.execute("""
                DELETE FROM dagens_kamp
                WHERE kickoff < NOW() - INTERVAL '4 hours'
                   OR kickoff < CURRENT_DATE
            """)
            remaining_picks = await conn.fetchval("SELECT COUNT(*) FROM picks")
            remaining_dk = await conn.fetchval("SELECT COUNT(*) FROM dagens_kamp")
        return {
            "status": "ok",
            "deleted_from_picks": int(deleted_picks),
            "deleted_from_dagens_kamp": int(deleted_dk),
            "remaining_picks": int(remaining_picks),
            "remaining_dagens_kamp": int(remaining_dk),
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)[:300]})


@app.post("/admin/cleanup-test-picks")
async def admin_cleanup_test_picks():
    """Sletter test-rader med home_team='TestHome' fra picks og dagens_kamp."""
    if not db_state.connected or not db_state.pool:
        return JSONResponse(status_code=503, content={"error": "DB offline"})
    try:
        async with db_state.pool.acquire() as conn:
            # STEG 1 — finn
            picks_rows = await conn.fetch(
                "SELECT id, match_name, home_team, away_team, telegram_posted "
                "FROM picks WHERE home_team = 'TestHome' OR away_team = 'TestAway'"
            )
            dk_rows = await conn.fetch(
                "SELECT id, home_team, away_team "
                "FROM dagens_kamp WHERE home_team = 'TestHome' OR away_team = 'TestAway'"
            )
            # STEG 2 — slett
            picks_del = await conn.fetchval(
                "WITH d AS (DELETE FROM picks WHERE home_team='TestHome' OR away_team='TestAway' RETURNING id) "
                "SELECT COUNT(*) FROM d"
            )
            dk_del = await conn.fetchval(
                "WITH d AS (DELETE FROM dagens_kamp WHERE home_team='TestHome' OR away_team='TestAway' RETURNING id) "
                "SELECT COUNT(*) FROM d"
            )
            # STEG 3 — bekreft
            picks_left = await conn.fetchval(
                "SELECT COUNT(*) FROM picks WHERE home_team='TestHome'"
            )
            dk_left = await conn.fetchval(
                "SELECT COUNT(*) FROM dagens_kamp WHERE home_team='TestHome'"
            )
        return {
            "found_in_picks": [dict(r) for r in picks_rows],
            "found_in_dagens_kamp": [dict(r) for r in dk_rows],
            "deleted_from_picks": picks_del,
            "deleted_from_dagens_kamp": dk_del,
            "picks_remaining": picks_left,
            "dagens_kamp_remaining": dk_left,
            "clean": picks_left == 0 and dk_left == 0,
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)[:300]})


@app.post("/admin/picks-rollback")
async def admin_picks_rollback():
    """
    Rollback: picks → picks_v2_failed, picks_v1_backup → picks.
    Brukes hvis picks-switch brøt eksisterende endpoints.
    """
    if not db_state.connected or not db_state.pool:
        return JSONResponse(status_code=503, content={"error": "DB offline"})

    try:
        async with db_state.pool.acquire() as conn:
            # Sjekk at backup eksisterer
            has_backup = await conn.fetchval(
                "SELECT EXISTS (SELECT FROM information_schema.tables "
                "WHERE table_name = 'picks_v1_backup')"
            )
            if not has_backup:
                return JSONResponse(
                    status_code=404,
                    content={"error": "picks_v1_backup finnes ikke — rollback ikke mulig"}
                )

            count_before = await conn.fetchval("SELECT COUNT(*) FROM picks")
            backup_count = await conn.fetchval("SELECT COUNT(*) FROM picks_v1_backup")

            await conn.execute("""
                ALTER TABLE picks RENAME TO picks_v2_failed;
                ALTER TABLE picks_v1_backup RENAME TO picks;
            """)

            count_after = await conn.fetchval("SELECT COUNT(*) FROM picks")

        logger.info(f"[Admin] Rollback fullført: picks_v1_backup → picks ({count_after} rader)")
        return {
            "status": "ROLLBACK_OK",
            "picks_count_after": count_after,
            "picks_v2_failed": "bevart for analyse",
            "backup_count": backup_count,
        }
    except Exception as e:
        logger.error(f"[Admin] picks-rollback feilet: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)[:300]})


@app.post("/admin/picks-switch")
async def admin_picks_switch():
    """
    FASE F: Atomic table switch — picks_v2 → picks.
    KUN kall etter at picks_v2 backfill er verifisert (old == new).
    Rollback automatisk ved feil.
    """
    if not db_state.connected or not db_state.pool:
        return JSONResponse(status_code=503, content={"error": "DB offline"})

    try:
        async with db_state.pool.acquire() as conn:
            # Verifiser old == new FØR switch
            counts = await conn.fetchrow("""
                SELECT
                    (SELECT COUNT(*) FROM picks) as old_count,
                    (SELECT COUNT(*) FROM picks_v2) as new_count
            """)
            old_count = counts["old_count"]
            new_count = counts["new_count"]

            if old_count != new_count:
                return JSONResponse(
                    status_code=409,
                    content={
                        "error": "Count mismatch — switch avbrutt",
                        "old_picks": old_count,
                        "new_picks_v2": new_count,
                        "diff": abs(old_count - new_count),
                    }
                )

            # Atomisk switch
            await conn.execute("""
                BEGIN;
                ALTER TABLE picks RENAME TO picks_v1_backup;
                ALTER TABLE picks_v2 RENAME TO picks;
                DROP TRIGGER IF EXISTS picks_sync_trigger ON picks_v1_backup;
                COMMIT;
            """)

            # Verifiser
            final_count = await conn.fetchval("SELECT COUNT(*) FROM picks")

        logger.info(f"[Admin] picks_v2 → picks switch fullført. {final_count} rader.")
        return {
            "status": "SWITCHED",
            "picks_count": final_count,
            "picks_v1_backup": "bevart",
            "old_count_before_switch": old_count,
        }

    except Exception as e:
        logger.error(f"[Admin] picks-switch feilet: {e}")
        # Rollback: sjekk om vi trenger å gjenopprette
        try:
            async with db_state.pool.acquire() as conn:
                has_backup = await conn.fetchval(
                    "SELECT EXISTS (SELECT FROM information_schema.tables "
                    "WHERE table_name = 'picks_v1_backup')"
                )
                has_picks = await conn.fetchval(
                    "SELECT EXISTS (SELECT FROM information_schema.tables "
                    "WHERE table_name = 'picks')"
                )
                if has_backup and not has_picks:
                    await conn.execute("""
                        BEGIN;
                        ALTER TABLE picks_v1_backup RENAME TO picks;
                        COMMIT;
                    """)
                    logger.warning("[Admin] Rollback fullført — picks_v1_backup → picks")
        except Exception as rb_err:
            logger.error(f"[Admin] Rollback feilet: {rb_err}")

        return JSONResponse(
            status_code=500,
            content={"error": str(e)[:200], "rollback": "forsøkt"}
        )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.exception(f"[Global] Uventet feil: {exc}")
    return JSONResponse(
        status_code=500,
        content={
            "status": "error",
            "message": "Intern serverfeil",
            "type": type(exc).__name__,
            "detail": str(exc)[:200]
        }
    )


if __name__ == "__main__":
    import uvicorn
    logger.info(f"Fyrer opp Uvicorn på port {cfg.PORT}...")
    uvicorn.run(app, host="0.0.0.0", port=cfg.PORT, log_level="info")

@app.get("/test-scorers")
async def test_scorers_endpoint(
    home: str = "Brighton and Hove Albion",
    away: str = "Liverpool"
):
    """Diagnostic endpoint: test scorer fetch with real Railway API key."""
    import httpx
    key = cfg.FOOTBALL_DATA_API_KEY  # uses hardcoded fallback if env var not set
    out: dict = {
        "key_present": bool(key),
        "key_preview": key[:8] + "..." if key else "MISSING",
        "scorers": [],
        "raw_pl_top10": [],
        "api_status": None,
        "error": None,
    }
    if not key:
        return out
    try:
        # Single fetch for both key validation AND raw data display
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get(
                "https://api.football-data.org/v4/competitions/PL/scorers?limit=10",
                headers={"X-Auth-Token": key},
            )
        out["api_status"] = r.status_code
        if r.status_code != 200:
            out["error"] = r.text[:200]
            return out
        raw = r.json().get("scorers", [])
        out["raw_pl_top10"] = [
            {"name": s.get("player", {}).get("name"),
             "team": s.get("team", {}).get("name"),
             "goals": s.get("numberOfGoals")}
            for s in raw
        ]
        # Wait 1 second before _get_scorers to avoid rate-limiting the second PL call
        import asyncio
        await asyncio.sleep(1)
        out["scorers"] = _get_scorers(home, away)
    except Exception as e:
        out["error"] = str(e)
    return out


# ─── Blokk 3.5 — Notion result logger ────────────────────────────────────────
async def _log_result_to_notion(pick_row: dict, result: str, clv: Optional[float]):
    """Logger pick-resultat til Notion (oppdaterer Status + CLV)."""
    if not cfg.NOTION_TOKEN or not cfg.NOTION_DB_ID:
        return
    try:
        name = f"✅ RESULT: {pick_row.get('home_team','?')} vs {pick_row.get('away_team','?')}"
        status_label = result  # WIN, LOSS, VOID

        props: dict = {
            "Name":    {"title": [{"text": {"content": name}}]},
            "Status":  {"select": {"name": status_label}},
            "Pick":    {"rich_text": [{"text": {"content": str(pick_row.get("pick",""))}}]},
            "Odds":    {"number": float(pick_row.get("odds") or 0)},
        }
        if clv is not None:
            props["EV"] = {"rich_text": [{"text": {"content": f"CLV: {clv:+.2f}%"}}]}
        if pick_row.get("home_team"):
            props["Hjemmelag"] = {"rich_text": [{"text": {"content": pick_row["home_team"]}}]}
        if pick_row.get("away_team"):
            props["Bortelag"] = {"rich_text": [{"text": {"content": pick_row["away_team"]}}]}

        async with httpx.AsyncClient(timeout=12) as client:
            resp = await client.post(
                "https://api.notion.com/v1/pages",
                headers={
                    "Authorization": f"Bearer {cfg.NOTION_TOKEN}",
                    "Notion-Version": "2022-06-28",
                    "Content-Type": "application/json",
                },
                json={"parent": {"database_id": cfg.NOTION_DB_ID}, "properties": props},
            )
        if resp.status_code == 200:
            logger.info(f"[Notion] Resultat logget: {result} for pick {pick_row.get('id')}")
        else:
            logger.warning(f"[Notion] Resultat-logging feil {resp.status_code}: {resp.text[:150]}")
    except Exception as exc:
        logger.warning(f"[Notion] Resultat-logging exception: {exc}")


# ─── Blokk 3 — v10.2.1: PATCH /picks/{pick_id}/result ────────────────────────
@app.patch("/picks/{pick_id}/result")
async def update_pick_result(pick_id: int, body: ResultUpdate):
    if body.result not in {"WIN", "LOSS", "VOID"}:
        raise HTTPException(status_code=400, detail='result må være "WIN", "LOSS" eller "VOID"')
    if not db_state.connected or db_state.pool is None:
        raise HTTPException(status_code=503, detail="Database ikke tilgjengelig")
    async with db_state.pool.acquire() as conn:
        # Ensure result column exists on dagens_kamp
        await conn.execute("""
            ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS result       VARCHAR(10);
            ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS closing_odds FLOAT;
            ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS clv          FLOAT;
        """)
        pick = await conn.fetchrow(
            "SELECT id, odds, pick, home_team, away_team, match FROM dagens_kamp WHERE id = $1",
            pick_id
        )
        if not pick:
            raise HTTPException(status_code=404, detail=f"Pick {pick_id} finnes ikke i dagens_kamp")
        clv: Optional[float] = None
        if body.closing_odds and body.closing_odds > 0:
            market_odds = float(pick["odds"] or 0)
            if market_odds > 0:
                clv = round((market_odds / body.closing_odds - 1) * 100, 2)
        await conn.execute(
            "UPDATE dagens_kamp SET result=$1, closing_odds=$2, clv=$3 WHERE id=$4",
            body.result, body.closing_odds, clv, pick_id
        )
    pick_dict = dict(pick)
    pick_dict["match_name"] = pick_dict.pop("match", "")
    # Fire-and-forget Notion logging
    asyncio.create_task(_log_result_to_notion(pick_dict, body.result, clv))
    return {
        "pick_id": pick_id,
        "result": body.result,
        "closing_odds": body.closing_odds,
        "clv": clv,
        "clv_label": f"{clv:+.2f}%" if clv is not None else "Ingen CLV",
    }


@app.get("/control-wall")
async def get_control_wall():
    if not db_state.connected or db_state.pool is None:
        return JSONResponse(status_code=503, content={"error": "Database ikke tilgjengelig"})
    try:
        async with db_state.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    id,
                    COALESCE(match, match_name, '') AS match_name,
                    COALESCE(market_hint, pick, '') AS market,
                    COALESCE(atomic_score, 0)       AS omega_score,
                    COALESCE(edge, 0)               AS edge,
                    COALESCE(ev, 0)                 AS ev,
                    COALESCE(confidence, '')        AS confidence,
                    COALESCE(xg_divergence_home, 0) AS xg_home,
                    COALESCE(xg_divergence_away, 0) AS xg_away,
                    result
                FROM dagens_kamp
                WHERE timestamp >= NOW() - INTERVAL '24 hours'
                ORDER BY COALESCE(atomic_score, 0) DESC
                LIMIT 200
            """)

        def veto(r: dict) -> str:
            ev_val = r["ev"] or 0
            om_val = r["omega_score"] or 0
            conf   = r["confidence"] or ""
            if ev_val < 8:
                return f"EV +{ev_val:.1f}% under terskel (≥8% kreves)"
            if conf not in ("HIGH", "VERY HIGH"):
                return f"Konfidens '{conf}' — ikke HIGH"
            if om_val < 50:
                return f"OMEGA {om_val} — signal for svakt (≥50 kreves)"
            return "Utilstrekkelig samlet edge"

        all_rows = [dict(r) for r in rows]
        accepted = [r for r in all_rows if (r["ev"] or 0) >= 8 and (r["confidence"] or "") in ("HIGH", "VERY HIGH")]
        rejected = [r for r in all_rows if not ((r["ev"] or 0) >= 8 and (r["confidence"] or "") in ("HIGH", "VERY HIGH"))]

        return {
            "date": datetime.utcnow().date().isoformat(),
            "scanned": len(all_rows),
            "accepted": len(accepted),
            "rejected_count": len(rejected),
            "rejected_picks": [
                {**r, "veto_reason": veto(r)} for r in rejected[:30]
            ],
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)[:200]})


@app.get("/picks/{pick_id}/receipt")
async def get_pick_receipt(pick_id: int):
    if not db_state.connected or db_state.pool is None:
        return JSONResponse(status_code=503, content={"error": "Database ikke tilgjengelig"})
    try:
        async with db_state.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT
                    id,
                    COALESCE(match, match_name, '') AS match_name,
                    COALESCE(market_hint, pick, '') AS market,
                    COALESCE(odds, 0)               AS placed_odds,
                    COALESCE(atomic_score, 0)       AS omega_score,
                    COALESCE(ev, 0)                 AS ev_pct,
                    clv                             AS clv_pct,
                    COALESCE(result, 'PENDING')     AS result,
                    timestamp
                FROM dagens_kamp
                WHERE id = $1
            """, pick_id)
        if not row:
            return JSONResponse(status_code=404, content={"error": f"Pick {pick_id} ikke funnet"})
        d = dict(row)
        return {
            "id": d["id"],
            "match": d["match_name"],
            "market": d["market"],
            "placed_odds": float(d["placed_odds"] or 0),
            "omega_score": int(d["omega_score"] or 0),
            "ev_pct": float(d["ev_pct"] or 0),
            "clv_pct": round(float(d["clv_pct"]), 2) if d["clv_pct"] is not None else None,
            "result": d["result"],
            "date": d["timestamp"].strftime("%d.%m.%Y") if d["timestamp"] else "",
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)[:200]})


# ─── TEAM LOGO PROXY ─────────────────────────────────────────────────────────
# Fetches TheSportsDB server-side to avoid browser CORS restrictions.
@app.get("/team-logo")
async def get_team_logo(team: str):
    try:
        import httpx
        url = f"https://www.thesportsdb.com/api/v1/json/3/searchteams.php?t={team}"
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.get(url)
            data = r.json()
            if data.get("teams"):
                logo = data["teams"][0].get("strTeamBadge") or ""
                return {"logo": logo}
        return {"logo": ""}
    except Exception:
        return {"logo": ""}


@app.get("/ladder-history")
async def get_ladder_history():
    """Return bot pick history with bankroll simulation (ladder/reset model)."""
    _EMPTY = {
        "ladder": [], "current_bankroll": 1000, "peak_bankroll": 1000,
        "max_streak": 0, "total_picks": 0, "wins": 0, "hit_rate": 0.0,
        "milestones": [], "next_pick": None, "bot_status": "WAITING",
    }
    try:
        async with db_state.pool.acquire() as conn:
            # Use dagens_kamp — has pick, edge, ev, kickoff with correct column names
            settled = await conn.fetch("""
                SELECT
                    COALESCE(home_team || ' vs ' || away_team, match, '') AS match_name,
                    COALESCE(league, '') AS league,
                    kickoff AS kickoff_time,
                    odds,
                    COALESCE(ev, 0) AS soft_ev,
                    COALESCE(atomic_score, 0) AS atomic_score,
                    COALESCE(tier, 'EDGE') AS tier,
                    COALESCE(tier_label, 'EDGE') AS tier_label,
                    result,
                    COALESCE(signal_velocity, 'NEUTRAL') AS signal_velocity
                FROM dagens_kamp
                WHERE result IS NOT NULL
                ORDER BY kickoff ASC
            """)
            next_row = await conn.fetchrow("""
                SELECT
                    COALESCE(home_team || ' vs ' || away_team, match, '') AS match_name,
                    kickoff AS kickoff_time,
                    odds
                FROM dagens_kamp
                WHERE result IS NULL AND kickoff > NOW() - INTERVAL '2 hours'
                ORDER BY kickoff ASC LIMIT 1
            """)

        START = 1000.0
        bank  = START
        peak  = START
        wins  = 0
        current_streak = 0
        max_streak     = 0
        milestone_levels = [5000, 10000, 25000, 50000]
        triggered: set[int] = set()
        milestones: list[dict] = []
        ladder: list[dict] = []

        for row in settled:
            stake   = round(bank, 2)          # full bankroll is the stake
            result  = (row["result"] or "").upper()
            odds    = float(row["odds"] or 2.0)
            won     = result == "WIN"

            # Reasoning from available signals
            parts: list[str] = []
            vel = row.get("signal_velocity") or ""
            if vel and vel not in ("NEUTRAL", ""):
                parts.append(f"Odds-bevegelse: {vel}")
            ev_val = float(row["soft_ev"] or 0)
            tier   = row["tier"] or "EDGE"
            reasoning = " · ".join(parts) if parts else \
                f"EV +{ev_val:.1f}% oppdaget · OMEGA {tier}-nivå · {int(row['atomic_score'] or 0)}/9 signaler"

            pick_type = "1X2"

            if won:
                bank  = round(stake * odds, 2)   # full compound: stake × odds
                wins += 1
                current_streak += 1
                max_streak = max(max_streak, current_streak)
            else:
                bank = START      # RESET on loss
                current_streak = 0

            bank = max(bank, 0.0)
            peak = max(peak, bank)

            # Milestones
            for level in milestone_levels:
                if level not in triggered and bank >= level:
                    triggered.add(level)
                    milestones.append({
                        "amount":  level,
                        "match":   row["match_name"],
                        "message": f"🎯 {level:,} NOK nådd etter {row['match_name']}!".replace(",", " "),
                    })

            ladder.append({
                "match":          row["match_name"],
                "league":         row["league"] or "",
                "kickoff":        row["kickoff_time"].isoformat() if row["kickoff_time"] else "",
                "odds":           odds,
                "pick_type":      pick_type,
                "atomic_score":   int(row["atomic_score"] or 0),
                "won":            won,
                "stake":          stake,
                "bankroll_after": round(bank),
                "streak":         current_streak if won else 0,
                "reasoning":      reasoning,
                "is_demo":        False,
            })

        # Perfect run: what if every pick won?
        perfect_bank = START
        for step in ladder:
            perfect_bank = round(perfect_bank * step["odds"], 2)

        total = len(ladder)
        hit_rate = round(wins / total * 100, 1) if total > 0 else 0.0
        bot_status = "ACTIVE" if next_row else ("WAITING" if total == 0 else "IDLE")

        next_pick_data = None
        if next_row:
            ko = next_row["kickoff_time"]
            np_odds = float(next_row["odds"] or 0)
            next_pick_data = {
                "match":            next_row["match_name"],
                "kickoff":          ko.isoformat() if ko else "",
                "odds":             np_odds,
                "potential_win":    round(bank * np_odds, 2),
                "potential_profit": round(bank * (np_odds - 1), 2),
            }

        return {
            "ladder":               ladder,
            "current_bankroll":     round(bank),
            "peak_bankroll":        round(peak),
            "max_streak":           max_streak,
            "total_picks":          total,
            "wins":                 wins,
            "hit_rate":             hit_rate,
            "milestones":           milestones,
            "next_pick":            next_pick_data,
            "bot_status":           bot_status,
            "perfect_run_bankroll": round(perfect_bank),
            "perfect_run_text":     f"Hvis boten traff alle: 1 000 → {round(perfect_bank):,} NOK".replace(",", " "),
        }
    except Exception as e:
        logger.error(f"/ladder-history error: {e}")
        return {
            "ladder": [], "current_bankroll": 1000, "peak_bankroll": 1000,
            "max_streak": 0, "total_picks": 0, "wins": 0, "hit_rate": 0.0,
            "milestones": [], "next_pick": None, "bot_status": "ERROR",
        }

@app.post("/admin/seed-ladder")
async def admin_seed_ladder():
    """Re-seed picks_v2 with compound-staking demo picks. Deletes existing demo data first."""
    if not db_state.connected or not db_state.pool:
        return JSONResponse(status_code=503, content={"error": "DB offline"})
    try:
        async with db_state.pool.acquire() as conn:
            deleted = await conn.fetchval(
                "WITH d AS (DELETE FROM picks_v2 WHERE match_name IS NOT NULL RETURNING id) "
                "SELECT COUNT(*) FROM d"
            )

            from datetime import datetime, timezone, timedelta
            now = datetime.now(timezone.utc)

            # Sequence: WIN WIN LOSS WIN WIN LOSS WIN
            # Bankroll: 1000→1850→3885→reset 1000→1650→3135→reset 1000→1780
            demo = [
                ("Bayer Leverkusen vs Dortmund",  "Germany - Bundesliga",
                 now - timedelta(days=14), 1.85, 8.4, 7.9, 7, "EDGE",     "💪 EDGE",       "WIN"),
                ("Inter Milan vs Napoli",          "Italy - Serie A",
                 now - timedelta(days=11), 2.10, 9.1, 8.6, 8, "EDGE",     "💪 EDGE",       "WIN"),
                ("Atletico Madrid vs Sevilla",     "Spain - LaLiga",
                 now - timedelta(days=9),  1.72, 7.3, 6.9, 6, "MONITORED","📊 MONITORED",  "LOSS"),
                ("PSG vs Lyon",                    "France - Ligue 1",
                 now - timedelta(days=7),  1.65, 8.8, 8.2, 7, "EDGE",     "💪 EDGE",       "WIN"),
                ("Liverpool vs Aston Villa",       "England - Premier League",
                 now - timedelta(days=5),  1.90, 9.6, 9.1, 9, "ATOMIC",   "⚡ ATOMIC",     "WIN"),
                ("Marseille vs Nice",              "France - Ligue 1",
                 now - timedelta(days=3),  2.05, 7.8, 7.3, 6, "MONITORED","📊 MONITORED",  "LOSS"),
                ("Bayern München vs Stuttgart",    "Germany - Bundesliga",
                 now - timedelta(days=1),  1.78, 9.8, 9.2, 9, "ATOMIC",   "⚡ ATOMIC",     "WIN"),
            ]

            inserted = 0
            for match_name, league, kickoff, odds, edge, ev, atomic, tier, tier_label, result in demo:
                home, away = (match_name.split(" vs ", 1) + ["Unknown"])[:2]
                await conn.execute("""
                    INSERT INTO picks_v2 (
                        match_name, home_team, away_team, league, kickoff_time,
                        odds, soft_edge, soft_ev, atomic_score,
                        tier, tier_label, kelly_multiplier, kelly_stake,
                        result, created_at, posted_at, telegram_posted
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,0.5,2.00,$12,NOW(),NOW(),TRUE)
                """, match_name, home, away, league, kickoff,
                     odds, edge, ev, atomic, tier, tier_label, result)
                inserted += 1

            return {"status": "seeded", "deleted": deleted or 0, "inserted": inserted}
    except Exception as e:
        logger.error(f"/admin/seed-ladder error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)[:300]})


# ── placeholder kept for diff alignment ──
@app.get("/admin/debug-picks-v2")
async def debug_picks_v2():
    """Check actual columns and row count of picks_v2 on the server."""
    if not db_state.connected or not db_state.pool:
        return {"error": "DB offline"}
    async with db_state.pool.acquire() as conn:
        cols = await conn.fetch(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name='picks_v2' AND table_schema='public' ORDER BY ordinal_position"
        )
        count = await conn.fetchval("SELECT COUNT(*) FROM picks_v2")
        count_result = await conn.fetchval("SELECT COUNT(*) FROM picks_v2 WHERE result IS NOT NULL")
        tables = await conn.fetch(
            "SELECT table_name, table_type FROM information_schema.tables "
            "WHERE table_name IN ('picks','picks_v2','picks_v1_backup') AND table_schema='public'"
        )
        return {
            "picks_v2_columns": [{"name": r["column_name"], "type": r["data_type"]} for r in cols],
            "picks_v2_count": count,
            "picks_v2_result_count": count_result,
            "tables": [{"name": r["table_name"], "type": r["table_type"]} for r in tables],
        }


async def _ladder_history_compat():
    return {
            "history": [],
            "milestones": [],
            "next_pick": None,
        }
