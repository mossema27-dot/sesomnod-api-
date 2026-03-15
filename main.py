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
    {"key": "soccer_epl",                    "name": "Premier League",   "flag": "🏴󠁧󠁢󠁥󠁮󠁧󠁿"},
    {"key": "soccer_spain_la_liga",           "name": "La Liga",          "flag": "🇪🇸"},
    {"key": "soccer_germany_bundesliga",      "name": "Bundesliga",       "flag": "🇩🇪"},
    {"key": "soccer_italy_serie_a",           "name": "Serie A",          "flag": "🇮🇹"},
    {"key": "soccer_france_ligue_one",        "name": "Ligue 1",          "flag": "🇫🇷"},
    {"key": "soccer_uefa_champions_league",   "name": "Champions League", "flag": "🏆"},
    {"key": "soccer_uefa_europa_league",      "name": "Europa League",    "flag": "🇪🇺"},
    {"key": "soccer_netherlands_eredivisie",  "name": "Eredivisie",       "flag": "🇳🇱"},
]

EV_MIN              = float(os.getenv("EV_MIN", "1.5"))        # Konfigurerbar via Railway env
EDGE_MIN            = float(os.getenv("EDGE_MIN", "1.5"))       # Fase 0: min edge mot Pinnacle
CONFIDENCE_MIN      = int(os.getenv("CONFIDENCE_MIN", "65"))    # Fase 0: min confidence
MIN_BOOKMAKERS      = int(os.getenv("MIN_BOOKMAKERS", "3"))     # Fase 0: min antall bookmakers
PINNACLE_EDGE_MIN   = 1.0    # Min edge mot Pinnacle (brukt kun i logging)
PINNACLE_MARGIN_MAX = 4.0    # Max Pinnacle margin%
ODDS_MIN            = float(os.getenv("ODDS_MIN", "1.60"))      # Fase 0: under 1.60 = for lav verdi
ODDS_MAX            = float(os.getenv("ODDS_MAX", "4.50"))      # Fase 0: over 4.50 = for høy varians
MATCH_HOURS_MAX     = int(os.getenv("MATCH_HOURS_MAX", "96"))
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
    ODDS_API_KEY: str    = _clean("ODDS_API_KEY")
    NOTION_TOKEN: str    = _clean("NOTION_TOKEN")
    NOTION_DB_ID: str    = _clean("NOTION_DATABASE_ID")
    PORT: int            = int(os.getenv("PORT", "8000"))
    ENVIRONMENT: str     = os.getenv("RAILWAY_ENVIRONMENT", "development")
    SERVICE_NAME: str    = os.getenv("RAILWAY_SERVICE_NAME", "sesomnod-api")

cfg = Config()


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

                ALTER TABLE daily_summaries ADD COLUMN IF NOT EXISTS trigger_type VARCHAR(50) DEFAULT 'scheduled';
                ALTER TABLE daily_summaries ADD COLUMN IF NOT EXISTS match_id TEXT;
                ALTER TABLE daily_summaries ADD COLUMN IF NOT EXISTS result TEXT;
                ALTER TABLE daily_summaries ADD COLUMN IF NOT EXISTS reason TEXT;
            """)

            # Indeks for snapshot-oppslag
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_odds_snapshots_league_time
                    ON odds_snapshots(league_key, snapshot_time DESC);
                CREATE INDEX IF NOT EXISTS idx_clv_pick_id ON clv_records(pick_id);
                CREATE INDEX IF NOT EXISTS idx_dagens_kamp_kickoff ON dagens_kamp(kickoff);
            """)

        logger.info("[DB] Tabeller OK")
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
# ODDS CACHING (fetch 2x/day only)
# ─────────────────────────────────────────────────────────
async def fetch_all_odds():
    """
    Kjører kl. 07:00 og 14:00 UTC.
    Henter odds fra The Odds API for alle ligaer og lagrer som snapshots i DB.
    Estimert API-forbruk: 8 ligaer × 2/dag × 30 dager = 480 req/mnd.
    """
    logger.info("[OddsCache] fetch_all_odds startet")

    if not cfg.ODDS_API_KEY:
        logger.warning("[OddsCache] ODDS_API_KEY mangler")
        return

    if not db_state.connected or not db_state.pool:
        logger.warning("[OddsCache] DB offline — kan ikke lagre snapshots")
        return

    snap_time = datetime.now(timezone.utc)
    saved = 0

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
                if resp.status_code != 200:
                    logger.warning(f"[OddsCache] {league['key']}: HTTP {resp.status_code}")
                    continue

                data = resp.json()
                if not isinstance(data, list):
                    continue

                async with db_state.pool.acquire() as conn:
                    await conn.execute("""
                        INSERT INTO odds_snapshots (league_key, snapshot_time, data)
                        VALUES ($1, $2, $3)
                    """, league["key"], snap_time, json.dumps(data))

                saved += 1
                logger.info(f"[OddsCache] {league['name']}: {len(data)} kamper lagret")

            except Exception as e:
                logger.warning(f"[OddsCache] Feil for {league['key']}: {e}")
                continue

    logger.info(f"[OddsCache] Ferdig — {saved}/{len(SCAN_LEAGUES)} ligaer cachet kl. {snap_time.strftime('%H:%M')} UTC")


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


async def _analyse_snapshot(league: dict, matches: list, now: datetime) -> list:
    """
    Analyserer en liste med kamper fra snapshot.
    Bruker Pinnacle som sharp reference.
    SCORE = EV_pct × log(bookmaker_count + 1)
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

            # Finn Pinnacle
            pinnacle_bk = next(
                (bk for bk in bookmakers if bk.get("key") == "pinnacle"),
                None
            )
            if not pinnacle_bk:
                continue

            # Pinnacle h2h odds
            pin_h2h = next(
                (mkt for mkt in pinnacle_bk.get("markets", []) if mkt["key"] == "h2h"),
                None
            )
            if not pin_h2h:
                continue

            pin_outcomes = {o["name"]: o["price"] for o in pin_h2h.get("outcomes", [])}
            pin_home = pin_outcomes.get(m["home_team"])
            pin_away = pin_outcomes.get(m["away_team"])
            pin_draw = pin_outcomes.get("Draw")

            if not pin_home or not pin_away or not pin_draw:
                continue

            p_home, p_draw, p_away, pin_margin = _pinnacle_no_vig(pin_home, pin_draw, pin_away)

            if pin_margin > PINNACLE_MARGIN_MAX:
                continue

            # Soft book odds — Pinnacle EKSKLUDERT (vi bruker Pinnacle kun som referanse)
            num_bk = len(bookmakers)
            home_list, draw_list, away_list = [], [], []
            over25_list, over35_list = [], []
            # spreads: {point: [home_price, ...]} og {point: [away_price, ...]}
            spreads_home: dict = {}
            spreads_away: dict = {}

            for bk in bookmakers:
                if bk.get("key") == "pinnacle":
                    continue  # Pinnacle er referanse, ikke betting-target
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

            outcomes_to_check = []
            if best_home and ODDS_MIN <= best_home <= ODDS_MAX:
                outcomes_to_check.append((p_home, best_home, f"{m['home_team']} vinner", "h2h", pin_home))
            if best_draw and ODDS_MIN <= best_draw <= ODDS_MAX:
                outcomes_to_check.append((p_draw, best_draw, "Uavgjort", "h2h", pin_draw))
            if best_away and ODDS_MIN <= best_away <= ODDS_MAX:
                outcomes_to_check.append((p_away, best_away, f"{m['away_team']} vinner", "h2h", pin_away))
            # Over 2.5 — bruk Pinnacle no-vig som model_prob (ikke Dixon-Coles formel)
            if over25_list and ODDS_MIN <= max(over25_list) <= ODDS_MAX:
                pin_totals = next(
                    (mkt for mkt in pinnacle_bk.get("markets", []) if mkt["key"] == "totals"),
                    None
                )
                pin_over25 = pin_under25 = None
                if pin_totals:
                    for o in pin_totals.get("outcomes", []):
                        if abs(o.get("point", 0) - 2.5) < 0.1:
                            if o["name"] == "Over":
                                pin_over25 = o["price"]
                            elif o["name"] == "Under":
                                pin_under25 = o["price"]
                if pin_over25 and pin_under25:
                    raw_o, raw_u = 1 / pin_over25, 1 / pin_under25
                    p_over25 = raw_o / (raw_o + raw_u)
                    outcomes_to_check.append((p_over25, max(over25_list), "Over 2.5 mål", "totals_over25", pin_over25))

            # Over 3.5 — bruk Pinnacle no-vig som model_prob
            if over35_list and ODDS_MIN <= max(over35_list) <= ODDS_MAX:
                pin_totals = next(
                    (mkt for mkt in pinnacle_bk.get("markets", []) if mkt["key"] == "totals"),
                    None
                )
                pin_over35 = pin_under35 = None
                if pin_totals:
                    for o in pin_totals.get("outcomes", []):
                        if abs(o.get("point", 0) - 3.5) < 0.1:
                            if o["name"] == "Over":
                                pin_over35 = o["price"]
                            elif o["name"] == "Under":
                                pin_under35 = o["price"]
                if pin_over35 and pin_under35:
                    raw_o, raw_u = 1 / pin_over35, 1 / pin_under35
                    p_over35 = raw_o / (raw_o + raw_u)
                    outcomes_to_check.append((p_over35, max(over35_list), "Over 3.5 mål", "totals_over35", pin_over35))

            # Spreads / Asian Handicap — samme EV-formel som totals
            pin_spreads = next(
                (mkt for mkt in pinnacle_bk.get("markets", []) if mkt["key"] == "spreads"),
                None
            )
            if pin_spreads and (spreads_home or spreads_away):
                pin_sp_map: dict = {}  # {point: {"home": price, "away": price}}
                for o in pin_spreads.get("outcomes", []):
                    pt  = round(o.get("point", 0), 1)
                    nm  = o.get("name", "")
                    prc = o.get("price")
                    if not prc:
                        continue
                    pin_sp_map.setdefault(pt, {})
                    if nm == m["home_team"]:
                        pin_sp_map[pt]["home"] = prc
                    elif nm == m["away_team"]:
                        pin_sp_map[pt]["away"] = prc

                for pt, pin_sides in pin_sp_map.items():
                    pin_sp_home = pin_sides.get("home")
                    pin_sp_away = pin_sides.get("away")
                    if not pin_sp_home or not pin_sp_away:
                        continue
                    # Pinnacle no-vig probability for hver side
                    raw_h = 1 / pin_sp_home
                    raw_a = 1 / pin_sp_away
                    total = raw_h + raw_a
                    p_sp_home = raw_h / total
                    p_sp_away = raw_a / total

                    best_sp_home = max(spreads_home[pt]) if pt in spreads_home else None
                    best_sp_away = max(spreads_away.get(-pt, [])) if -pt in spreads_away else None

                    label_pt = f"+{pt}" if pt > 0 else str(pt)
                    if best_sp_home and ODDS_MIN <= best_sp_home <= ODDS_MAX:
                        outcomes_to_check.append((
                            p_sp_home, best_sp_home,
                            f"{m['home_team']} handicap {label_pt}", "spreads", pin_sp_home
                        ))
                    neg_pt = -pt
                    neg_label = f"+{neg_pt}" if neg_pt > 0 else str(neg_pt)
                    if best_sp_away and ODDS_MIN <= best_sp_away <= ODDS_MAX:
                        outcomes_to_check.append((
                            p_sp_away, best_sp_away,
                            f"{m['away_team']} handicap {neg_label}", "spreads", pin_sp_away
                        ))

            for model_prob, odds_val, pick_label, market_type, pin_odds_ref in outcomes_to_check:
                if match_pick_count >= MAX_PICKS_PER_MATCH:
                    break

                market_prob = 1 / odds_val
                # EV: model_prob (Pinnacle no-vig) vs beste soft book odds
                ev_pct = round((model_prob * odds_val - 1) * 100, 2)
                # Edge: prosentpoeng over soft book implied probability
                edge_pct = round((model_prob - market_prob) * 100, 2)
                # Pin fair odds (for logging/CLV — ikke som filter)
                pin_fair_odds = round(1 / model_prob, 3) if model_prob > 0 else None

                if ev_pct < EV_MIN:
                    continue

                # Fase 0 gate: edge og confidence
                if edge_pct < EDGE_MIN:
                    continue
                if 75 < CONFIDENCE_MIN:
                    continue

                score = round(ev_pct * math.log(num_bk + 1), 4)

                candidates.append({
                    "league_key": league["key"],
                    "league": league["name"],
                    "league_flag": league["flag"],
                    "home_team": m["home_team"],
                    "away_team": m["away_team"],
                    "commence_time": m["commence_time"],
                    "hours_to_kickoff": round(hours, 1),
                    "pick": pick_label,
                    "odds": odds_val,
                    "market_type": market_type,
                    "model_prob": round(model_prob * 100, 2),
                    "market_prob": round(market_prob * 100, 2),
                    "edge": edge_pct,
                    "ev": ev_pct,
                    "score": score,
                    "num_bookmakers": num_bk,
                    "pinnacle_opening": round(pin_odds_ref, 2) if pin_odds_ref else None,
                    "pinnacle_fair_odds": pin_fair_odds,
                    "pinnacle_margin": round(pin_margin, 2),
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
                picks = await _analyse_snapshot(league, matches, now)
                candidates.extend(picks)

            except Exception as e:
                logger.warning(f"[Analyse] Feil for {league['key']}: {e}")
                continue

    if not candidates:
        logger.info("[Analyse] Ingen kvalifiserte picks denne runden")
        return

    # Sorter etter SCORE desc
    candidates.sort(key=lambda x: x["score"], reverse=True)
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

            row_id = await conn.fetchval("""
                INSERT INTO dagens_kamp
                    (match, league, home_team, away_team, pick, odds, stake,
                     edge, ev, confidence, kickoff, telegram_posted,
                     market_type, score, bookmaker_count, pinnacle_opening, total_scanned)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,FALSE,$12,$13,$14,$15,$16)
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
            )
            newly_inserted.append({"id": row_id, **pick, "total_scanned": total_scanned})
            logger.info(f"[Analyse] Ny pick (id={row_id}): {pick['pick']} @ {pick['odds']} SCORE={pick['score']}")

    if not newly_inserted:
        logger.info("[Analyse] Ingen nye picks (alle allerede i DB)")
        return

    if not cfg.TELEGRAM_TOKEN or not cfg.TELEGRAM_CHAT_ID:
        logger.warning("[Analyse] TELEGRAM mangler — lagret men ikke postet")
        return

    posts_left = max(0, DAILY_POST_LIMIT - int(daily_posted))
    rank = 1
    for pick in newly_inserted[:posts_left]:
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
                logger.info(f"[Analyse] Postet til Telegram: {pick['pick']} — {pick['home_team']} vs {pick['away_team']}")
            else:
                logger.error(f"[Analyse] Telegram feil {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            logger.exception(f"[Analyse] Feil ved posting id={pick['id']}: {e}")
        rank += 1

    skipped = len(newly_inserted) - posts_left
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
                picks = await _analyse_snapshot(league, [m], now)

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
                    all_raw = await _analyse_snapshot(league, [m], now)
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
def _format_pick_message(pick: dict, rank: int = 1, total_scanned: int = 0) -> str:
    kickoff = pick.get("kickoff")
    if kickoff:
        if isinstance(kickoff, str):
            kickoff = datetime.fromisoformat(kickoff.replace("Z", "+00:00"))
        cet = kickoff + timedelta(hours=1)
        dato = cet.strftime("%-d. %b")
        tid = cet.strftime("%H:%M")
    elif pick.get("commence_time"):
        kickoff_dt = datetime.fromisoformat(pick["commence_time"].replace("Z", "+00:00"))
        cet = kickoff_dt + timedelta(hours=1)
        dato = cet.strftime("%-d. %b")
        tid = cet.strftime("%H:%M")
    else:
        dato, tid = "–", "–"

    if pick.get("league_flag") and pick.get("league"):
        league = f"{pick['league_flag']} {pick['league']}"
    else:
        league = pick.get("league", "–")

    home_team  = pick.get("home_team") or pick.get("match", "–").split(" vs ")[0]
    away_team  = pick.get("away_team") or pick.get("match", "–").split(" vs ")[-1]
    odds_val   = float(pick.get("odds") or 0)
    edge_val   = float(pick.get("edge") or 0)
    ev_val     = float(pick.get("ev") or 0)
    score_val  = float(pick.get("score") or 0)
    num_bk     = pick.get("num_bookmakers") or pick.get("bookmaker_count") or 0
    pick_label = pick.get("pick", "–")
    market     = pick.get("market_type") or "h2h"
    scan_count = pick.get("total_scanned") or total_scanned or 0

    market_display = {
        "h2h": "1X2",
        "totals_over25": "Over 2.5",
        "totals_over35": "Over 3.5",
    }.get(market, market.upper())

    return (
        "⚡ SESOMNOD ENGINE\n"
        "Football Decision Intelligence\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🏆 {league} · MATCH BRIEF\n\n"
        f"🏟 {home_team}\n"
        "         VS\n"
        f"       {away_team}\n\n"
        f"🕒 {dato} · {tid} CET\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "📈 EDGE ANALYSE\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Market:    {market_display}\n"
        f"Odds:      {odds_val}\n"
        f"Edge:      +{edge_val}%\n"
        f"EV:        +{ev_val}%\n"
        f"SCORE:     {score_val:.2f}\n"
        f"Books:     {num_bk}\n"
        f"Scan:      {scan_count} kamper analysert\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "🎯 MODEL DECISION\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"PICK:    {pick_label}\n"
        f"ODDS:    @ {odds_val}\n"
        f"RANK:    #{rank} av dagen\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        "You don't get picks. You get control. ⚡\n"
        "SesomNod Engine"
    )


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
            daily_posted = await conn.fetchval("""
                SELECT COUNT(*) FROM dagens_kamp
                WHERE telegram_posted = TRUE AND timestamp >= $1
            """, today_start)

            posts_left = max(0, DAILY_POST_LIMIT - int(daily_posted))
            if posts_left == 0:
                logger.info(f"[Scheduler] Daglig grense ({DAILY_POST_LIMIT}) nådd")
                return

            rows = await conn.fetch("""
                SELECT * FROM dagens_kamp
                WHERE telegram_posted = FALSE AND timestamp >= $1
                ORDER BY score DESC NULLS LAST, ev DESC NULLS LAST
                LIMIT $2
            """, today_start, posts_left)

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

    # Hent odds 2x/dag (07:00 + 14:00 UTC)
    scheduler.add_job(
        fetch_all_odds,
        trigger=CronTrigger(hour="7,14", minute=0, timezone="UTC"),
        id="fetch_all_odds",
        misfire_grace_time=300,
        replace_existing=True,
    )

    # Analyser 3x/dag (07:05 + 14:05 + 20:00 UTC)
    scheduler.add_job(
        run_analysis,
        trigger=CronTrigger(hour="7,14", minute=5, timezone="UTC"),
        id="run_analysis_morning_afternoon",
        misfire_grace_time=300,
        replace_existing=True,
    )
    scheduler.add_job(
        run_analysis,
        trigger=CronTrigger(hour=20, minute=0, timezone="UTC"),
        id="run_analysis_evening",
        misfire_grace_time=300,
        replace_existing=True,
    )

    # Post 09:00 UTC (legacy-jobb)
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

    scheduler.start()
    logger.info(
        "[Scheduler] Startet — "
        "Odds: 07:00+14:00 | Analyse: 07:05+14:05+20:00 | "
        "CLV: hvert 30min | CLV-rapport: man 08:00 UTC"
    )

    if db_state.connected:
        logger.info("[APP] SesomNod Engine KLAR! (FULL MODE)")
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
    version="9.0.0",
    lifespan=lifespan,
)

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
            "version": "9.0.0-clv",
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
        "version": "9.0.0-clv",
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


@app.get("/picks")
async def get_picks():
    if not db_state.connected or not db_state.pool:
        return JSONResponse(status_code=503, content={"status": "offline", "data": [], "error": "Database ikke tilgjengelig"})
    try:
        async with db_state.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM picks ORDER BY timestamp DESC LIMIT 100")
        return {"status": "ok", "data": [dict(r) for r in rows], "count": len(rows)}
    except Exception as e:
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
                picks = await _analyse_snapshot(league, matches, now)
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
                picks = await _analyse_snapshot(league, matches, now)
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


@app.post("/post-telegram")
async def trigger_post_telegram():
    """Poster beste upostede HIGH/MEDIUM confidence pick til Telegram."""
    if not cfg.TELEGRAM_TOKEN or not cfg.TELEGRAM_CHAT_ID:
        return JSONResponse(status_code=503, content={"status": "error", "error": "TELEGRAM ikke konfigurert"})
    if not db_state.connected or not db_state.pool:
        return JSONResponse(status_code=503, content={"status": "error", "error": "DB offline"})

    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    async with db_state.pool.acquire() as conn:
        daily_posted = await conn.fetchval("""
            SELECT COUNT(*) FROM dagens_kamp
            WHERE telegram_posted = TRUE AND timestamp >= $1
        """, today_start)

        if int(daily_posted) >= DAILY_POST_LIMIT:
            return {"status": "skipped", "reason": f"Daglig grense ({DAILY_POST_LIMIT}) nådd", "posted_today": int(daily_posted)}

        row = await conn.fetchrow("""
            SELECT * FROM dagens_kamp
            WHERE telegram_posted = FALSE AND timestamp >= $1
            ORDER BY score DESC NULLS LAST, ev DESC NULLS LAST
            LIMIT 1
        """, today_start)

    if not row:
        return {"status": "skipped", "reason": "Ingen upostede picks i dag"}

    pick_data = dict(row)
    already_posted = int(daily_posted)
    rank = already_posted + 1

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
            return {
                "status": "posted",
                "pick_id": pick_data["id"],
                "pick": pick_data.get("pick"),
                "match": pick_data.get("match"),
                "odds": float(pick_data.get("odds") or 0),
                "ev": float(pick_data.get("ev") or 0),
                "score": float(pick_data.get("score") or 0),
                "telegram_status": resp.status_code,
            }
        else:
            return JSONResponse(status_code=502, content={
                "status": "error",
                "telegram_status": resp.status_code,
                "detail": resp.text[:200],
            })
    except Exception as e:
        logger.exception(f"[/post-telegram] Feil: {e}")
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


@app.post("/add-pick")
async def add_pick(payload: dict):
    """Inserter en pick direkte i dagens_kamp og logger til Notion."""
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
                float(payload.get("edge", 0)),
                float(payload.get("ev_pct", 0)),
                int(payload.get("confidence", 0)),
                kickoff_dt,
                payload.get("market_type", "h2h"),
                float(payload.get("score", 0)) if payload.get("score") else None,
                int(payload.get("bookmaker_count", 0)) if payload.get("bookmaker_count") else None,
                float(payload.get("pinnacle_opening", 0)) if payload.get("pinnacle_opening") else None,
            )
        pick_data = {**payload, "id": row_id, "kickoff": kickoff_dt}
        await _log_notion_pick(pick_data)
        return {"status": "ok", "id": row_id, "match": f"{payload['home_team']} vs {payload['away_team']}"}
    except Exception as e:
        logger.exception(f"[/add-pick] Feil: {e}")
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
    return {
        "service": cfg.SERVICE_NAME,
        "version": "9.0.0-clv",
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
            "ev_min": EV_MIN,
            "ev_min_source": "env" if os.getenv("EV_MIN") else "default",
            "edge_min": EDGE_MIN,
            "edge_min_source": "env" if os.getenv("EDGE_MIN") else "default",
            "confidence_min": CONFIDENCE_MIN,
            "min_bookmakers": MIN_BOOKMAKERS,
            "odds_min": ODDS_MIN,
            "odds_max": ODDS_MAX,
            "pinnacle_edge_min": PINNACLE_EDGE_MIN,
            "daily_post_limit": DAILY_POST_LIMIT,
            "api_fetches_per_day": 2,
            "estimated_api_calls_per_month": len(SCAN_LEAGUES) * 2 * 30,
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


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
