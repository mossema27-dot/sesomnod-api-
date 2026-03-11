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

EV_MIN              = 8.0    # Min EV% for å kvalifisere
PINNACLE_EDGE_MIN   = 5.0    # Min edge mot Pinnacle
PINNACLE_MARGIN_MAX = 4.0    # Max Pinnacle margin%
ODDS_MIN            = 1.40
ODDS_MAX            = 6.00
MATCH_HOURS_MAX     = 72
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
    DATABASE_URL: str  = _clean("DATABASE_URL")
    TELEGRAM_TOKEN: str = _clean("TELEGRAM_TOKEN")
    TELEGRAM_CHAT_ID: str = _clean("TELEGRAM_CHAT_ID")
    ODDS_API_KEY: str  = _clean("ODDS_API_KEY")
    PORT: int          = int(os.getenv("PORT", "8000"))
    ENVIRONMENT: str   = os.getenv("RAILWAY_ENVIRONMENT", "development")
    SERVICE_NAME: str  = os.getenv("RAILWAY_SERVICE_NAME", "sesomnod-api")

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
                    date DATE UNIQUE,
                    profit NUMERIC(10,2),
                    num_picks INTEGER,
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
                        "markets": "h2h,totals",
                        "oddsFormat": "decimal",
                        "bookmakers": "pinnacle,bet365,betway,unibet,williamhill,bwin,nordicbet,betsson",
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

            # Consensus bookmaker odds (alle utenom Pinnacle)
            num_bk = len(bookmakers)
            home_list, draw_list, away_list = [], [], []
            over25_list, over35_list = [], []

            for bk in bookmakers:
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

            if not home_list:
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
            if over25_list and ODDS_MIN <= max(over25_list) <= ODDS_MAX:
                p_over25 = min(0.88, max(0.28, 0.35 + (p_home + p_away) * 0.42 - p_draw * 0.15))
                pin_over25 = None
                pin_totals = next(
                    (mkt for mkt in pinnacle_bk.get("markets", []) if mkt["key"] == "totals"),
                    None
                )
                if pin_totals:
                    for o in pin_totals.get("outcomes", []):
                        if abs(o.get("point", 0) - 2.5) < 0.1 and o["name"] == "Over":
                            pin_over25 = o["price"]
                outcomes_to_check.append((p_over25, max(over25_list), "Over 2.5 mål", "totals_over25", pin_over25))
            if over35_list and ODDS_MIN <= max(over35_list) <= ODDS_MAX:
                p_over35 = min(0.65, max(0.10, (p_home + p_away) * 0.38 - p_draw * 0.12))
                pin_over35 = None
                pin_totals = next(
                    (mkt for mkt in pinnacle_bk.get("markets", []) if mkt["key"] == "totals"),
                    None
                )
                if pin_totals:
                    for o in pin_totals.get("outcomes", []):
                        if abs(o.get("point", 0) - 3.5) < 0.1 and o["name"] == "Over":
                            pin_over35 = o["price"]
                outcomes_to_check.append((p_over35, max(over35_list), "Over 3.5 mål", "totals_over35", pin_over35))

            for model_prob, odds_val, pick_label, market_type, pin_odds_ref in outcomes_to_check:
                if match_pick_count >= MAX_PICKS_PER_MATCH:
                    break

                market_prob = 1 / odds_val
                ev_pct = round((model_prob * odds_val - 1) * 100, 2)
                edge_pct = round((model_prob - market_prob) * 100, 2)

                # Pinnacle-basert edge
                if pin_odds_ref:
                    pin_prob = 1 / pin_odds_ref
                    pin_edge = round((model_prob - pin_prob) * 100, 2)
                else:
                    pin_edge = edge_pct

                if ev_pct < EV_MIN:
                    continue
                if pin_edge < PINNACLE_EDGE_MIN:
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
                    "pin_edge": pin_edge,
                    "ev": ev_pct,
                    "score": score,
                    "num_bookmakers": num_bk,
                    "pinnacle_opening": round(pin_odds_ref, 2) if pin_odds_ref else None,
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
async def track_clv():
    """
    Kjører hvert 30. minutt.
    For picks der kickoff er passert (kamp ferdig), henter Pinnacle-sluttodds
    og beregner CLV = (odds_taken / pinnacle_closing - 1) × 100.
    """
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
    """Manuell trigger for odds-fetching."""
    asyncio.create_task(fetch_all_odds())
    return {"status": "triggered", "message": "fetch_all_odds startet i bakgrunnen"}


@app.post("/run-analysis")
async def trigger_run_analysis():
    """Manuell trigger for analyse-jobb."""
    asyncio.create_task(run_analysis())
    return {"status": "triggered", "message": "run_analysis startet i bakgrunnen"}


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
            "port": cfg.PORT,
            "environment": cfg.ENVIRONMENT,
        },
        "scanner": {
            "leagues": len(SCAN_LEAGUES),
            "ev_min": EV_MIN,
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
