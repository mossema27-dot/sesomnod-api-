"""
SesomNod Engine v8.0 — "RAILWAY EDITION"
==========================================
GARANTIER:
- Starter alltid
- Stopper ALDRI av seg selv
- /health returnerer alltid 200
- DB-feil er ufarlig (Graceful Degradation)
- Bruker asyncpg direkte mot Railway PostgreSQL
- Ingen Supabase DNS-avhengighet
"""

# ─────────────────────────────────────────────────────────
# KRITISK: Patch sys.exit FØR noen imports
# ─────────────────────────────────────────────────────────
import sys
import os

_original_exit = sys.exit
def _safe_exit(code=0):
    import logging
    logging.getLogger("sesomnod").critical(
        f"[FORTRESS] sys.exit({code}) blokkert! Appen fortsetter."
    )
sys.exit = _safe_exit

# ─────────────────────────────────────────────────────────
# IMPORTS
# ─────────────────────────────────────────────────────────
import asyncio
import logging
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

# ─────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("sesomnod")

# ─────────────────────────────────────────────────────────
# SAFE IMPORT
# ─────────────────────────────────────────────────────────
def _safe_import(module_name: str):
    try:
        import importlib
        mod = importlib.import_module(module_name)
        logger.info(f"[Import] ✅ {module_name} lastet OK")
        return mod
    except Exception as e:
        logger.warning(f"[Import] ⚠️ {module_name} feilet: {e}")
        return None

bankroll_module = _safe_import("bankroll")
dagens_kamp_module = _safe_import("dagens_kamp")
auto_result_module = _safe_import("auto_result")

# ─────────────────────────────────────────────────────────
# SCANNER KONFIGURASJON
# ─────────────────────────────────────────────────────────
SCAN_LEAGUES = [
    {"key": "soccer_epl",                    "name": "Premier League",   "flag": "🏴󠁧󠁢󠁥󠁮󠁧󠁿"},
    {"key": "soccer_spain_la_liga",           "name": "La Liga",          "flag": "🇪🇸"},
    {"key": "soccer_germany_bundesliga",      "name": "Bundesliga",       "flag": "🇩🇪"},
    {"key": "soccer_italy_serie_a",           "name": "Serie A",          "flag": "🇮🇹"},
    {"key": "soccer_france_ligue_one",        "name": "Ligue 1",          "flag": "🇫🇷"},
    {"key": "soccer_uefa_champions_league",   "name": "Champions League", "flag": "🏆"},
]
EDGE_MIN = 8.0
CONFIDENCE_HIGH = 70
DAILY_POST_LIMIT = 5

# ─────────────────────────────────────────────────────────
# KONFIGURASJON
# ─────────────────────────────────────────────────────────
def _clean(key: str) -> str:
    val = os.getenv(key, "")
    if val:
        val = (val.strip()
               .replace("\n", "").replace("\r", "")
               .replace("\t", "").replace("\x00", "")
               .replace("\ufeff", ""))
    return val

class Config:
    DATABASE_URL: str = _clean("DATABASE_URL")
    TELEGRAM_TOKEN: str = _clean("TELEGRAM_TOKEN")
    TELEGRAM_CHAT_ID: str = _clean("TELEGRAM_CHAT_ID")
    ODDS_API_KEY: str = _clean("ODDS_API_KEY")
    PORT: int = int(os.getenv("PORT", "8000"))
    ENVIRONMENT: str = os.getenv("RAILWAY_ENVIRONMENT", "development")
    SERVICE_NAME: str = os.getenv("RAILWAY_SERVICE_NAME", "sesomnod-api")

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
    """Opprett asyncpg connection pool mot Railway PostgreSQL."""
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
        logger.info("[DB] ✅ Tilkoblet Railway PostgreSQL!")

        await ensure_tables(pool)
        return True

    except Exception as e:
        err = str(e)[:200]
        await db_state.mark_fail(err)
        logger.warning(f"[DB] Offline — {err}")
        return False


async def ensure_tables(pool: asyncpg.Pool):
    """Opprett nødvendige tabeller hvis de ikke finnes."""
    try:
        async with pool.acquire() as conn:
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
            """)

            # Migrer eksisterende dagens_kamp tabell med nye kolonner
            await conn.execute("""
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS league TEXT;
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS home_team TEXT;
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS away_team TEXT;
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS edge NUMERIC(6,2);
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS ev NUMERIC(6,2);
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS confidence INTEGER;
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS kickoff TIMESTAMPTZ;
                ALTER TABLE dagens_kamp ADD COLUMN IF NOT EXISTS telegram_posted BOOLEAN DEFAULT FALSE;
            """)

        logger.info("[DB] ✅ Tabeller OK")
    except Exception as e:
        logger.warning(f"[DB] Tabell-feil: {e}")


async def reconnect_loop():
    """Bakgrunns-loop som prøver å reconnecte hvis DB er offline."""
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
# MULTI-LEAGUE SCANNER
# ─────────────────────────────────────────────────────────
async def _scan_leagues(odds_api_key: str) -> list:
    """
    Scanner alle SCAN_LEAGUES via Odds API.
    Returnerer kvalifiserte picks sortert etter EV desc.
    Filtreringsregler: edge >= EDGE_MIN OG confidence >= CONFIDENCE_HIGH.
    """
    candidates = []
    now = datetime.now(timezone.utc)

    def _median(lst):
        s = sorted(lst)
        n = len(s)
        return s[n // 2] if n % 2 == 1 else (s[n // 2 - 1] + s[n // 2]) / 2

    async with httpx.AsyncClient(timeout=20) as client:
        for league in SCAN_LEAGUES:
            try:
                resp = await client.get(
                    f"https://api.the-odds-api.com/v4/sports/{league['key']}/odds/",
                    params={
                        "apiKey": odds_api_key,
                        "regions": "eu",
                        "markets": "h2h,totals",
                        "oddsFormat": "decimal",
                    }
                )
                if resp.status_code != 200:
                    logger.warning(f"[Scanner] {league['key']}: HTTP {resp.status_code}")
                    continue

                matches = resp.json()
                if not isinstance(matches, list):
                    continue

                for m in matches:
                    try:
                        commence = datetime.fromisoformat(
                            m["commence_time"].replace("Z", "+00:00")
                        )
                        hours = (commence - now).total_seconds() / 3600
                        if not (6 <= hours <= 96):
                            continue

                        bookmakers = m.get("bookmakers", [])
                        if not bookmakers:
                            continue

                        # Ekstraher consensus odds
                        home_list, draw_list, away_list, over25_list = [], [], [], []
                        for bk in bookmakers:
                            for mkt in bk.get("markets", []):
                                if mkt["key"] == "h2h":
                                    for o in mkt.get("outcomes", []):
                                        if o["name"] == "Draw":
                                            draw_list.append(o["price"])
                                        elif len(home_list) <= len(away_list):
                                            home_list.append(o["price"])
                                        else:
                                            away_list.append(o["price"])
                                elif mkt["key"] == "totals":
                                    for o in mkt.get("outcomes", []):
                                        if abs(o.get("point", 0) - 2.5) < 0.1 and o["name"] == "Over":
                                            over25_list.append(o["price"])

                        if not home_list:
                            continue

                        ho = round(_median(home_list), 3)
                        dr = round(_median(draw_list), 3) if draw_list else 3.4
                        aw = round(_median(away_list), 3) if away_list else 3.5

                        # Remove vig → true probabilities
                        raw_h, raw_d, raw_a = 1 / ho, 1 / dr, 1 / aw
                        total = raw_h + raw_d + raw_a
                        p_home = raw_h / total
                        p_draw = raw_d / total
                        p_away = raw_a / total

                        num_bk = len(bookmakers)

                        # Over 2.5 sannsynlighet (Dixon-Coles approx)
                        decisive = p_home + p_away
                        p_over25 = min(0.88, max(0.28, 0.35 + decisive * 0.42 - p_draw * 0.15))

                        outcomes_to_check = [
                            (p_home, ho, f"{m['home_team']} vinner"),
                            (p_draw, dr, "Uavgjort"),
                            (p_away, aw, f"{m['away_team']} vinner"),
                        ]
                        if over25_list:
                            outcomes_to_check.append(
                                (p_over25, round(_median(over25_list), 3), "Over 2.5 mål")
                            )

                        for model_prob, odds_val, pick_label in outcomes_to_check:
                            market_prob = 1 / odds_val
                            edge = round((model_prob - market_prob) * 100, 2)
                            ev = round((model_prob * odds_val - 1) * 100, 2)

                            if edge < EDGE_MIN:
                                continue

                            # Confidence score
                            conf = 50
                            conf += min(15, max(p_home, p_away) * 20)
                            if ev > 5:
                                conf += 15
                            elif ev > 2:
                                conf += 8
                            if num_bk >= 10:
                                conf += 10
                            elif num_bk >= 5:
                                conf += 5
                            if 12 <= hours <= 48:
                                conf += 5
                            conf = min(99, max(45, int(conf)))

                            if conf < CONFIDENCE_HIGH:
                                continue

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
                                "model_prob": round(model_prob * 100, 2),
                                "market_prob": round(market_prob * 100, 2),
                                "edge": edge,
                                "ev": ev,
                                "confidence": conf,
                                "num_bookmakers": num_bk,
                            })

                    except Exception as e:
                        logger.warning(f"[Scanner] Match-feil i {league['key']}: {e}")
                        continue

            except Exception as e:
                logger.warning(f"[Scanner] Liga-feil {league['key']}: {e}")
                continue

    candidates.sort(key=lambda x: x["ev"], reverse=True)
    return candidates


# ─────────────────────────────────────────────────────────
# TELEGRAM MESSAGE FORMATTER
# ─────────────────────────────────────────────────────────
def _format_pick_message(pick: dict) -> str:
    """Bygger Telegram-melding fra pick-dict (DB-rad eller scanner-resultat)."""
    kickoff = pick.get("kickoff")
    if kickoff:
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

    home_team = pick.get("home_team") or pick.get("match", "–").split(" vs ")[0]
    away_team = pick.get("away_team") or pick.get("match", "–").split(" vs ")[-1]
    odds_val = float(pick.get("odds") or 0)
    edge_val = float(pick.get("edge") or 0)
    ev_val = float(pick.get("ev") or 0)
    confidence_val = pick.get("confidence") or 0
    pick_label = pick.get("pick", "–")

    return (
        "⚡ SESOMNOD ENGINE\n"
        "Football Decision Intelligence\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🏆 {league} · MATCH BRIEF\n\n"
        f"🏟 {home_team}\n"
        "         VS\n"
        f"       {away_team}\n\n"
        f"🕒 {dato} · {tid}\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "📈 EDGE ANALYSE\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Odds:      {odds_val}\n"
        f"Edge:      +{edge_val}% ✅\n"
        f"EV:        +{ev_val}% 🔥\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "🎯 MODEL DECISION\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"PICK:        {pick_label}\n"
        f"ODDS:        @ {odds_val}\n"
        f"CONFIDENCE:  {confidence_val}%\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        "You don't get picks. You get control. ⚡\n"
        "SesomNod Engine"
    )


# ─────────────────────────────────────────────────────────
# SCHEDULER JOBS
# ─────────────────────────────────────────────────────────
async def scan_og_lagre_pick():
    """Kjører kl. 08:45 UTC — scanner alle ligaer og lagrer beste pick i dagens_kamp."""
    logger.info("[Scheduler] scan_og_lagre_pick startet")

    if not cfg.ODDS_API_KEY:
        logger.warning("[Scheduler] ODDS_API_KEY mangler — hopper over")
        return

    try:
        candidates = await _scan_leagues(cfg.ODDS_API_KEY)

        if not candidates:
            logger.info("[Scheduler] Ingen kvalifiserte picks (edge >= 8% og confidence >= HIGH) — poster ikke")
            return

        best = candidates[0]
        logger.info(
            f"[Scheduler] Beste pick: {best['pick']} @ {best['odds']} "
            f"— EV={best['ev']}% Edge={best['edge']}% Conf={best['confidence']}%"
        )

        if not db_state.connected or not db_state.pool:
            logger.warning("[Scheduler] DB offline — kan ikke lagre pick")
            return

        kickoff_dt = datetime.fromisoformat(best["commence_time"].replace("Z", "+00:00"))

        async with db_state.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO dagens_kamp
                    (match, league, home_team, away_team, pick, odds, stake, edge, ev, confidence, kickoff)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            """,
                f"{best['home_team']} vs {best['away_team']}",
                f"{best['league_flag']} {best['league']}",
                best["home_team"],
                best["away_team"],
                best["pick"],
                best["odds"],
                5.0,
                best["edge"],
                best["ev"],
                best["confidence"],
                kickoff_dt,
            )

        logger.info("[Scheduler] Beste pick lagret i dagens_kamp tabellen")

    except Exception as e:
        logger.exception(f"[Scheduler] Uventet feil i scan_og_lagre_pick: {e}")


async def scan_lagre_og_post_alle():
    """
    Kjører kl 07, 09, 11, 13, 15, 17, 19 UTC.
    Lagrer ALLE kvalifiserte picks med deduplication og poster nye til Telegram.
    Maks DAILY_POST_LIMIT posts per dag.
    """
    logger.info("[Scheduler] scan_lagre_og_post_alle startet")

    if not cfg.ODDS_API_KEY:
        logger.warning("[Scheduler] ODDS_API_KEY mangler — hopper over")
        return

    if not db_state.connected or not db_state.pool:
        logger.warning("[Scheduler] DB offline — hopper over")
        return

    try:
        candidates = await _scan_leagues(cfg.ODDS_API_KEY)
        if not candidates:
            logger.info("[Scheduler] Ingen kvalifiserte picks denne runden")
            return

        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        newly_inserted = []

        async with db_state.pool.acquire() as conn:
            daily_posted = await conn.fetchval("""
                SELECT COUNT(*) FROM dagens_kamp
                WHERE telegram_posted = TRUE AND timestamp >= $1
            """, today_start)

            for pick in candidates:
                kickoff_dt = datetime.fromisoformat(pick["commence_time"].replace("Z", "+00:00"))

                # Deduplication: hopp over hvis pick allerede finnes
                exists = await conn.fetchval("""
                    SELECT 1 FROM dagens_kamp
                    WHERE home_team = $1 AND away_team = $2 AND kickoff = $3
                    LIMIT 1
                """, pick["home_team"], pick["away_team"], kickoff_dt)

                if exists:
                    continue

                row_id = await conn.fetchval("""
                    INSERT INTO dagens_kamp
                        (match, league, home_team, away_team, pick, odds, stake,
                         edge, ev, confidence, kickoff, telegram_posted)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, FALSE)
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
                    pick["confidence"],
                    kickoff_dt,
                )
                newly_inserted.append({"id": row_id, **pick})
                logger.info(f"[Scheduler] Ny pick lagret (id={row_id}): {pick['pick']} — {pick['home_team']} vs {pick['away_team']}")

        if not newly_inserted:
            logger.info("[Scheduler] Ingen nye picks å lagre (alle allerede i DB)")
            return

        if not cfg.TELEGRAM_TOKEN or not cfg.TELEGRAM_CHAT_ID:
            logger.warning("[Scheduler] TELEGRAM_TOKEN/CHAT_ID mangler — lagret men ikke postet")
            return

        posts_left = max(0, DAILY_POST_LIMIT - int(daily_posted))
        for pick in newly_inserted[:posts_left]:
            try:
                message = _format_pick_message(pick)
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
                    logger.info(f"[Scheduler] Postet til Telegram: {pick['pick']} — {pick['home_team']} vs {pick['away_team']}")
                else:
                    logger.error(f"[Scheduler] Telegram feil {resp.status_code}: {resp.text[:200]}")
            except Exception as e:
                logger.exception(f"[Scheduler] Feil ved posting av pick id={pick['id']}: {e}")

        skipped = len(newly_inserted) - posts_left
        if skipped > 0:
            logger.info(f"[Scheduler] {skipped} pick(s) lagret men ikke postet — daglig grense ({DAILY_POST_LIMIT}) nådd")

    except Exception as e:
        logger.exception(f"[Scheduler] Uventet feil i scan_lagre_og_post_alle: {e}")


async def post_dagens_kamp_telegram():
    """Kjører kl. 09:00 UTC — poster upostede picks fra dagens_kamp (maks DAILY_POST_LIMIT per dag)."""
    logger.info("[Scheduler] post_dagens_kamp_telegram startet")

    if not cfg.TELEGRAM_TOKEN or not cfg.TELEGRAM_CHAT_ID:
        logger.warning("[Scheduler] TELEGRAM_TOKEN/CHAT_ID mangler — hopper over")
        return

    if not db_state.connected or not db_state.pool:
        logger.warning("[Scheduler] DB offline — kan ikke lese dagens pick")
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
                logger.info(f"[Scheduler] Daglig grense ({DAILY_POST_LIMIT}) nådd — poster ikke")
                return

            rows = await conn.fetch("""
                SELECT * FROM dagens_kamp
                WHERE telegram_posted = FALSE AND timestamp >= $1
                ORDER BY ev DESC NULLS LAST
                LIMIT $2
            """, today_start, posts_left)

        if not rows:
            logger.info("[Scheduler] Ingen upostede picks i dag — poster ikke")
            return

        for row in rows:
            pick_data = dict(row)
            try:
                message = _format_pick_message(pick_data)
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
                    logger.info(f"[Scheduler] Postet til Telegram (09:00): {pick_data.get('pick')} — {pick_data.get('match')}")
                else:
                    logger.error(f"[Scheduler] Telegram feil {resp.status_code}: {resp.text[:200]}")
            except Exception as e:
                logger.exception(f"[Scheduler] Feil ved posting av pick id={pick_data.get('id')}: {e}")

    except Exception as e:
        logger.exception(f"[Scheduler] Uventet feil: {e}")


# ─────────────────────────────────────────────────────────
# LIFESPAN
# ─────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("=" * 60)
    logger.info(f"  🚀 {cfg.SERVICE_NAME} v8.0 RAILWAY EDITION starter...")
    logger.info(f"  Miljø:   {cfg.ENVIRONMENT}")
    logger.info(f"  Port:    {cfg.PORT}")
    logger.info(f"  DB:      {'Satt ✅' if cfg.DATABASE_URL else 'MANGLER ❌'}")
    logger.info("=" * 60)

    await connect_db()
    reconnect_task = asyncio.create_task(reconnect_loop())

    # ── SCHEDULER ──────────────────────────────────────────────
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        scan_og_lagre_pick,
        trigger=CronTrigger(hour=8, minute=45, timezone="UTC"),
        id="scan_og_lagre_pick",
        misfire_grace_time=300,
        replace_existing=True,
    )
    scheduler.add_job(
        post_dagens_kamp_telegram,
        trigger=CronTrigger(hour=9, minute=0, timezone="UTC"),
        id="post_dagens_kamp",
        misfire_grace_time=300,
        replace_existing=True,
    )
    scheduler.add_job(
        scan_lagre_og_post_alle,
        trigger=CronTrigger(hour="7,9,11,13,15,17,19", minute=0, timezone="UTC"),
        id="scan_lagre_og_post_alle",
        misfire_grace_time=300,
        replace_existing=True,
    )
    scheduler.start()
    logger.info(
        "[Scheduler] AsyncIOScheduler startet — "
        "Scanner kl. 08:45 UTC | Telegram kl. 09:00 UTC | "
        "Full scanner kl. 07,09,11,13,15,17,19 UTC"
    )
    # ───────────────────────────────────────────────────────────

    if db_state.connected:
        logger.info("[APP] ✅ SesomNod Engine KLAR! (FULL MODE)")
    else:
        logger.info("[APP] ✅ SesomNod Engine KLAR! (OFFLINE MODE)")

    yield

    scheduler.shutdown(wait=False)
    logger.info("[Scheduler] AsyncIOScheduler stoppet")
    reconnect_task.cancel()
    try:
        await reconnect_task
    except asyncio.CancelledError:
        pass
    if db_state.pool:
        await db_state.pool.close()
    logger.info("[APP] 👋 SesomNod Engine avslutter.")


# ─────────────────────────────────────────────────────────
# APP
# ─────────────────────────────────────────────────────────
app = FastAPI(
    title="SesomNod Engine API",
    version="8.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────────────────
# ENDEPUNKTER
# ─────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    try:
        return JSONResponse(status_code=200, content={
            "status": "online",
            "service": cfg.SERVICE_NAME,
            "version": "8.0.0-railway",
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
        "version": "8.0.0-railway",
        "status": "online",
        "db_connected": db_state.connected,
        "endpoints": ["/health", "/picks", "/bankroll", "/dagens-kamp", "/scan-alle-kamper", "/docs"],
    }


@app.get("/bankroll")
async def get_bankroll():
    if not db_state.connected or not db_state.pool:
        return JSONResponse(status_code=503, content={
            "status": "offline",
            "data": [],
            "error": "Database ikke tilgjengelig"
        })
    try:
        async with db_state.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM bankroll ORDER BY timestamp DESC LIMIT 100"
            )
        return {
            "status": "ok",
            "data": [dict(r) for r in rows],
            "count": len(rows)
        }
    except Exception as e:
        logger.error(f"[/bankroll] Feil: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e)[:200]})


@app.get("/picks")
async def get_picks():
    if not db_state.connected or not db_state.pool:
        return JSONResponse(status_code=503, content={
            "status": "offline",
            "data": [],
            "error": "Database ikke tilgjengelig"
        })
    try:
        async with db_state.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM picks ORDER BY timestamp DESC LIMIT 100"
            )
        return {
            "status": "ok",
            "data": [dict(r) for r in rows],
            "count": len(rows)
        }
    except Exception as e:
        logger.error(f"[/picks] Feil: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e)[:200]})


@app.get("/dagens-kamp")
async def get_dagens_kamp():
    if not db_state.connected or not db_state.pool:
        return JSONResponse(status_code=503, content={
            "status": "offline",
            "data": [],
            "error": "Database ikke tilgjengelig"
        })
    try:
        async with db_state.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM dagens_kamp ORDER BY timestamp DESC LIMIT 50"
            )
        return {
            "status": "ok",
            "data": [dict(r) for r in rows],
            "count": len(rows)
        }
    except Exception as e:
        logger.error(f"[/dagens-kamp] Feil: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e)[:200]})


@app.get("/scan-alle-kamper")
async def scan_alle_kamper():
    """Scanner alle ligaer og returnerer kvalifiserte picks (edge >= 8%, confidence >= HIGH)."""
    if not cfg.ODDS_API_KEY:
        return JSONResponse(status_code=503, content={
            "status": "error",
            "error": "ODDS_API_KEY mangler"
        })
    try:
        candidates = await _scan_leagues(cfg.ODDS_API_KEY)
        best = candidates[0] if candidates else None
        return {
            "status": "ok",
            "scanned_leagues": [f"{l['flag']} {l['name']}" for l in SCAN_LEAGUES],
            "filters": {"edge_min_pct": EDGE_MIN, "confidence_min": CONFIDENCE_HIGH},
            "qualified_picks": len(candidates),
            "best_pick": best,
            "all_picks": candidates,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        logger.exception(f"[/scan-alle-kamper] Feil: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e)[:200]})


@app.get("/db/retry")
async def db_retry():
    """Tving ny DB-tilkobling."""
    logger.info("[DB] Manuell retry...")
    if db_state.pool:
        try:
            await db_state.pool.close()
        except Exception:
            pass
    await db_state.mark_fail("Manuell retry")
    success = await connect_db()
    return {
        "triggered": True,
        "success": success,
        "db": db_state.to_dict()
    }


@app.get("/status")
async def status():
    return {
        "service": cfg.SERVICE_NAME,
        "version": "8.0.0-railway",
        "db": db_state.to_dict(),
        "config": {
            "database_url_set": bool(cfg.DATABASE_URL),
            "telegram_set": bool(cfg.TELEGRAM_TOKEN),
            "odds_api_set": bool(cfg.ODDS_API_KEY),
            "port": cfg.PORT,
            "environment": cfg.ENVIRONMENT,
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ─────────────────────────────────────────────────────────
# GLOBAL EXCEPTION HANDLER
# ─────────────────────────────────────────────────────────
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
