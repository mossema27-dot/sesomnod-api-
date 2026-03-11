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
from datetime import datetime, timezone

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
                    pick TEXT,
                    odds NUMERIC(5,2),
                    stake NUMERIC(10,2),
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
# SCHEDULER JOBS
# ─────────────────────────────────────────────────────────
async def post_dagens_kamp_telegram():
    """Kjører kl. 09:00 UTC — analyserer og poster Dagens Kamp til Telegram."""
    logger.info("[Scheduler] post_dagens_kamp_telegram startet")

    if not dagens_kamp_module:
        logger.warning("[Scheduler] dagens_kamp_module ikke lastet — hopper over")
        return

    if not cfg.ODDS_API_KEY:
        logger.warning("[Scheduler] ODDS_API_KEY mangler — hopper over")
        return

    if not cfg.TELEGRAM_TOKEN or not cfg.TELEGRAM_CHAT_ID:
        logger.warning("[Scheduler] TELEGRAM_TOKEN/CHAT_ID mangler — hopper over")
        return

    try:
        analysis = await dagens_kamp_module.analyze_dagens_kamp(cfg.ODDS_API_KEY)

        if "error" in analysis:
            logger.warning(f"[Scheduler] Analyse feilet: {analysis['error']}")
            return

        m = analysis["match"]
        probs = analysis["probabilities"]
        rec = analysis["recommendation"]

        kickoff_parts = m["kickoff_display"].split(" kl. ")
        dato = kickoff_parts[0] if len(kickoff_parts) > 1 else m["kickoff_display"]
        tid = kickoff_parts[1] if len(kickoff_parts) > 1 else ""

        edge = round(rec["ev_pct"] / rec["odds"], 2)

        message = (
            "⚡ SESOMNOD ENGINE\n"
            "Football Decision Intelligence\n"
            "━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🏆 {m['league_flag']} {m['league']} · MATCH BRIEF\n\n"
            f"🏟 {m['home_team']}\n"
            "         VS\n"
            f"       {m['away_team']}\n\n"
            f"🕒 {dato} · {tid}\n\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "📊 MODEL PROBABILITIES\n"
            "━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Over 2.5 mål   {probs['over25']}%\n"
            f"Begge scorer   {probs['btts']}%\n"
            f"Hjemmeseier    {probs['home_win']}%\n\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "📈 EDGE ANALYSE\n"
            "━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Odds:      {rec['odds']}\n"
            f"Edge:      +{edge}% ✅\n"
            f"EV:        +{rec['ev_pct']}% 🔥\n\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "🎯 MODEL DECISION\n"
            "━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"PICK:        {rec['pick']}\n"
            f"ODDS:        @ {rec['odds']}\n"
            f"CONFIDENCE:  {rec['confidence']}%\n\n"
            "━━━━━━━━━━━━━━━━━━━━━\n\n"
            "You don't get picks. You get control. ⚡\n"
            "SesomNod Engine"
        )

        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"https://api.telegram.org/bot{cfg.TELEGRAM_TOKEN}/sendMessage",
                json={
                    "chat_id": cfg.TELEGRAM_CHAT_ID,
                    "text": message,
                },
            )

        if resp.status_code == 200:
            logger.info("[Scheduler] Dagens Kamp postet til Telegram OK")
        else:
            logger.error(f"[Scheduler] Telegram feil {resp.status_code}: {resp.text[:200]}")

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
        post_dagens_kamp_telegram,
        trigger=CronTrigger(hour=9, minute=0, timezone="UTC"),
        id="post_dagens_kamp",
        misfire_grace_time=300,
        replace_existing=True,
    )
    scheduler.start()
    logger.info("[Scheduler] AsyncIOScheduler startet — Dagens Kamp kjører kl. 09:00 UTC")
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
        "endpoints": ["/health", "/picks", "/bankroll", "/dagens-kamp", "/docs"],
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
