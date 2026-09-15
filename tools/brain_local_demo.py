"""
Oraklion Brain v2 — LOKAL DEMO-KJØRER (M1). ALDRI PROD.

Hva den gjør
  * Starter en innebygd PostgreSQL (pgserver) under <repo>/.brain_demo_pg/ — eller bruker
    BRAIN_DEMO_DSN hvis satt. DSN som inneholder "railway"/"rlwy" NEKTES.
  * Kjører migreringen (down → up) mot den lokale DB-en og seeder det syntetiske M1-løpet
    som er dokumentert i docs/BRAIN_V2_STATE.md (kandidat → COMMIT → close 1.85 + WIN → REVEAL → HOLD).
  * Serverer de ekte endepunktene fra services/oraklion_brain/api.py på 127.0.0.1:<port>
    med envelope.source = "demo" (frontend viser DEMO-banner) + /health {"mode":"DEMO"}.
  * Kjører engine.tick() hvert 15. minutt mot lokal DB (0 API-kall) så heartbeat = RUNNING.

Hva den IKKE gjør
  * Importerer ikke main.py. Leser ikke .env. Ingen APScheduler, ingen Telegram, ingen
    eksterne API-kall, ingen skriving til Railway/prod. Bind kun 127.0.0.1.

Kjør:  python3 tools/brain_local_demo.py          (port 8100; BRAIN_DEMO_PORT overstyrer,
                                                    BRAIN_DEMO_PGDATA overstyrer datamappe)
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Defensivt: aldri arv prod-hemmeligheter inn i demo-prosessen.
for _k in ("DATABASE_URL", "TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID", "DON_INTERNAL_TELEGRAM_CHAT_ID",
           "RAILWAY_ENVIRONMENT", "RAILWAY_GIT_COMMIT_SHA", "BRAIN_V2_JOBS"):
    os.environ.pop(_k, None)
os.environ["RAILWAY_GIT_COMMIT_SHA"] = "demo"  # MODEL_VERSION = sniper_live@demo

PORT = int(os.environ.get("BRAIN_DEMO_PORT", "8100"))
UP = (ROOT / "migrations" / "2026-09-15_brain_v2_up.sql").read_text()
DOWN = (ROOT / "migrations" / "2026-09-15_brain_v2_down.sql").read_text()

SNIPER_DDL = """
CREATE TABLE IF NOT EXISTS sniper_bets_v1 (
    id BIGSERIAL PRIMARY KEY, match_id TEXT NOT NULL, league TEXT NOT NULL,
    home_team TEXT NOT NULL, away_team TEXT NOT NULL, kickoff_time TIMESTAMPTZ NOT NULL,
    pick_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(), market TEXT NOT NULL DEFAULT 'OVER_2_5',
    model_prob FLOAT NOT NULL, market_implied_prob FLOAT NOT NULL, edge_pct FLOAT NOT NULL,
    odds_open FLOAT NOT NULL, odds_open_timestamp TIMESTAMPTZ NOT NULL, odds_open_source TEXT NOT NULL DEFAULT 'pinnacle',
    odds_close FLOAT, odds_close_timestamp TIMESTAMPTZ, close_capture_minutes_before INT, clv_source TEXT,
    home_goals INT, away_goals INT, total_goals INT, result TEXT, settled_at TIMESTAMPTZ,
    fixture_status TEXT, market_tier TEXT DEFAULT 'PRIMARY', created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(match_id, market)
);
"""


def _dsn() -> str:
    dsn = os.environ.get("BRAIN_DEMO_DSN")
    if dsn:
        if "railway" in dsn or "rlwy" in dsn:
            raise SystemExit("NEKTET: BRAIN_DEMO_DSN peker mot Railway. Demo kjører aldri mot prod.")
        return dsn
    import pgserver  # innebygd PostgreSQL, ingen systeminstallasjon
    pgdata = Path(os.environ.get("BRAIN_DEMO_PGDATA") or (ROOT / ".brain_demo_pg"))
    pgdata.mkdir(parents=True, exist_ok=True)
    server = pgserver.get_server(str(pgdata))
    return server.get_uri()


async def seed_synthetic_m1(pool) -> dict:
    """Syntetisk M1-løp, tidsstemplet relativt til nå slik at heartbeat/SLA gir RUNNING."""
    from services.oraklion_brain import engine as E
    now = datetime.now(timezone.utc).replace(microsecond=0)
    t0 = now - timedelta(hours=6)           # TICK1 (commit)
    kickoff = t0 + timedelta(hours=2)       # avspark = nå − 4 t
    t1 = now - timedelta(hours=1)           # TICK2 (første observasjon av WIN)
    t2 = now                                # TICK3 (settle → REVEAL, deretter HOLD)

    async with pool.acquire() as conn:
        await conn.execute(DOWN)
        await conn.execute("DROP TABLE IF EXISTS sniper_bets_v1; DROP TABLE IF EXISTS sniper_scan_log;")
        await conn.execute(SNIPER_DDL)
        await conn.execute(UP)
        sid = await conn.fetchval(
            "INSERT INTO sniper_bets_v1 (match_id, league, home_team, away_team, kickoff_time, pick_timestamp,"
            " model_prob, market_implied_prob, edge_pct, odds_open, odds_open_timestamp, result, market_tier)"
            " VALUES ('DEMO-1001','DEMO LEAGUE','DEMO HOME','DEMO AWAY',$1,$2,0.61,0.5128,9.7,1.95,$2,'PENDING','PRIMARY')"
            " RETURNING id;", kickoff, t0 - timedelta(minutes=20))
    r1 = await E.tick(pool, t0)
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE sniper_bets_v1 SET odds_close=1.85, odds_close_timestamp=$2, close_capture_minutes_before=5,"
            " clv_source='PRE_KICKOFF', result='WIN', home_goals=2, away_goals=1, total_goals=3,"
            " settled_at=$3, fixture_status='FT' WHERE id=$1;",
            sid, kickoff - timedelta(minutes=5), kickoff + timedelta(hours=2))
    r2 = await E.tick(pool, t1)
    r3 = await E.tick(pool, t2)
    async with pool.acquire() as conn:
        events = [r["event_type"] for r in await conn.fetch("SELECT event_type FROM oraklion.brain_events ORDER BY seq;")]
        problems = await conn.fetch("SELECT * FROM oraklion.brain_verify_chain();")
    return {"tick1": r1, "tick2": r2, "tick3": r3, "events": events, "chain_problems": len(problems)}


class DBState:
    def __init__(self, pool):
        self.pool = pool
        self.connected = True


async def _tick_loop(pool):
    from services.oraklion_brain import engine as E
    while True:
        await asyncio.sleep(900)
        try:
            await E.tick(pool)
        except Exception as e:  # noqa: BLE001
            print(f"[demo] tick error: {e}", flush=True)


def build_app(pool):
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import HTMLResponse
    from services.oraklion_brain import api

    # Alle "live"-konvolutter merkes "demo" — frontend viser DEMO-banner på envelope.source.
    _orig_envelope = api._envelope

    def _demo_envelope(data, *, degraded=False, source="live"):
        return _orig_envelope(data, degraded=degraded, source="demo" if source == "live" else f"demo:{source}")

    api._envelope = _demo_envelope
    api.configure(DBState(pool))

    app = FastAPI(title="Oraklion Brain v2 — LOCAL DEMO", docs_url=None, redoc_url=None)
    app.add_middleware(CORSMiddleware, allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
                       allow_methods=["GET"], allow_headers=["*"])
    app.include_router(api.router)

    @app.get("/public/oraklion/state")
    async def demo_state():
        """Skallets TopBar leser dette (BRIEF-2 §8.1). Avledet fra den lokale demo-DB-en, aldri oppdiktet:
        engine_paused=False fordi demo-tick-løkken kjører; leagues/scanned/sealed/chain fra brain-tabellene."""
        async with pool.acquire() as conn:
            leagues = await conn.fetchval("SELECT count(DISTINCT league) FROM sniper_bets_v1;")
            scanned = await conn.fetchval(
                "SELECT COALESCE(SUM(scanned_n), 0) FROM oraklion.brain_ticks"
                " WHERE tick_utc::date = (NOW() AT TIME ZONE 'UTC')::date;")
            sealed = await conn.fetchval(
                "SELECT count(*) FROM oraklion.brain_events WHERE event_type = 'COMMIT'"
                " AND ts_utc::date = (NOW() AT TIME ZONE 'UTC')::date;")
            problems = await conn.fetchval("SELECT count(*) FROM oraklion.brain_verify_chain();")
        return _demo_envelope({
            "engine_paused": False, "paused_since": None,
            "leagues_monitored": int(leagues or 0), "scanned_today": int(scanned or 0),
            "sealed_today": int(sealed or 0), "chain_intact": (problems == 0),
        })

    @app.get("/health")
    async def health():
        return {"status": "ok", "mode": "DEMO", "brain_v2": "on", "production_writes": False, "telegram": False}

    @app.get("/", response_class=HTMLResponse)
    async def index():
        return ("<h1 style='font-family:monospace'>DEMO — Oraklion Brain v2 (lokal, syntetiske data)</h1>"
                "<p><a href='/public/oraklion/brain'>/public/oraklion/brain</a> · "
                "<a href='/public/oraklion/brain/ledger.json'>/public/oraklion/brain/ledger.json</a> · "
                "<a href='/health'>/health</a></p>")

    @app.on_event("startup")
    async def _start():
        asyncio.create_task(_tick_loop(pool))

    return app


def main() -> None:
    import asyncpg
    import uvicorn

    dsn = _dsn()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    pool = loop.run_until_complete(asyncpg.create_pool(dsn, min_size=1, max_size=4))
    seed = loop.run_until_complete(seed_synthetic_m1(pool))
    print(f"[demo] seed: events={seed['events']} chain_problems={seed['chain_problems']}", flush=True)
    print(f"[demo] tick1={seed['tick1'].get('scan')} tick2={seed['tick2'].get('settle')} tick3={seed['tick3'].get('settle')} {seed['tick3'].get('scan')}", flush=True)
    if seed["events"] != ["EPOCH_START", "COMMIT", "REVEAL", "HOLD"] or seed["chain_problems"]:
        raise SystemExit(f"DEMO-seed avvek fra forventet M1-løp: {seed}")
    app = build_app(pool)
    print(f"[demo] DEMO-backend klar: http://127.0.0.1:{PORT}/public/oraklion/brain", flush=True)
    config = uvicorn.Config(app, host="127.0.0.1", port=PORT, log_level="info", loop="none")
    server = uvicorn.Server(config)
    loop.run_until_complete(server.serve())


if __name__ == "__main__":
    main()
