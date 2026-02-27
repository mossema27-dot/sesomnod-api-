"""
SESOMNOD ENGINE — FastAPI Backend v3.4 (DB-OPTIONAL MODE)
ULTIMATE STABILIZATION: Zero-Crash Policy, Offline-Ready, Health-First.
"""

import os
import logging
import sys
import time
import json
import asyncio
from datetime import date, datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

# ── 1. LOGGING & SANITIZATION ────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    stream=sys.stdout
)
log = logging.getLogger("sesomnod.core")

# Global DB Status Flag
DB_ONLINE = False

def deep_sanitize_env():
    critical_keys = ["SUPABASE_URL", "SUPABASE_PAT", "SUPABASE_PROJECT", "TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID"]
    for key in critical_keys:
        val = os.getenv(key, "")
        if val:
            clean_val = "".join(char for char in val.strip() if char.isprintable())
            os.environ[key] = clean_val

deep_sanitize_env()

# ── 2. BASE IMPORTS ──────────────────────────────────────────
import httpx
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ── 3. APP INITIALIZATION ─────────────────────────────────────
app = FastAPI(title="SesomNod Engine API", version="3.4.0" )
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── 4. CONFIGURATION ──────────────────────────────────────────
SUPABASE_PAT = os.getenv("SUPABASE_PAT", "")
SUPABASE_PROJECT = os.getenv("SUPABASE_PROJECT", "")
SUPABASE_QUERY_URL = f"https://api.supabase.com/v1/projects/{SUPABASE_PROJECT}/database/query" if SUPABASE_PROJECT else ""
ODDS_API_KEY = os.getenv("ODDS_API_KEY", "" )

# ── 5. INDESTRUCTIBLE DB ENGINE ───────────────────────────────
async def safe_db_query(sql: str) -> list:
    """Never crashes. Returns empty list on any error."""
    if not SUPABASE_QUERY_URL or not SUPABASE_PAT:
        return []
    async with httpx.AsyncClient( ) as client:
        try:
            resp = await client.post(
                SUPABASE_QUERY_URL,
                headers={"Authorization": f"Bearer {SUPABASE_PAT}", "Content-Type": "application/json"},
                json={"query": sql},
                timeout=10.0
            )
            if resp.status_code in (200, 201):
                return resp.json()
            log.warning(f"DB API returned {resp.status_code}: {resp.text}")
            return []
        except Exception as e:
            log.error(f"DB Connection failed (Non-fatal): {e}")
            return []

async def db_execute(sql: str) -> list:
    return await safe_db_query(sql)

# ── 6. GUARANTEED HEALTHCHECK ─────────────────────────────────
@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "sesomnod-api",
        "database": "online" if DB_ONLINE else "offline",
        "mode": "stable"
    }

# ── 7. LIFESPAN (ZERO-CRASH STARTUP) ──────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global DB_ONLINE
    log.info("Starting SesomNod Engine...")
    
    try:
        # Test DB connection without failing
        test = await safe_db_query("SELECT 1")
        if test:
            DB_ONLINE = True
            log.info("✅ Database connected")
            # Only try to init tables if DB is online
            from bankroll import ensure_bankroll_tables
            await ensure_bankroll_tables(db_execute)
        else:
            log.warning("⚠️ Database unavailable - running in OFFLINE MODE")
    except Exception as e:
        log.warning(f"⚠️ Startup DB check failed: {e} - running in OFFLINE MODE")
    
    scheduler_task = asyncio.create_task(background_scheduler())
    log.info("Application startup complete. System is LIVE.")
    yield
    scheduler_task.cancel()

app.router.lifespan_context = lifespan

async def background_scheduler():
    while True:
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError: break
        except Exception: pass

# ── 8. FAIL-SAFE ENDPOINTS ────────────────────────────────────
@app.get("/stats")
async def get_stats():
    try:
        rows = await safe_db_query("SELECT COUNT(*) as total, COALESCE(SUM(pl_beregnet), 0) as pl FROM picks")
        if rows: return {**rows[0], "status": "online" if DB_ONLINE else "offline"}
    except: pass
    return {"total": 0, "pl": 0, "status": "offline"}

@app.get("/bankroll")
async def get_bankroll():
    try:
        from bankroll import get_current_bankroll, BANKROLL_GOAL, BANKROLL_START
        current = await get_current_bankroll(db_execute)
        return {"current": current, "goal": BANKROLL_GOAL, "start": BANKROLL_START, "status": "online" if DB_ONLINE else "offline"}
    except: pass
    return {"current": 0, "goal": 0, "status": "offline"}

@app.get("/dagens-kamp")
async def get_dagens_kamp():
    try:
        today = date.today().isoformat()
        rows = await safe_db_query(f"SELECT * FROM dagens_kamp WHERE dato = '{today}'")
        if rows: return {**rows[0], "status": "cached"}
    except: pass
    return {"status": "offline", "message": "Database unavailable or no analysis found."}

# ── 9. ENTRY POINT ────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, workers=1, log_level="info")
