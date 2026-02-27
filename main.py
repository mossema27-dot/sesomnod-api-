"""
SESOMNOD ENGINE — FastAPI Backend v3.0 (Stabilized)
Full Automation: Auto result-check, bankroll tracker, Telegram, scheduler
"""

import os
import logging

# ── PREFLIGHT & SANITIZATION (MUST BE FIRST) ──────────────────
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("preflight")

def sanitize_env(key):
    val = os.getenv(key, "")
    if val:
        val = val.strip()  # Removes hidden \n, \r, and spaces
        os.environ[key] = val
    return val

def preflight_check():
    keys = ["SUPABASE_URL", "SUPABASE_PAT", "TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID", "ODDS_API_KEY"]
    for key in keys:
        val = sanitize_env(key)
        has_newline = '\n' in val or '\r' in val
        log.info(f"[PREFLIGHT] {key}: len={len(val)} clean={not has_newline}")
        if has_newline:
            log.error(f"[PREFLIGHT] CRITICAL: {key} contains newline!")

preflight_check()

import math
import json
import asyncio
import random
from datetime import date, datetime, timedelta, timezone
from typing import Optional, List
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# ── APP INITIALIZATION ────────────────────────────────────────
app = FastAPI(title="SesomNod Engine API")

# ── DUMB HEALTHCHECK (NO DB DEPENDENCY) ────────────────────────
@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "sesomnod-api",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

# ── IMPORTS AFTER APP INIT TO PREVENT CIRCULAR DEPS ───────────
from dagens_kamp import analyze_dagens_kamp, format_dagens_kamp_telegram, DISCLAIMER
from auto_result import (
    check_result_football_data,
    check_result_odds_api,
    determine_result,
    format_win_telegram,
    format_loss_telegram,
    format_push_telegram,
)
from bankroll import (
    ensure_bankroll_tables,
    get_current_bankroll,
    get_bankroll_history,
    apply_win,
    apply_loss,
    apply_push,
    format_daily_summary_telegram,
    BANKROLL_GOAL,
    BANKROLL_START,
)

# ── CONFIG ────────────────────────────────────────────────────
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")
FOOTBALL_DATA_KEY = os.getenv("FOOTBALL_DATA_KEY", "")

SUPABASE_PAT = os.getenv("SUPABASE_PAT", "")
SUPABASE_PROJECT = os.getenv("SUPABASE_PROJECT", "")
SUPABASE_QUERY_URL = f"https://api.supabase.com/v1/projects/{SUPABASE_PROJECT}/database/query"

# ── DATABASE HELPERS ──────────────────────────────────────────
async def db_query(sql: str) -> list:
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(
                SUPABASE_QUERY_URL,
                headers={
                    "Authorization": f"Bearer {SUPABASE_PAT}",
                    "Content-Type": "application/json"
                },
                json={"query": sql},
                timeout=30
            )
            if resp.status_code not in (200, 201):
                log.error(f"DB error: {resp.text}")
                return []
            return resp.json()
        except Exception as e:
            log.error(f"DB connection failed: {e}")
            return []

async def db_execute(sql: str) -> list:
    return await db_query(sql)

# ── LIFESPAN ──────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Safe database initialization
    try:
        # Tables will only be created if DB is reachable
        await ensure_dagens_kamp_table()
        await ensure_bankroll_tables(db_execute)
        log.info("Database initialization check complete.")
    except Exception as e:
        log.warning(f"Startup DB check skipped/failed: {e}")
    
    # Start background scheduler
    task = asyncio.create_task(background_scheduler())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

app.router.lifespan_context = lifespan

# ── THE REST OF THE CODE (MODIFIED FOR STABILITY) ──────────────
# (Keeping the existing logic but ensuring safety)

async def ensure_dagens_kamp_table():
    await db_execute("""
        CREATE TABLE IF NOT EXISTS dagens_kamp (
            id SERIAL PRIMARY KEY,
            dato DATE NOT NULL DEFAULT CURRENT_DATE,
            league TEXT,
            league_flag TEXT,
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            commence_time TIMESTAMPTZ,
            pick TEXT,
            odds NUMERIC(6,3),
            ev_pct NUMERIC(6,2),
            confidence INTEGER,
            home_win_pct NUMERIC(5,1),
            draw_pct NUMERIC(5,1),
            away_win_pct NUMERIC(5,1),
            over25_pct NUMERIC(5,1),
            btts_pct NUMERIC(5,1),
            kelly_stake NUMERIC(5,2),
            simulation_data JSONB,
            rationale TEXT,
            resultat TEXT,
            home_score INTEGER,
            away_score INTEGER,
            result_source TEXT,
            result_checked_at TIMESTAMPTZ,
            posted_telegram BOOLEAN DEFAULT FALSE,
            result_posted_telegram BOOLEAN DEFAULT FALSE,
            matches_analyzed INTEGER DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(dato)
        )
    """)

# [Note: The rest of your endpoints like /stats, /bankroll, /telegram remain the same 
# but will now use the stabilized db_query/db_execute helpers above]

# ... (Include all other functions from your original main.py here) ...

# ── MAIN ENTRY ────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    # Force port 8000 for Railway stability as requested
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)
