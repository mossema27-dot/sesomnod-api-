import sys
import os
import json
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional, List, Dict, Any

# FORTRESS: Block sys.exit before ANY imports
_original_exit = sys.exit
def _safe_exit(code=0):
    print(f"[FORTRESS] sys.exit({code}) blocked! Continuing execution...")
    return None
sys.exit = _safe_exit
_original_os_exit = os._exit
def _safe_os_exit(code=0):
    print(f"[FORTRESS] os._exit({code}) blocked! Continuing execution...")
    return None
os._exit = _safe_os_exit

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Safe import function
def _safe_import(module_name):
    try:
        module = __import__(module_name)
        print(f"[IMPORT] {module_name} loaded OK")
        return module
    except Exception as e:
        print(f"[IMPORT] {module_name} failed: {e}")
        return None

# Import Fortress modules
bankroll_module = _safe_import("bankroll")
dagens_kamp_module = _safe_import("dagens_kamp")
auto_result_module = _safe_import("auto_result")

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
import asyncpg

# Config
class Config:
    def __init__(self):
        self.DATABASE_URL = self._clean(os.getenv("DATABASE_URL", ""))
        self.SUPABASE_SERVICE_KEY = self._clean(os.getenv("SUPABASE_SERVICE_KEY", ""))
        self.TELEGRAM_TOKEN = self._clean(os.getenv("TELEGRAM_TOKEN", ""))
        self.TELEGRAM_CHAT_ID = self._clean(os.getenv("TELEGRAM_CHAT_ID", ""))
        self.ODDS_API_KEY = self._clean(os.getenv("ODDS_API_KEY", ""))
    
    def _clean(self, value):
        if not value:
            return value
        return value.strip().replace('\n', '').replace('\r', '').replace('\t', '')

cfg = Config()

# Database state
class DBState:
    def __init__(self):
        self.online = False
        self.pool = None
        self.error = None
        self.attempt_count = 0

db_state = DBState()

# Lifespan manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[STARTUP] SesomNod API v7.2 PostgreSQL Edition starting...")
    
    # Connect to database
    try:
        if cfg.DATABASE_URL:
            db_state.pool = await asyncpg.create_pool(
                cfg.DATABASE_URL,
                min_size=1,
                max_size=10,
                command_timeout=60
            )
            # Test connection
            async with db_state.pool.acquire() as conn:
                result = await conn.fetchval("SELECT 1")
                if result == 1:
                    db_state.online = True
                    print(f"[DB] Connected to PostgreSQL via Session Pooler!")
        else:
            print("[DB] No DATABASE_URL set")
    except Exception as e:
        db_state.error = str(e)
        print(f"[DB] Connection failed: {e}")
    
    yield
    
    # Shutdown
    print("[SHUTDOWN] Cleaning up...")
    if db_state.pool:
        await db_state.pool.close()

app = FastAPI(
    title="SesomNod Engine",
    version="7.2.0-postgresql",
    lifespan=lifespan
)

@app.middleware("http")
async def request_middleware(request: Request, call_next):
    try:
        response = await call_next(request)
        return response
    except Exception as e:
        print(f"[ERROR] {e}")
        return JSONResponse(
            status_code=500,
            content={"error": "Internal error", "detail": str(e)}
        )

@app.get("/health")
async def health():
    db_status = {
        "connected": db_state.online,
        "error": db_state.error,
        "attempt_count": db_state.attempt_count
    }
    
    return {
        "status": "online" if db_state.online else "degraded",
        "service": "sesomnod-api",
        "version": "7.2.0-postgresql",
        "db": db_status,
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/bankroll")
async def get_bankroll():
    if not db_state.online or not db_state.pool:
        raise HTTPException(status_code=503, detail="Database offline")
    
    try:
        async with db_state.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM bankroll ORDER BY timestamp DESC LIMIT 100"
            )
            return {"data": [dict(row) for row in rows]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/picks")
async def get_picks():
    if not db_state.online or not db_state.pool:
        raise HTTPException(status_code=503, detail="Database offline")
    
    try:
        async with db_state.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM picks ORDER BY created_at DESC LIMIT 100"
            )
            return {"data": [dict(row) for row in rows]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/dagens-kamp")
async def get_dagens_kamp():
    if not db_state.online or not db_state.pool:
        raise HTTPException(status_code=503, detail="Database offline")
    
    try:
        async with db_state.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM dagens_kamp ORDER BY created_at DESC LIMIT 1"
            )
            if row:
                return {"data": dict(row)}
            return {"data": None}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
async def root():
    return {
        "message": "SesomNod Engine v7.2",
        "version": "7.2.0-postgresql",
        "status": "running"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
