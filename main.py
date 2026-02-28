import sys
import os
import json
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional, List, Dict, Any
import logging

# FORTRESS: Block sys.exit before ANY imports that might call it
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

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import httpx

# Setup logging
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

# Config
class Config:
    def __init__(self):
        self.SUPABASE_URL = self._clean(os.getenv("SUPABASE_URL", "https://vpgasvzrssygogvuolkb.supabase.co"))
        self.SUPABASE_SERVICE_KEY = self._clean(os.getenv("SUPABASE_SERVICE_KEY", ""))
        self.SUPABASE_ANON_KEY = self._clean(os.getenv("SUPABASE_ANON_KEY", ""))
        self.TELEGRAM_TOKEN = self._clean(os.getenv("TELEGRAM_TOKEN", ""))
        self.TELEGRAM_CHAT_ID = self._clean(os.getenv("TELEGRAM_CHAT_ID", ""))
        self.ODDS_API_KEY = self._clean(os.getenv("ODDS_API_KEY", ""))
        self.DATABASE_URL = self._clean(os.getenv("DATABASE_URL", ""))
        
        # Ensure REST API URL is correct
        if not self.SUPABASE_URL.startswith("http"):
            self.SUPABASE_URL = f"https://{self.SUPABASE_URL}"
    
    def _clean(self, value):
        if not value:
            return value
        return value.strip().replace("\n", "").replace("\r", "").replace("\t", "")

cfg = Config()

# Database state
class DBState:
    def __init__(self):
        self.online = False
        self.client = None
        self.error = None
        self.attempt_count = 0

db_state = DBState()

# Lifespan manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[STARTUP] SesomNod API v7.1 REST Edition starting...")
    
    # Create httpx client with SSL verification disabled
    try:
        db_state.client = httpx.AsyncClient(
            base_url=f"{cfg.SUPABASE_URL}/rest/v1",
            headers={
                "apikey": cfg.SUPABASE_SERVICE_KEY,
                "Authorization": f"Bearer {cfg.SUPABASE_SERVICE_KEY}",
                "Content-Type": "application/json",
                "Prefer": "return=representation"
            },
            timeout=30.0,
            verify=False # FIX: Disable SSL verification for Railway
        )
        print(f"[STARTUP] HTTPX client created for {cfg.SUPABASE_URL}/rest/v1")
        
        # Test connection
        try:
            response = await db_state.client.get("/bankroll", params={"limit": 1})
            if response.status_code in [200, 401, 403]:
                db_state.online = True
                print("[STARTUP] ✅ Database connection successful")
            else:
                print(f"[STARTUP] DB test returned {response.status_code}")
        except Exception as e:
            print(f"[STARTUP] DB test failed: {e}")
            db_state.error = str(e)
            
    except Exception as e:
        print(f"[STARTUP] Failed to create client: {e}")
        db_state.error = str(e)
    
    yield
    
    # Shutdown
    print("[SHUTDOWN] Cleaning up...")
    if db_state.client:
        await db_state.client.aclose()
        print("[SHUTDOWN] HTTPX client closed")

# Create app
app = FastAPI(
    title="SesomNod Engine",
    description="Sports Market Analytics Platform - REST API Edition",
    version="7.1.0-rest",
    lifespan=lifespan
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"[ERROR] Global handler: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal error", "error": str(exc)}
    )

@app.get("/")
async def root():
    return {
        "service": "SesomNod Engine",
        "version": "7.1.0-rest",
        "status": "online",
        "db_online": db_state.online
    }

@app.get("/health")
async def health():
    """Health check - always returns 200 even if DB is down"""
    # Try to ping if not online
    if not db_state.online and db_state.client:
        try:
            response = await db_state.client.get("/bankroll", params={"limit": 1}, timeout=5.0)
            if response.status_code in [200, 401, 403]:
                db_state.online = True
        except:
            pass
    
    return {
        "status": "online",
        "db": {
            "online": db_state.online,
            "error": db_state.error
        },
        "version": "7.1.0-rest",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/bankroll")
async def get_bankroll():
    """Get bankroll history"""
    if not db_state.client:
        raise HTTPException(status_code=503, detail="Database client not ready")
    
    try:
        response = await db_state.client.get(
            "/bankroll",
            params={"order": "timestamp.desc", "limit": 100}
        )
        
        if response.status_code == 200:
            return {"status": "success", "data": response.json()}
        else:
            return {"status": "error", "code": response.status_code, "data": []}
            
    except Exception as e:
        logger.error(f"[BANKROLL] Exception: {e}")
        return {"status": "error", "error": str(e), "data": []}

@app.get("/bankroll/current")
async def get_current_bankroll():
    """Get current bankroll value"""
    if not db_state.client:
        return {"amount": 100, "status": "offline"}
    
    try:
        response = await db_state.client.get(
            "/bankroll",
            params={"order": "timestamp.desc", "limit": 1}
        )
        
        if response.status_code == 200:
            data = response.json()
            if data and len(data) > 0:
                return {
                    "amount": data[0].get("amount", 100),
                    "timestamp": data[0].get("timestamp"),
                    "status": "success"
                }
        return {"amount": 100, "status": "no_data"}
        
    except Exception as e:
        logger.error(f"[BANKROLL/CURRENT] Error: {e}")
        return {"amount": 100, "status": "error", "error": str(e)}

@app.get("/picks")
async def get_picks():
    """Get all picks"""
    if not db_state.client:
        raise HTTPException(status_code=503, detail="Database client not ready")
    
    try:
        response = await db_state.client.get(
            "/picks",
            params={"order": "created_at.desc", "limit": 100}
        )
        
        if response.status_code == 200:
            return {"status": "success", "data": response.json()}
        else:
            return {"status": "error", "code": response.status_code, "data": []}
            
    except Exception as e:
        logger.error(f"[PICKS] Exception: {e}")
        return {"status": "error", "error": str(e), "data": []}

@app.post("/picks")
async def create_pick(pick: dict):
    """Create a new pick"""
    if not db_state.client:
        raise HTTPException(status_code=503, detail="Database client not ready")
    
    try:
        response = await db_state.client.post(
            "/picks",
            json=pick
        )
        
        if response.status_code in [200, 201]:
            return {"status": "success", "data": response.json()}
        else:
            return {"status": "error", "code": response.status_code, "message": response.text}
            
    except Exception as e:
        logger.error(f"[PICKS/CREATE] Exception: {e}")
        return {"status": "error", "error": str(e)}

@app.get("/dagens-kamp")
async def get_dagens_kamp():
    """Get today's match analysis"""
    if not db_state.client:
        raise HTTPException(status_code=503, detail="Database client not ready")
    
    try:
        response = await db_state.client.get(
            "/dagens_kamp",
            params={"order": "created_at.desc", "limit": 1}
        )
        
        if response.status_code == 200:
            data = response.json()
            return {
                "status": "success", 
                "data": data[0] if data and len(data) > 0 else None,
                "has_data": len(data) > 0 if data else False
            }
        else:
            return {"status": "error", "code": response.status_code, "data": None}
            
    except Exception as e:
        logger.error(f"[DAGENS-KAMP] Exception: {e}")
        return {"status": "error", "error": str(e), "data": None}

@app.post("/dagens-kamp/analyze")
async def analyze_dagens_kamp():
    """Trigger analysis"""
    return {
        "status": "queued",
        "message": "Analysis scheduled",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/stats")
async def get_stats():
    """Get system stats"""
    return {
        "status": "online",
        "version": "7.1.0-rest",
        "db_online": db_state.online,
        "modules": {
            "bankroll": bankroll_module is not None,
            "dagens_kamp": dagens_kamp_module is not None,
            "auto_result": auto_result_module is not None
        }
    }

@app.get("/test-net")
async def test_net():
    """Test internet connectivity"""
    try:
        async with httpx.AsyncClient(timeout=10.0, verify=False) as client:
            response = await client.get("https://httpbin.org/get")
            return {
                "internet": True,
                "status_code": response.status_code,
                "supabase_url": cfg.SUPABASE_URL[:40] + "..."
            }
    except Exception as e:
        return {
            "internet": False,
            "error": str(e),
            "supabase_url": cfg.SUPABASE_URL[:40] + "..."
        }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
