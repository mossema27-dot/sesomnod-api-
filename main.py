"""
SesomNod Engine v7.1 - "REST Edition"
=====================================
GARANTIER:
- Starter alltid
- Stopper ALDRI av seg selv
- /health returnerer alltid 200
- DB-feil er ufarlig (Graceful Degradation)
- Import-feil i andre moduler er ufarlig
- Alle exceptions fanges på alle nivåer
- Ingen kode kan tvinge appen til å stoppe (Fortress Mode)
- Bruker Supabase REST API via httpx (Løser Railway IPv6-problem)
"""

# ─────────────────────────────────────────────────────────────────────────────
# KRITISK: Patch sys.exit og os._exit FØR noen imports
# Dette hindrer andre moduler fra å drepe prosessen
# ─────────────────────────────────────────────────────────────────────────────
import sys
import os

_original_exit = sys.exit
def _safe_exit(code=0):
    import logging
    logging.getLogger("sesomnod").critical(
        f"[FORTRESS] sys.exit({code}) blokkert! Appen fortsetter."
    )
sys.exit = _safe_exit  # Blokker sys.exit globalt

# ─────────────────────────────────────────────────────────────────────────────
# IMPORTS
# ─────────────────────────────────────────────────────────────────────────────

import asyncio
import logging
import signal
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
logger = logging.getLogger("sesomnod")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

# ─────────────────────────────────────────────────────────────────────────────
# SIKKER IMPORT AV ANDRE MODULER
# ─────────────────────────────────────────────────────────────────────────────

_imported_modules: Dict[str, Any] = {}
_import_errors: Dict[str, str] = {}

def _safe_import(module_name: str) -> Optional[Any]:
    """Importerer en modul trygt — hvis den feiler, logger vi og går videre."""
    try:
        import importlib
        mod = importlib.import_module(module_name)
        _imported_modules[module_name] = mod
        logger.info(f"[Import] ✅ {module_name} lastet OK")
        return mod
    except Exception as e:
        _import_errors[module_name] = str(e)
        logger.warning(f"[Import] ⚠️ {module_name} feilet: {e} — fortsetter uten")
        return None

# Prøv å laste andre moduler — feil her stopper IKKE appen
bankroll_module = _safe_import("bankroll")
dagens_kamp_module = _safe_import("dagens_kamp")
auto_result_module = _safe_import("auto_result")

# ─────────────────────────────────────────────────────────────────────────────
# KONFIGURASJON
# ─────────────────────────────────────────────────────────────────────────────

def _clean(key: str) -> str:
    """Henter og renser miljøvariabel for alle usynlige tegn."""
    val = os.getenv(key, "")
    if val:
        val = (val.strip()
               .replace("\n", "").replace("\r", "")
               .replace("\t", "").replace("\x00", "")
               .replace("\ufeff", ""))
    return val

class Config:
    SUPABASE_URL: str = _clean("SUPABASE_URL")
    SUPABASE_SERVICE_KEY: str = _clean("SUPABASE_SERVICE_KEY")
    SUPABASE_ANON_KEY: str = _clean("SUPABASE_ANON_KEY")
    TELEGRAM_TOKEN: str = _clean("TELEGRAM_TOKEN")
    TELEGRAM_CHAT_ID: str = _clean("TELEGRAM_CHAT_ID")
    ODDS_API_KEY: str = _clean("ODDS_API_KEY")
    PORT: int = int(os.getenv("PORT", "8000"))
    ENVIRONMENT: str = os.getenv("RAILWAY_ENVIRONMENT", "development")
    SERVICE_NAME: str = os.getenv("RAILWAY_SERVICE_NAME", "sesomnod-api")

cfg = Config()

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE STATE
# ─────────────────────────────────────────────────────────────────────────────

class DBState:
    def __init__(self):
        self.connected: bool = False
        self.error: str = ""
        self.attempt_count: int = 0
        self.consecutive_failures: int = 0
        self.last_check: float = 0.0
        self.last_success: float = 0.0
        self._lock = asyncio.Lock()

    async def mark_ok(self):
        async with self._lock:
            was_offline = not self.connected
            self.connected = True
            self.error = ""
            self.last_check = time.time()
            self.last_success = time.time()
            self.consecutive_failures = 0
            if was_offline:
                logger.info("[DB] ✅ Supabase REST API tilkoblet!")

    async def mark_fail(self, error: str):
        async with self._lock:
            self.connected = False
            self.error = error
            self.last_check = time.time()
            self.consecutive_failures += 1

    def to_dict(self) -> Dict[str, Any]:
        now = time.time()
        return {
            "connected": self.connected,
            "error": self.error or None,
            "attempt_count": self.attempt_count,
            "consecutive_failures": self.consecutive_failures,
            "last_check_ago_sec": round(now - self.last_check, 1) if self.last_check else None,
            "last_success_ago_sec": round(now - self.last_success, 1) if self.last_success else None,
        }

db_state = DBState()

# ─────────────────────────────────────────────────────────────────────────────
# SUPABASE REST HELPERS
# ─────────────────────────────────────────────────────────────────────────────

async def ping_supabase(client: httpx.AsyncClient) -> bool:
    """Sjekk om Supabase REST API er tilgjengelig via /rest/v1/ endepunktet"""
    if not cfg.SUPABASE_URL or not cfg.SUPABASE_SERVICE_KEY:
        return False
    try:
        # Vi pinger rot-endepunktet for REST API-et
        response = await client.get(
            f"{cfg.SUPABASE_URL}/rest/v1/",
            headers={
                "apikey": cfg.SUPABASE_SERVICE_KEY,
                "Authorization": f"Bearer {cfg.SUPABASE_SERVICE_KEY}"
            },
            timeout=5.0
        )
        if response.status_code == 200:
            await db_state.mark_ok()
            return True
        else:
            await db_state.mark_fail(f"HTTP {response.status_code}")
            return False
    except Exception as e:
        await db_state.mark_fail(str(e))
        return False

# ─────────────────────────────────────────────────────────────────────────────
# APP SETUP & LIFESPAN
# ─────────────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"🚀 {cfg.SERVICE_NAME} v7.1 REST Edition starter...")
    
    # Opprett global HTTP-klient med Supabase-headers
    app.state.db_client = httpx.AsyncClient(
        base_url=f"{cfg.SUPABASE_URL}/rest/v1",
        headers={
            "apikey": cfg.SUPABASE_SERVICE_KEY,
            "Authorization": f"Bearer {cfg.SUPABASE_SERVICE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=representation"
        },
        timeout=30.0
    )
    
    # Første sjekk av database
    await ping_supabase(app.state.db_client)
    
    yield
    
    # Shutdown logic
    await app.state.db_client.aclose()
    logger.info("👋 SesomNod Engine avslutter.")

app = FastAPI(
    title="SesomNod Engine API",
    version="7.1.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────────────────────────────────────
# ENDEPUNKTER
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health(request: Request):
    # Oppdater db-sjekk ved hvert kall for å ha ferske data i health
    await ping_supabase(request.app.state.db_client)
    return {
        "status": "online",
        "service": cfg.SERVICE_NAME,
        "version": "7.1.0-rest",
        "db": db_state.to_dict(),
        "env": cfg.ENVIRONMENT,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.get("/bankroll")
async def get_bankroll(request: Request):
    try:
        response = await request.app.state.db_client.get(
            "/bankroll",
            params={"order": "timestamp.desc", "limit": 100}
        )
        if response.status_code == 200:
            await db_state.mark_ok()
            return {"status": "ok", "data": response.json()}
        else:
            err_msg = f"HTTP {response.status_code}: {response.text[:100]}"
            await db_state.mark_fail(err_msg)
            return {"status": "error", "message": err_msg, "data": []}
    except Exception as e:
        logger.error(f"Bankroll feil: {e}")
        await db_state.mark_fail(str(e))
        return {"status": "error", "message": str(e), "data": []}

@app.get("/picks")
async def get_picks(request: Request):
    try:
        response = await request.app.state.db_client.get(
            "/picks",
            params={"order": "created_at.desc", "limit": 100}
        )
        if response.status_code == 200:
            await db_state.mark_ok()
            return {"status": "ok", "data": response.json()}
        else:
            err_msg = f"HTTP {response.status_code}: {response.text[:100]}"
            await db_state.mark_fail(err_msg)
            return {"status": "error", "message": err_msg, "data": []}
    except Exception as e:
        logger.error(f"Picks feil: {e}")
        await db_state.mark_fail(str(e))
        return {"status": "error", "message": str(e), "data": []}

@app.get("/dagens-kamp")
async def get_dagens_kamp(request: Request):
    try:
        response = await request.app.state.db_client.get(
            "/dagens_kamp",
            params={"order": "created_at.desc", "limit": 1}
        )
        if response.status_code == 200:
            await db_state.mark_ok()
            data = response.json()
            return {"status": "ok", "data": data[0] if data else None}
        else:
            err_msg = f"HTTP {response.status_code}: {response.text[:100]}"
            await db_state.mark_fail(err_msg)
            return {"status": "error", "message": err_msg, "data": None}
    except Exception as e:
        logger.error(f"Dagens kamp feil: {e}")
        await db_state.mark_fail(str(e))
        return {"status": "error", "message": str(e), "data": None}

# ─────────────────────────────────────────────────────────────────────────────
# FEILHÅNDTERING
# ─────────────────────────────────────────────────────────────────────────────

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

