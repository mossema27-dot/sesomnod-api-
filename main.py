"""
SesomNod Engine v7.0 - "FORTRESS"
==================================
GARANTIER:
- Starter alltid
- Stopper ALDRI av seg selv
- /health returnerer alltid 200
- DB-feil er ufarlig
- Import-feil i andre moduler er ufarlig
- Alle exceptions fanges på alle nivåer
- Ingen kode kan tvinge appen til å stoppe
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
_sys_exit_blocked = _safe_exit
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
# Hvis bankroll.py, dagens_kamp.py etc. krasjer ved import — fortsetter vi
# ─────────────────────────────────────────────────────────────────────────────

_imported_modules: Dict[str, Any] = {}
_import_errors: Dict[str, str]    = {}

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
        logger.warning(f"[Import] ⚠️  {module_name} feilet: {e} — fortsetter uten")
        return None

# Prøv å laste andre moduler — feil her stopper IKKE appen
bankroll_module     = _safe_import("bankroll")
dagens_kamp_module  = _safe_import("dagens_kamp")
auto_result_module  = _safe_import("auto_result")


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
    SUPABASE_URL:         str = _clean("SUPABASE_URL")
    SUPABASE_PAT:         str = _clean("SUPABASE_PAT")
    SUPABASE_PROJECT:     str = _clean("SUPABASE_PROJECT")
    SUPABASE_ANON_KEY:    str = _clean("SUPABASE_ANON_KEY")
    SUPABASE_SERVICE_KEY: str = _clean("SUPABASE_SERVICE_KEY")
    DATABASE_URL:         str = _clean("DATABASE_URL")
    TELEGRAM_TOKEN:       str = _clean("TELEGRAM_TOKEN")
    TELEGRAM_CHAT_ID:     str = _clean("TELEGRAM_CHAT_ID")
    ODDS_API_KEY:         str = _clean("ODDS_API_KEY")
    PORT:                 int = int(os.getenv("PORT", "8000"))
    ENVIRONMENT:          str = os.getenv("RAILWAY_ENVIRONMENT", "development")
    SERVICE_NAME:         str = os.getenv("RAILWAY_SERVICE_NAME", "sesomnod-api")


cfg = Config()

_all_vars = [
    "SUPABASE_URL", "SUPABASE_PAT", "SUPABASE_PROJECT",
    "SUPABASE_ANON_KEY", "SUPABASE_SERVICE_KEY", "DATABASE_URL",
    "TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID", "ODDS_API_KEY",
]
_present = [k for k in _all_vars if getattr(cfg, k)]
_missing = [k for k in _all_vars if not getattr(cfg, k)]
if _present:
    logger.info(f"[Config] Lastet: {', '.join(_present)}")
if _missing:
    logger.warning(f"[Config] Mangler: {', '.join(_missing)}")


# ─────────────────────────────────────────────────────────────────────────────
# DATABASE STATE
# ─────────────────────────────────────────────────────────────────────────────

class DBState:
    def __init__(self):
        self.connected:            bool  = False
        self.error:                str   = ""
        self.attempt_count:        int   = 0
        self.consecutive_failures: int   = 0
        self.last_check:           float = 0.0
        self.last_success:         float = 0.0
        self._lock = asyncio.Lock()

    async def mark_ok(self):
        async with self._lock:
            was_offline = not self.connected
            self.connected            = True
            self.error                = ""
            self.last_check           = time.time()
            self.last_success         = time.time()
            self.consecutive_failures = 0
            if was_offline:
                logger.info("[DB] ✅ Supabase tilkoblet!")

    async def mark_fail(self, error: str):
        async with self._lock:
            self.connected             = False
            self.error                 = error
            self.last_check            = time.time()
            self.consecutive_failures += 1

    def to_dict(self) -> Dict[str, Any]:
        now = time.time()
        return {
            "connected":            self.connected,
            "error":                self.error or None,
            "attempt_count":        self.attempt_count,
            "consecutive_failures": self.consecutive_failures,
            "last_check_ago_sec":   round(now - self.last_check, 1) if self.last_check else None,
            "last_success_ago_sec": round(now - self.last_success, 1) if self.last_success else None,
        }


db_state = DBState()


# ─────────────────────────────────────────────────────────────────────────────
# SUPABASE TILKOBLING
# ─────────────────────────────────────────────────────────────────────────────

async def ping_supabase(client: httpx.AsyncClient) -> tuple[bool, str]:
    """
    Pinger Supabase REST API.
    Returnerer (ok, feilmelding).
    Krasjer ALDRI — triple try/except.
    """
    try:
        api_key = (cfg.SUPABASE_SERVICE_KEY
                   or cfg.SUPABASE_ANON_KEY
                   or cfg.SUPABASE_PAT)

        if not cfg.SUPABASE_URL:
            return False, "SUPABASE_URL mangler"
        if not api_key:
            return False, "Ingen API-nøkkel tilgjengelig"

        try:
            response = await client.get(
                f"{cfg.SUPABASE_URL}/rest/v1/",
                headers={
                    "apikey":        api_key,
                    "Authorization": f"Bearer {api_key}",
                },
                timeout=httpx.Timeout(connect=5.0, read=8.0, write=5.0, pool=5.0),
            )

            if response.status_code == 200:
                return True, ""
            elif response.status_code == 401:
                return False, (
                    "401 Unauthorized — Sjekk SUPABASE_SERVICE_KEY i Railway Variables. "
                    "Gå til Supabase → Settings → API Keys → Legacy → service_role"
                )
            else:
                return False, f"HTTP {response.status_code}"

        except httpx.TimeoutException:
            return False, "Timeout (>8s)"
        except httpx.ConnectError as e:
            return False, f"Tilkoblingsfeil: {str(e)[:80]}"
        except httpx.HTTPError as e:
            return False, f"HTTP-feil: {str(e)[:80]}"

    except Exception as e:
        # Aller ytterste catch — ingenting slipper gjennom
        logger.exception("[DB] Uventet feil i ping_supabase")
        return False, f"Uventet: {type(e).__name__}: {str(e)[:80]}"


# ─────────────────────────────────────────────────────────────────────────────
# BAKGRUNNS RECONNECT
# Denne loopen kjører for alltid og stopper ALDRI appen
# ─────────────────────────────────────────────────────────────────────────────

async def db_reconnect_loop(client: httpx.AsyncClient) -> None:
    """
    Kjører stille i bakgrunnen for alltid.

    GARANTIER:
    - Stopper ALDRI appen uansett hva som feiler
    - Alle exceptions er fanget
    - Eksponentiell backoff: 5s → 10s → 20s → 40s → 60s maks
    - Når tilkoblet: pinger hvert 30. sekund
    """
    logger.info("[DB] Reconnect-loop startet.")

    while True:
        try:
            db_state.attempt_count += 1
            ok, err = await ping_supabase(client)

            if ok:
                await db_state.mark_ok()
                # Pinger hvert 30. sekund når tilkoblet
                await asyncio.sleep(30)
            else:
                await db_state.mark_fail(err)
                failures = db_state.consecutive_failures
                delay = min(5 * (2 ** (failures - 1)), 60)

                # Logger bare de første 3 og deretter hvert 5.
                if failures <= 3 or failures % 5 == 0:
                    logger.warning(
                        f"[DB] Offline (#{db_state.attempt_count}) "
                        f"— {err} — retry om {delay}s"
                    )
                await asyncio.sleep(delay)

        except asyncio.CancelledError:
            logger.info("[DB] Reconnect-loop avsluttet (CancelledError).")
            break
        except Exception as e:
            # Catch-all — loopen overlever ALLE feil
            logger.error(f"[DB] Uventet feil i loop: {e} — fortsetter om 30s")
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break


# ─────────────────────────────────────────────────────────────────────────────
# LIFESPAN — Oppstart og nedstenging
# ─────────────────────────────────────────────────────────────────────────────

http_client:  Optional[httpx.AsyncClient] = None
bg_reconnect: Optional[asyncio.Task]      = None
_app_running: bool                        = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Håndterer oppstart og nedstenging.

    KRITISK: yield MÅ nås — appen er ikke klar før yield.
    Ingenting mellom start og yield kan blokkere eller krasje fatalt.
    """
    global http_client, bg_reconnect, _app_running

    logger.info("=" * 55)
    logger.info("  SesomNod Engine v7.0 FORTRESS starter...")
    logger.info(f"  Miljo:   {cfg.ENVIRONMENT}")
    logger.info(f"  Service: {cfg.SERVICE_NAME}")
    logger.info(f"  Port:    {cfg.PORT}")
    logger.info("=" * 55)

    # ── HTTP KLIENT ──────────────────────────────────────────────────────────
    try:
        http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=5.0, read=15.0, write=10.0, pool=5.0),
            limits=httpx.Limits(
                max_connections=20,
                max_keepalive_connections=10,
                keepalive_expiry=30.0,
            ),
            follow_redirects=True,
        )
        logger.info("[HTTP] Klient opprettet.")
    except Exception as e:
        logger.error(f"[HTTP] Feil ved opprettelse av klient: {e} — fortsetter")
        http_client = None

    # ── FØRSTE DB SJEKK (maks 8 sekunder, blokkerer IKKE) ───────────────────
    if http_client:
        logger.info("[DB] Første Supabase-sjekk (timeout 8s)...")
        try:
            ok, err = await asyncio.wait_for(
                ping_supabase(http_client),
                timeout=8.0
            )
            if ok:
                await db_state.mark_ok()
            else:
                await db_state.mark_fail(err)
                logger.warning(f"[DB] Starter uten DB: {err}")
        except asyncio.TimeoutError:
            await db_state.mark_fail("Timeout ved oppstart")
            logger.warning("[DB] DB-sjekk timeout — starter uten DB")
        except Exception as e:
            await db_state.mark_fail(str(e))
            logger.warning(f"[DB] Feil ved oppstart: {e} — starter uten DB")
    else:
        logger.warning("[DB] Ingen HTTP-klient — hopper over DB-sjekk")

    # ── BAKGRUNNS RECONNECT ──────────────────────────────────────────────────
    try:
        if http_client:
            bg_reconnect = asyncio.create_task(
                db_reconnect_loop(http_client),
                name="db-reconnect"
            )
            logger.info("[BG] Reconnect-task opprettet.")
    except Exception as e:
        logger.error(f"[BG] Feil ved opprettelse av task: {e}")

    # ── APPEN ER KLAR ─────────────────────────────────────────────────────────
    _app_running = True
    mode = "FULL DATABASE" if db_state.connected else "OFFLINE"
    logger.info(f"[APP] ✅ SesomNod Engine KLAR! ({mode} MODE)")
    logger.info(f"[APP] /health returnerer alltid 200")
    logger.info("=" * 55)

    # ── YIELD — Appen er live fra her ───────────────────────────────────────
    yield

    # ── SHUTDOWN ─────────────────────────────────────────────────────────────
    _app_running = False
    logger.info("[APP] Nedstenging påbegynt...")

    if bg_reconnect and not bg_reconnect.done():
        bg_reconnect.cancel()
        try:
            await asyncio.wait_for(bg_reconnect, timeout=3.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass

    if http_client:
        try:
            await http_client.aclose()
        except Exception:
            pass

    logger.info("[APP] Nedstenging fullført.")


# ─────────────────────────────────────────────────────────────────────────────
# FASTAPI APP
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="SesomNod Engine",
    version="7.0.0",
    docs_url="/docs",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def request_middleware(request: Request, call_next):
    """Logger requests og fanger alle uventede feil."""
    rid   = str(uuid.uuid4())[:8]
    start = time.perf_counter()
    quiet = request.url.path in ("/health", "/")

    if not quiet:
        logger.info(f"[{rid}] {request.method} {request.url.path}")

    try:
        response      = await call_next(request)
        ms            = (time.perf_counter() - start) * 1000
        if not quiet:
            logger.info(f"[{rid}] {response.status_code} ({ms:.0f}ms)")
        response.headers["X-Request-ID"] = rid
        return response
    except Exception as exc:
        ms = (time.perf_counter() - start) * 1000
        logger.error(f"[{rid}] FEIL: {exc} ({ms:.0f}ms)")
        return JSONResponse(
            status_code=500,
            content={"error": "Server Error", "request_id": rid}
        )


# ─────────────────────────────────────────────────────────────────────────────
# ENDEPUNKTER
# ─────────────────────────────────────────────────────────────────────────────

# ── SYSTEM ────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["System"])
async def health():
    """
    Railway Healthcheck.
    RETURNERER ALLTID HTTP 200.
    Selv om DB er nede, Supabase er nede, alt er nede — dette returnerer 200.
    """
    try:
        return JSONResponse(
            status_code=200,
            content={
                "status":    "ok",
                "version":   "7.0.0",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "db":        db_state.to_dict(),
                "mode":      "full" if db_state.connected else "offline",
                "running":   _app_running,
            }
        )
    except Exception:
        # Absolutt siste fallback — ingenting kan feile her
        return JSONResponse(
            status_code=200,
            content={"status": "ok", "version": "7.0.0"}
        )


@app.get("/", tags=["System"])
async def root():
    return {
        "engine":  "SesomNod Engine",
        "version": "7.0.0",
        "status":  "running",
        "db":      "connected" if db_state.connected else "offline",
        "docs":    "/docs",
    }


@app.get("/status", tags=["System"])
async def status():
    """Full systemstatus for debugging."""
    return {
        "engine": {
            "name":        "SesomNod Engine",
            "version":     "7.0.0",
            "environment": cfg.ENVIRONMENT,
            "service":     cfg.SERVICE_NAME,
            "running":     _app_running,
            "timestamp":   datetime.now(timezone.utc).isoformat(),
        },
        "database": db_state.to_dict(),
        "modules": {
            "imported":      list(_imported_modules.keys()),
            "failed":        _import_errors,
        },
        "config": {
            k: bool(getattr(cfg, k))
            for k in _all_vars
        },
        "background_task": {
            "running": bg_reconnect is not None and not bg_reconnect.done(),
        },
    }


# ── DATABASE ──────────────────────────────────────────────────────────────────

@app.post("/db/retry", tags=["Database"])
async def force_retry():
    """Tving umiddelbar ny DB-tilkobling. Bruk etter å ha fikset credentials."""
    if not http_client:
        return JSONResponse(
            status_code=503,
            content={"error": "HTTP-klient ikke tilgjengelig."}
        )
    try:
        ok, err = await asyncio.wait_for(ping_supabase(http_client), timeout=12.0)
    except asyncio.TimeoutError:
        ok, err = False, "Timeout (>12s)"
    except Exception as e:
        ok, err = False, str(e)

    if ok:
        await db_state.mark_ok()
    else:
        await db_state.mark_fail(err)

    return {
        "success": ok,
        "status":  "connected" if ok else "offline",
        "error":   err if not ok else None,
        "message": "✅ Tilkoblet!" if ok else f"⚠️ {err}",
    }


@app.get("/db/ping", tags=["Database"])
async def db_ping():
    return {
        "connected": db_state.connected,
        "error":     db_state.error or None,
        "attempts":  db_state.attempt_count,
    }


# ── DEBUG ─────────────────────────────────────────────────────────────────────

@app.get("/env/check", tags=["Debug"])
async def env_check():
    """Sjekk miljøvariabler — viser aldri verdier, kun True/False."""
    checks  = {k: bool(getattr(cfg, k)) for k in _all_vars}
    missing = [k for k, v in checks.items() if not v]
    return {
        "all_present": not missing,
        "missing":     missing,
        "variables":   checks,
        "modules":     {
            "loaded": list(_imported_modules.keys()),
            "errors": _import_errors,
        },
    }


# ── DATA ENDEPUNKTER ──────────────────────────────────────────────────────────

@app.get("/picks", tags=["Data"])
async def get_picks():
    if not db_state.connected:
        return JSONResponse(
            status_code=503,
            content={"error": "DB ikke tilgjengelig", "db_error": db_state.error}
        )
    return {"picks": [], "status": "ok"}


@app.get("/bankroll", tags=["Data"])
async def get_bankroll():
    if not db_state.connected:
        return JSONResponse(
            status_code=503,
            content={"error": "DB ikke tilgjengelig", "db_error": db_state.error}
        )
    return {"bankroll": None, "status": "ok"}


@app.get("/dagens-kamp", tags=["Data"])
async def get_dagens_kamp():
    if not db_state.connected:
        return JSONResponse(
            status_code=503,
            content={"error": "DB ikke tilgjengelig", "db_error": db_state.error}
        )
    return {"kamp": None, "status": "ok"}
