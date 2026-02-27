"""
SesomNod Engine v6.0 - PRODUCTION READY
- Starter alltid, krasjer aldri
- Healthcheck returnerer alltid 200
- Auto-reconnect til Supabase
- Alle miljøvariabler renses for usynlige tegn
"""

import asyncio
import logging
import os
import sys
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Optional

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
# KONFIGURASJON
# ─────────────────────────────────────────────────────────────────────────────

def clean_env(key: str) -> str:
    """
    Henter miljøvariabel og fjerner ALLE usynlige tegn.
    Dette fikser det opprinnelige linjeskift-problemet som
    forårsaket 'Invalid non-printable ASCII character' i Railway.
    """
    value = os.getenv(key, "")
    if value:
        value = (
            value
            .strip()
            .replace("\n", "")
            .replace("\r", "")
            .replace("\t", "")
            .replace("\x00", "")
            .replace("\ufeff", "")
        )
    return value


class Config:
    SUPABASE_URL:         str = clean_env("SUPABASE_URL")
    SUPABASE_PAT:         str = clean_env("SUPABASE_PAT")
    SUPABASE_PROJECT:     str = clean_env("SUPABASE_PROJECT")
    SUPABASE_ANON_KEY:    str = clean_env("SUPABASE_ANON_KEY")
    SUPABASE_SERVICE_KEY: str = clean_env("SUPABASE_SERVICE_KEY")
    DATABASE_URL:         str = clean_env("DATABASE_URL")
    TELEGRAM_TOKEN:       str = clean_env("TELEGRAM_TOKEN")
    TELEGRAM_CHAT_ID:     str = clean_env("TELEGRAM_CHAT_ID")
    ODDS_API_KEY:         str = clean_env("ODDS_API_KEY")
    PORT:                 int = int(os.getenv("PORT", "8000"))
    ENVIRONMENT:          str = os.getenv("RAILWAY_ENVIRONMENT", "development")
    SERVICE_NAME:         str = os.getenv("RAILWAY_SERVICE_NAME", "sesomnod-api")


cfg = Config()

# Log status ved oppstart
_present = [k for k in [
    "SUPABASE_URL", "SUPABASE_PAT", "SUPABASE_PROJECT",
    "SUPABASE_ANON_KEY", "SUPABASE_SERVICE_KEY", "DATABASE_URL",
    "TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID", "ODDS_API_KEY"
] if getattr(cfg, k)]

_missing = [k for k in [
    "SUPABASE_URL", "SUPABASE_PAT", "SUPABASE_PROJECT",
    "SUPABASE_ANON_KEY", "SUPABASE_SERVICE_KEY", "DATABASE_URL",
    "TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID", "ODDS_API_KEY"
] if not getattr(cfg, k)]

if _present:
    logger.info(f"[Config] Lastet: {', '.join(_present)}")
if _missing:
    logger.warning(f"[Config] Mangler: {', '.join(_missing)}")


# ─────────────────────────────────────────────────────────────────────────────
# DATABASE STATE
# ─────────────────────────────────────────────────────────────────────────────

class DBState:
    """Trådsikker tilstandsmaskin for database-tilkobling."""

    def __init__(self):
        self.connected:            bool  = False
        self.error:                str   = ""
        self.attempt_count:        int   = 0
        self.consecutive_failures: int   = 0
        self.last_check:           float = 0.0
        self.last_success:         float = 0.0
        self._lock = asyncio.Lock()

    async def mark_connected(self):
        async with self._lock:
            was_offline = not self.connected
            self.connected            = True
            self.error                = ""
            self.last_check           = time.time()
            self.last_success         = time.time()
            self.consecutive_failures = 0
            if was_offline:
                logger.info("[DB] ✅ Supabase tilkoblet og fungerer!")

    async def mark_failed(self, error: str):
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

async def check_supabase(client: httpx.AsyncClient) -> tuple[bool, str]:
    """
    Sjekker Supabase REST API.
    Bruker service_role key (korrekt for backend).
    Returnerer (success, error_message).
    Krasjer ALDRI — alle exceptions fanges.
    """
    # Bruk beste tilgjengelige nøkkel
    api_key = cfg.SUPABASE_SERVICE_KEY or cfg.SUPABASE_ANON_KEY or cfg.SUPABASE_PAT

    if not cfg.SUPABASE_URL:
        return False, "SUPABASE_URL er ikke satt i Railway Variables"
    if not api_key:
        return False, "Ingen API-nøkkel funnet — sett SUPABASE_SERVICE_KEY i Railway Variables"

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
                "401 Unauthorized — Feil API-nøkkel. "
                "Gå til Supabase → Settings → API Keys → Legacy → kopier service_role → "
                "oppdater SUPABASE_SERVICE_KEY i Railway Variables"
            )
        elif response.status_code == 404:
            return False, f"404 Not Found — Sjekk SUPABASE_URL: {cfg.SUPABASE_URL}"
        else:
            return False, f"HTTP {response.status_code}: {response.text[:200]}"

    except httpx.ConnectTimeout:
        return False, "Timeout — Supabase svarte ikke innen 8 sekunder"
    except httpx.ConnectError as e:
        return False, f"Kan ikke nå Supabase: {str(e)[:100]}"
    except httpx.HTTPError as e:
        return False, f"HTTP-feil: {str(e)[:100]}"
    except Exception as e:
        logger.exception("[DB] Uventet feil i check_supabase")
        return False, f"Uventet feil: {type(e).__name__}: {str(e)[:100]}"


# ─────────────────────────────────────────────────────────────────────────────
# BAKGRUNNS RECONNECT LOOP
# ─────────────────────────────────────────────────────────────────────────────

async def reconnect_loop(client: httpx.AsyncClient) -> None:
    """
    Kjører stille i bakgrunnen for alltid.

    Retry-strategi (eksponentiell backoff):
      Forsøk 1: vent  5s
      Forsøk 2: vent 10s
      Forsøk 3: vent 20s
      Forsøk 4: vent 40s
      Forsøk 5+: vent 60s (maks)

    Når tilkoblet: pinger Supabase hvert 30. sekund
    for å oppdage hvis tilkoblingen faller ut.
    """
    BASE_DELAY   = 5
    MAX_DELAY    = 60
    HEALTHY_POLL = 30

    logger.info("[DB] Bakgrunns-reconnect loop startet.")

    while True:
        try:
            db_state.attempt_count += 1
            ok, err = await check_supabase(client)

            if ok:
                await db_state.mark_connected()
                await asyncio.sleep(HEALTHY_POLL)
            else:
                await db_state.mark_failed(err)
                failures = db_state.consecutive_failures
                delay = min(BASE_DELAY * (2 ** (failures - 1)), MAX_DELAY)

                # Logg kun de første 3 feilene og deretter hvert 5.
                if failures <= 3 or failures % 5 == 0:
                    logger.warning(
                        f"[DB] Offline (forsøk #{db_state.attempt_count}, "
                        f"{failures} på rad) — {err} — prøver om {delay}s"
                    )
                await asyncio.sleep(delay)

        except asyncio.CancelledError:
            logger.info("[DB] Reconnect loop avsluttet.")
            break
        except Exception as e:
            logger.exception(f"[DB] Uventet feil i reconnect loop: {e}")
            await asyncio.sleep(30)


# ─────────────────────────────────────────────────────────────────────────────
# LIFESPAN — Oppstart og nedstenging
# ─────────────────────────────────────────────────────────────────────────────

http_client: Optional[httpx.AsyncClient] = None
bg_reconnect: Optional[asyncio.Task]     = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client, bg_reconnect

    # ── BANNER ──────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("  SesomNod Engine v6.0 - PRODUCTION")
    logger.info(f"  Miljo:   {cfg.ENVIRONMENT}")
    logger.info(f"  Service: {cfg.SERVICE_NAME}")
    logger.info(f"  Port:    {cfg.PORT}")
    logger.info("=" * 60)

    # ── HTTP KLIENT ──────────────────────────────────────────────────────────
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

    # ── FØRSTE DB SJEKK ──────────────────────────────────────────────────────
    # Ikke-blokkerende: gir opp etter 10 sekunder og starter uansett
    logger.info("[DB] Prøver første Supabase-tilkobling (timeout 10s)...")
    try:
        ok, err = await asyncio.wait_for(
            check_supabase(http_client),
            timeout=10.0
        )
        if ok:
            await db_state.mark_connected()
        else:
            await db_state.mark_failed(err)
            logger.warning(f"[DB] Starter i offline mode: {err}")
            logger.warning("[DB] Bakgrunns-reconnect vil prøve automatisk.")
    except asyncio.TimeoutError:
        await db_state.mark_failed("Timeout ved første tilkobling (>10s)")
        logger.warning("[DB] Timeout ved oppstart — starter uten DB")
    except Exception as e:
        await db_state.mark_failed(str(e))
        logger.warning(f"[DB] Feil ved oppstart: {e}")

    # ── BAKGRUNNS RECONNECT ──────────────────────────────────────────────────
    bg_reconnect = asyncio.create_task(
        reconnect_loop(http_client),
        name="db-reconnect"
    )
    logger.info("[BG] Bakgrunns-reconnect startet.")

    # ── APPEN ER KLAR ─────────────────────────────────────────────────────────
    mode = "FULL DATABASE MODE" if db_state.connected else "OFFLINE MODE"
    logger.info(f"[APP] ✅ SesomNod Engine KLAR! ({mode})")
    logger.info(f"[APP] Healthcheck: /health | Status: /status | Docs: /docs")
    logger.info("=" * 60)

    # ── YIELD: APPEN KJORER ──────────────────────────────────────────────────
    yield

    # ── SHUTDOWN ─────────────────────────────────────────────────────────────
    logger.info("[APP] Nedstenging påbegynt...")

    if bg_reconnect and not bg_reconnect.done():
        bg_reconnect.cancel()
        try:
            await asyncio.wait_for(bg_reconnect, timeout=3.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
        logger.info("[BG] Bakgrunnsoppgave avsluttet.")

    if http_client:
        await http_client.aclose()
        logger.info("[HTTP] Klient lukket.")

    logger.info("[APP] SesomNod Engine stoppet rent. 👋")


# ─────────────────────────────────────────────────────────────────────────────
# FASTAPI APP
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="SesomNod Engine",
    description="SesomNod Engine — bygget for å aldri krasje.",
    version="6.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# CORS — tillat alle origins nå, stram inn når frontend er klar
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── REQUEST LOGGING MIDDLEWARE ────────────────────────────────────────────────
@app.middleware("http")
async def request_middleware(request: Request, call_next):
    request_id = str(uuid.uuid4())[:8]
    start_time = time.perf_counter()
    is_silent  = request.url.path in ("/health", "/")

    if not is_silent:
        logger.info(f"[{request_id}] → {request.method} {request.url.path}")

    try:
        response = await call_next(request)
        duration_ms = (time.perf_counter() - start_time) * 1000

        if not is_silent:
            logger.info(f"[{request_id}] ← {response.status_code} ({duration_ms:.0f}ms)")

        response.headers["X-Request-ID"]    = request_id
        response.headers["X-Response-Time"] = f"{duration_ms:.0f}ms"
        return response

    except Exception as exc:
        duration_ms = (time.perf_counter() - start_time) * 1000
        logger.error(f"[{request_id}] 💥 FEIL: {exc} ({duration_ms:.0f}ms)")
        return JSONResponse(
            status_code=500,
            content={
                "error":      "Internal Server Error",
                "request_id": request_id,
                "message":    str(exc),
            }
        )


# ─────────────────────────────────────────────────────────────────────────────
# ENDEPUNKTER
# ─────────────────────────────────────────────────────────────────────────────

# ── SYSTEM ────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["System"], summary="Railway Healthcheck")
async def health():
    """
    Railway bruker dette endepunktet for å sjekke at appen lever.
    RETURNERER ALLTID HTTP 200 — uansett om DB er oppe eller nede.
    DB-status er informasjon, ikke en fatal feil.
    """
    return JSONResponse(
        status_code=200,
        content={
            "status":    "ok",
            "version":   "6.0.0",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "db":        db_state.to_dict(),
            "mode":      "full" if db_state.connected else "offline",
        }
    )


@app.get("/", tags=["System"], summary="Rot-endepunkt")
async def root():
    return {
        "engine":    "SesomNod Engine",
        "version":   "6.0.0",
        "status":    "running",
        "db_status": "connected" if db_state.connected else "offline",
        "links": {
            "health": "/health",
            "status": "/status",
            "docs":   "/docs",
        }
    }


@app.get("/status", tags=["System"], summary="Detaljert systemstatus")
async def detailed_status():
    """Full oversikt over systemtilstand. Brukes til debugging."""
    return {
        "engine": {
            "name":        "SesomNod Engine",
            "version":     "6.0.0",
            "environment": cfg.ENVIRONMENT,
            "service":     cfg.SERVICE_NAME,
            "timestamp":   datetime.now(timezone.utc).isoformat(),
        },
        "database": db_state.to_dict(),
        "config": {
            "SUPABASE_URL":         cfg.SUPABASE_URL or None,
            "SUPABASE_PAT":         bool(cfg.SUPABASE_PAT),
            "SUPABASE_PROJECT":     bool(cfg.SUPABASE_PROJECT),
            "SUPABASE_ANON_KEY":    bool(cfg.SUPABASE_ANON_KEY),
            "SUPABASE_SERVICE_KEY": bool(cfg.SUPABASE_SERVICE_KEY),
            "DATABASE_URL":         bool(cfg.DATABASE_URL),
            "TELEGRAM_TOKEN":       bool(cfg.TELEGRAM_TOKEN),
            "TELEGRAM_CHAT_ID":     bool(cfg.TELEGRAM_CHAT_ID),
            "ODDS_API_KEY":         bool(cfg.ODDS_API_KEY),
        },
        "background_task": {
            "running": bg_reconnect is not None and not bg_reconnect.done(),
        }
    }


# ── DATABASE ──────────────────────────────────────────────────────────────────

@app.post("/db/retry", tags=["Database"], summary="Tving ny DB-tilkobling")
async def force_db_retry():
    """
    Tvinger en umiddelbar ny tilkoblingsforsøk mot Supabase.
    Bruk dette etter at du har fikset credentials i Railway Variables.
    Ingen restart nødvendig.
    """
    if not http_client:
        return JSONResponse(
            status_code=503,
            content={"error": "HTTP-klient ikke klar ennå."}
        )

    logger.info("[DB] Manuelt retry-forsøk via API...")

    try:
        ok, err = await asyncio.wait_for(
            check_supabase(http_client),
            timeout=12.0
        )
    except asyncio.TimeoutError:
        ok, err = False, "Timeout (>12s)"

    if ok:
        await db_state.mark_connected()
    else:
        await db_state.mark_failed(err)
        logger.warning(f"[DB] Manuelt retry feilet: {err}")

    return {
        "success":   ok,
        "db_status": "connected" if ok else "offline",
        "error":     err if not ok else None,
        "message":   "✅ Tilkoblet!" if ok else f"⚠️ Feilet: {err}",
        "next_step": (
            "Gå til Supabase → Settings → API Keys → Legacy fanen → "
            "kopier service_role nøkkelen → oppdater SUPABASE_SERVICE_KEY "
            "i Railway Variables → kall /db/retry igjen"
        ) if not ok and "401" in (err or "") else None,
    }


@app.get("/db/ping", tags=["Database"], summary="Rask DB-statussjekk")
async def db_ping():
    """Returnerer nåværende DB-status uten nytt tilkoblingsforsøk."""
    return {
        "connected": db_state.connected,
        "error":     db_state.error or None,
        "attempts":  db_state.attempt_count,
    }


# ── DEBUG ─────────────────────────────────────────────────────────────────────

@app.get("/env/check", tags=["Debug"], summary="Sjekk miljøvariabler")
async def env_check():
    """
    Sjekker hvilke Railway Variables som er satt.
    Viser ALDRI verdiene — kun True/False per variabel.
    """
    checks = {
        "SUPABASE_URL":         bool(cfg.SUPABASE_URL),
        "SUPABASE_PAT":         bool(cfg.SUPABASE_PAT),
        "SUPABASE_PROJECT":     bool(cfg.SUPABASE_PROJECT),
        "SUPABASE_ANON_KEY":    bool(cfg.SUPABASE_ANON_KEY),
        "SUPABASE_SERVICE_KEY": bool(cfg.SUPABASE_SERVICE_KEY),
        "DATABASE_URL":         bool(cfg.DATABASE_URL),
        "TELEGRAM_TOKEN":       bool(cfg.TELEGRAM_TOKEN),
        "TELEGRAM_CHAT_ID":     bool(cfg.TELEGRAM_CHAT_ID),
        "ODDS_API_KEY":         bool(cfg.ODDS_API_KEY),
    }
    missing = [k for k, v in checks.items() if not v]

    return {
        "all_present": len(missing) == 0,
        "missing":     missing,
        "variables":   checks,
        "tip": (
            f"Legg til disse i Railway Variables: {', '.join(missing)}"
            if missing else
            "🎉 Alle variabler er på plass!"
        ),
    }


# ── DATA ENDEPUNKTER ──────────────────────────────────────────────────────────

@app.get("/picks", tags=["Data"], summary="Hent dagens picks")
async def get_picks():
    """Henter picks fra Supabase-databasen."""
    if not db_state.connected:
        return JSONResponse(
            status_code=503,
            content={
                "error":     "Database ikke tilgjengelig",
                "db_error":  db_state.error,
                "message":   "Kall /db/retry for å prøve å koble til igjen",
            }
        )
    # TODO: Implementer spørring mot picks-tabellen
    return {"picks": [], "status": "ok", "note": "Klar for implementasjon"}


@app.get("/bankroll", tags=["Data"], summary="Hent bankroll-status")
async def get_bankroll():
    """Henter bankroll fra Supabase-databasen."""
    if not db_state.connected:
        return JSONResponse(
            status_code=503,
            content={"error": "Database ikke tilgjengelig", "db_error": db_state.error}
        )
    # TODO: Implementer spørring mot bankroll-tabellen
    return {"bankroll": None, "status": "ok", "note": "Klar for implementasjon"}


@app.get("/dagens-kamp", tags=["Data"], summary="Hent dagens kamp")
async def get_dagens_kamp():
    """Henter dagens kamp fra Supabase-databasen."""
    if not db_state.connected:
        return JSONResponse(
            status_code=503,
            content={"error": "Database ikke tilgjengelig", "db_error": db_state.error}
        )
    # TODO: Implementer spørring mot dagens_kamp-tabellen
    return {"kamp": None, "status": "ok", "note": "Klar for implementasjon"}
