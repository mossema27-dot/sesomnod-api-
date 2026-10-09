"""VIP access: free Inner Circle for allowlisted emails — only after verified login.

Flow: user signs in on the frontend with a Supabase magic link / OTP sent to her
inbox (Supabase sends the mail). The frontend calls GET /vip/entitlement with the
Supabase access token. We verify that token server-side against Supabase
(/auth/v1/user), require a confirmed email, and look the email up in `vip_access`.
Typing someone's email is therefore not enough: you must control the inbox.

No Stripe changes. Router is only mounted when VIP_ACCESS_ENABLED=1.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from typing import Any, Awaitable, Callable, Optional

from fastapi import APIRouter, Header, Request
from fastapi.responses import JSONResponse

TIER = "inner_circle"
SOURCE = "VIP — gratis, satt av Don"
CODE_SOURCE = "VIP-kode — gratis, satt av Don, første VIP-kunde"
CODE_ALPHABET = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"   # no 0/O, 1/I/L
SESSION_DAYS = 365
PBKDF2_ITER = 200_000
RATE_MAX, RATE_WINDOW = 5, 15 * 60                  # 5 attempts per 15 min per IP
_attempts: dict = {}


# ── Code helpers ────────────────────────────────────────────────
def generate_code() -> str:
    pick = lambda n: "".join(secrets.choice(CODE_ALPHABET) for _ in range(n))  # noqa: E731
    return f"VIP-{pick(4)}-{pick(4)}"


def normalize_code(code: str) -> str:
    c = "".join(ch for ch in (code or "").upper() if ch.isalnum())
    if c.startswith("VIP"):
        c = c[3:]
    return c


def hash_code(code: str, salt: Optional[bytes] = None) -> str:
    salt = salt or secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac("sha256", normalize_code(code).encode(), salt, PBKDF2_ITER)
    return f"pbkdf2${base64.b64encode(salt).decode()}${base64.b64encode(dk).decode()}"


def verify_code(code: str, stored: Optional[str]) -> bool:
    try:
        algo, salt_b64, dk_b64 = (stored or "").split("$")
        if algo != "pbkdf2":
            return False
        cand = hash_code(code, base64.b64decode(salt_b64)).split("$")[2]
        return hmac.compare_digest(cand, dk_b64)
    except ValueError:
        return False


def rate_limited(key: str, now: Optional[float] = None) -> bool:
    now = now or time.time()
    hits = [t for t in _attempts.get(key, []) if now - t < RATE_WINDOW]
    _attempts[key] = hits
    if len(hits) >= RATE_MAX:
        return True
    hits.append(now)
    return False


# ── Signed long-lived session ───────────────────────────────────
def _secret() -> bytes:
    s = os.environ.get("VIP_SESSION_SECRET")
    if not s or len(s) < 32:
        raise RuntimeError("VIP_SESSION_SECRET missing/too short")
    return s.encode()


def _b64(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode().rstrip("=")


def issue_session(email: str, now: Optional[float] = None) -> str:
    now = int(now or time.time())
    body = _b64(json.dumps({"e": email, "t": TIER, "iat": now, "exp": now + SESSION_DAYS * 86400}).encode())
    sig = _b64(hmac.new(_secret(), body.encode(), hashlib.sha256).digest())
    return f"vip.{body}.{sig}"


def read_session(token: str, now: Optional[float] = None) -> Optional[dict]:
    try:
        _, body, sig = token.split(".")
        good = _b64(hmac.new(_secret(), body.encode(), hashlib.sha256).digest())
        if not hmac.compare_digest(good, sig):
            return None
        data = json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))
        return data if data.get("exp", 0) > (now or time.time()) else None
    except (ValueError, RuntimeError):
        return None

router = APIRouter()
_db_state: Any = None
# Injectable for tests: async (token) -> supabase user dict | None
_verify_token: Optional[Callable[[str], Awaitable[Optional[dict]]]] = None


def configure(db_state: Any, verify_token: Optional[Callable] = None) -> None:
    global _db_state, _verify_token
    _db_state = db_state
    _verify_token = verify_token


def enabled() -> bool:
    return os.environ.get("VIP_ACCESS_ENABLED") == "1"


def normalize(email: Optional[str]) -> Optional[str]:
    e = (email or "").strip().lower()
    return e if e and "@" in e else None


async def supabase_user(token: str) -> Optional[dict]:
    """Ask Supabase who owns this access token. Returns None if invalid."""
    import httpx
    url, anon = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_ANON_KEY")
    if not url or not anon:
        raise RuntimeError("SUPABASE_URL / SUPABASE_ANON_KEY not configured")
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.get(f"{url.rstrip('/')}/auth/v1/user",
                        headers={"apikey": anon, "Authorization": f"Bearer {token}"})
    return r.json() if r.status_code == 200 else None


def verified_email(user: Optional[dict]) -> Optional[str]:
    """Email only counts once Supabase has confirmed it (magic link/OTP clicked)."""
    if not user or not (user.get("email_confirmed_at") or user.get("confirmed_at")):
        return None
    return normalize(user.get("email"))


async def lookup(conn, email: str) -> Optional[dict]:
    row = await conn.fetchrow(
        "SELECT email, tier, source, granted_at FROM vip_access WHERE email = $1 AND revoked_at IS NULL", email)
    return dict(row) if row else None


@router.get("/vip/entitlement")
async def entitlement(authorization: Optional[str] = Header(default=None)):
    if not authorization or not authorization.lower().startswith("bearer "):
        return JSONResponse(status_code=401, content={"error": "login required"})
    token = authorization.split(" ", 1)[1].strip()
    if token.startswith("vip."):
        return await _code_session_entitlement(token)
    try:
        user = await (_verify_token or supabase_user)(token)
    except Exception:
        return JSONResponse(status_code=503, content={"error": "auth verification unavailable"})
    if not user:
        return JSONResponse(status_code=401, content={"error": "invalid session"})
    email = verified_email(user)
    if not email:
        return JSONResponse(status_code=403, content={"error": "email not verified"})
    if _db_state is None or not getattr(_db_state, "pool", None):
        return JSONResponse(status_code=503, content={"error": "DB offline"})
    async with _db_state.pool.acquire() as conn:
        row = await lookup(conn, email)
    if not row:
        return {"vip": False, "tier": None}
    return {"vip": True, "tier": row["tier"], "source": row["source"],
            "granted_at": row["granted_at"].isoformat() if row.get("granted_at") else None}


async def _code_session_entitlement(token: str):
    data = read_session(token)
    if not data:
        return JSONResponse(status_code=401, content={"error": "invalid session"})
    if _db_state is None or not getattr(_db_state, "pool", None):
        return JSONResponse(status_code=503, content={"error": "DB offline"})
    async with _db_state.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT email, tier, code_hash FROM vip_access WHERE email = $1 AND revoked_at IS NULL "
            "AND code_revoked_at IS NULL", data["e"])
    if not row or not row["code_hash"]:
        return JSONResponse(status_code=401, content={"error": "code revoked"})
    return {"vip": True, "tier": row["tier"], "source": CODE_SOURCE}


@router.post("/vip/code")
async def redeem_code(request: Request):
    ip = (request.headers.get("x-forwarded-for") or (request.client.host if request.client else "?")).split(",")[0].strip()
    if rate_limited(ip):
        return JSONResponse(status_code=429, content={"error": "for mange forsøk, prøv igjen senere"})
    try:
        body = await request.json()
    except Exception:
        body = {}
    code = str((body or {}).get("code") or "")
    if len(normalize_code(code)) != 8:
        return JSONResponse(status_code=401, content={"error": "ugyldig kode"})
    if _db_state is None or not getattr(_db_state, "pool", None):
        return JSONResponse(status_code=503, content={"error": "DB offline"})
    async with _db_state.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT email, code_hash FROM vip_access WHERE revoked_at IS NULL AND code_revoked_at IS NULL "
            "AND code_hash IS NOT NULL")
    match = None
    for r in rows:                      # check every row (few rows) — no early exit timing hint
        if verify_code(code, r["code_hash"]):
            match = r["email"]
    if not match:
        return JSONResponse(status_code=401, content={"error": "ugyldig kode"})
    return {"token": issue_session(match), "tier": TIER, "source": CODE_SOURCE, "expires_in_days": SESSION_DAYS}
