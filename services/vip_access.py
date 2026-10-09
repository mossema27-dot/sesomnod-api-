"""VIP access: free Inner Circle for allowlisted emails — only after verified login.

Flow: user signs in on the frontend with a Supabase magic link / OTP sent to her
inbox (Supabase sends the mail). The frontend calls GET /vip/entitlement with the
Supabase access token. We verify that token server-side against Supabase
(/auth/v1/user), require a confirmed email, and look the email up in `vip_access`.
Typing someone's email is therefore not enough: you must control the inbox.

No Stripe changes. Router is only mounted when VIP_ACCESS_ENABLED=1.
"""
from __future__ import annotations

import os
from typing import Any, Awaitable, Callable, Optional

from fastapi import APIRouter, Header
from fastapi.responses import JSONResponse

TIER = "inner_circle"
SOURCE = "VIP — gratis, satt av Don"

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
