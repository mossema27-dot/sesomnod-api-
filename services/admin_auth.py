"""
AdminAuthMiddleware — beskytter admin- og docs-endepunkter med ADMIN_API_KEY.

Sjekker Authorization: Bearer <key> header mot os.environ["ADMIN_API_KEY"].
Returnerer 401 hvis nøkkel mangler, 403 hvis feil.
Ubeskyttede ruter passerer uendret.
"""
from __future__ import annotations

import os
import logging

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

logger = logging.getLogger("sesomnod.admin_auth")

PROTECTED_PREFIXES: tuple[str, ...] = (
    "/admin",
    "/waitlist/admin",
    "/operator",
    "/docs",
    "/redoc",
    "/openapi.json",
    # Legacy sunset: pre-mai aggregater og in-sample backtest-tall.
    # Trukket fra offentlig visning — /public/oraklion/* er eneste kilde.
    # Merk: /dagens-kamp og /v3/dagens-kamp er BEVISST utelatt; de har
    # 24t-freshness-gate og svarer ærlig med stale:true.
    "/ladder-history",
    "/dashboard/stats",
    "/proof",
    "/clv",
    "/backtest",
    "/v3/prism",
    "/v3/swarm-intelligence",
)


class AdminAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if not any(path.startswith(prefix) for prefix in PROTECTED_PREFIXES):
            return await call_next(request)

        api_key = os.environ.get("ADMIN_API_KEY")
        if not api_key:
            logger.error("[AdminAuth] ADMIN_API_KEY not set in environment")
            return JSONResponse(
                status_code=503,
                content={"detail": "Admin auth not configured"},
            )

        auth_header = request.headers.get("authorization", "")
        if not auth_header:
            return JSONResponse(
                status_code=401,
                content={"detail": "Missing Authorization header"},
            )

        # Accept "Bearer <key>" or raw key
        token = auth_header.removeprefix("Bearer ").strip()
        if token != api_key:
            logger.warning("[AdminAuth] Invalid key attempt on %s", request.url.path)
            return JSONResponse(
                status_code=403,
                content={"detail": "Invalid admin key"},
            )

        return await call_next(request)
