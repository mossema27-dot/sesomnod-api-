"""
AdminAuthMiddleware — beskytter alle /admin/* endepunkter med ADMIN_API_KEY.

Sjekker Authorization: Bearer <key> header mot os.environ["ADMIN_API_KEY"].
Returnerer 401 hvis nøkkel mangler, 403 hvis feil.
Non-admin ruter passerer uendret.
"""
from __future__ import annotations

import os
import logging

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

logger = logging.getLogger("sesomnod.admin_auth")


class AdminAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if not request.url.path.startswith("/admin"):
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
