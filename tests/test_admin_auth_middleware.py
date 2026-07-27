"""
Unit tests for AdminAuthMiddleware protected-prefix coverage.

Verifies that /admin, /waitlist/admin, /operator, /docs, /redoc, and
/openapi.json all require a valid ADMIN_API_KEY, while other paths pass
through unauthenticated.

Runs without starlette.testclient / httpx by driving the dispatch
coroutine directly with lightweight fakes — matches the pattern used
in test_market_scanner_ingestion.py (pure module tests).
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Optional

import pytest

from services.admin_auth import AdminAuthMiddleware, PROTECTED_PREFIXES


API_KEY = "test-admin-key-xyz"


@dataclass
class _FakeURL:
    path: str


@dataclass
class _FakeRequest:
    path: str
    headers: dict = field(default_factory=dict)

    @property
    def url(self) -> _FakeURL:
        return _FakeURL(self.path)


class _SentinelResponse:
    """Marker returned by our fake call_next so tests can prove passthrough."""

    status_code = 200
    body = b"ok"


async def _passthrough(_request):
    return _SentinelResponse()


def _dispatch(request: _FakeRequest):
    mw = AdminAuthMiddleware(app=None)
    return asyncio.run(mw.dispatch(request, _passthrough))


def _body(response) -> dict:
    return json.loads(response.body)


PROTECTED_SAMPLES = [
    "/admin/anything",
    "/waitlist/admin",
    "/operator/status",
    "/docs",
    "/redoc",
    "/openapi.json",
]

UNPROTECTED_SAMPLES = [
    "/health",
    "/smartpick/123",
    "/waitlist/public",
    "/",
]


@pytest.fixture(autouse=True)
def _set_key(monkeypatch):
    monkeypatch.setenv("ADMIN_API_KEY", API_KEY)


class TestProtectedPrefixes:
    @pytest.mark.parametrize("path", PROTECTED_SAMPLES)
    def test_missing_header_returns_401(self, path):
        resp = _dispatch(_FakeRequest(path=path))
        assert resp.status_code == 401
        assert _body(resp) == {"detail": "Missing Authorization header"}

    @pytest.mark.parametrize("path", PROTECTED_SAMPLES)
    def test_wrong_key_returns_403(self, path):
        req = _FakeRequest(path=path, headers={"authorization": "Bearer wrong-key"})
        resp = _dispatch(req)
        assert resp.status_code == 403
        assert _body(resp) == {"detail": "Invalid admin key"}

    @pytest.mark.parametrize("path", PROTECTED_SAMPLES)
    def test_valid_bearer_passes(self, path):
        req = _FakeRequest(path=path, headers={"authorization": f"Bearer {API_KEY}"})
        resp = _dispatch(req)
        assert isinstance(resp, _SentinelResponse)

    @pytest.mark.parametrize("path", PROTECTED_SAMPLES)
    def test_valid_raw_key_passes(self, path):
        req = _FakeRequest(path=path, headers={"authorization": API_KEY})
        resp = _dispatch(req)
        assert isinstance(resp, _SentinelResponse)


class TestUnprotectedPaths:
    @pytest.mark.parametrize("path", UNPROTECTED_SAMPLES)
    def test_passes_without_header(self, path):
        resp = _dispatch(_FakeRequest(path=path))
        assert isinstance(resp, _SentinelResponse)


class TestConfiguration:
    def test_missing_env_returns_503(self, monkeypatch):
        monkeypatch.delenv("ADMIN_API_KEY", raising=False)
        req = _FakeRequest(path="/admin/anything", headers={"authorization": "Bearer x"})
        resp = _dispatch(req)
        assert resp.status_code == 503
        assert _body(resp) == {"detail": "Admin auth not configured"}

    def test_protected_prefixes_snapshot(self):
        # Guardrail — if the tuple changes, tests must be updated too so the
        # coverage matrix above stays honest.
        assert PROTECTED_PREFIXES == (
            "/admin",
            "/waitlist/admin",
            "/operator",
            "/docs",
            "/redoc",
            "/openapi.json",
        )
