"""VIP access tests — no network, no real DB."""
import asyncio
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from services import vip_access as V  # noqa: E402

VIP = "vip@example.com"


class FakeConn:
    def __init__(self, rows): self.rows = rows
    async def fetchrow(self, q, email):
        r = self.rows.get(email)
        return r if r and not r.get("revoked_at") else None


class FakePool:
    def __init__(self, rows): self.rows = rows
    def acquire(self):
        pool = self
        class Ctx:
            async def __aenter__(self): return FakeConn(pool.rows)
            async def __aexit__(self, *a): return False
        return Ctx()


class DB:
    connected = True
    def __init__(self, rows): self.pool = FakePool(rows)


USERS = {
    "tok-vip": {"email": "VIP@Example.com", "email_confirmed_at": "2026-10-09T21:00:00Z"},
    "tok-vip-unverified": {"email": VIP, "email_confirmed_at": None},
    "tok-other": {"email": "someone@example.com", "email_confirmed_at": "2026-10-09T21:00:00Z"},
}


async def fake_verify(token):
    return USERS.get(token)


def client(rows):
    V.configure(DB(rows), verify_token=fake_verify)
    app = FastAPI()
    app.include_router(V.router)
    return TestClient(app)


ROWS = {VIP: {"email": VIP, "tier": V.TIER, "source": V.SOURCE, "granted_at": datetime(2026, 10, 9, tzinfo=timezone.utc)}}


class VipAccessTests(unittest.TestCase):
    def test_verified_vip_gets_inner_circle_with_honest_source(self):
        r = client(ROWS).get("/vip/entitlement", headers={"Authorization": "Bearer tok-vip"})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["tier"], "inner_circle")
        self.assertEqual(r.json()["source"], "VIP — gratis, satt av Don")

    def test_unverified_email_is_refused(self):
        r = client(ROWS).get("/vip/entitlement", headers={"Authorization": "Bearer tok-vip-unverified"})
        self.assertEqual(r.status_code, 403)

    def test_no_token_or_bad_token(self):
        c = client(ROWS)
        self.assertEqual(c.get("/vip/entitlement").status_code, 401)
        self.assertEqual(c.get("/vip/entitlement", headers={"Authorization": "Bearer forged"}).status_code, 401)

    def test_other_user_not_vip(self):
        r = client(ROWS).get("/vip/entitlement", headers={"Authorization": "Bearer tok-other"})
        self.assertEqual(r.json(), {"vip": False, "tier": None})

    def test_revoked(self):
        rows = {VIP: {**ROWS[VIP], "revoked_at": datetime.now(timezone.utc)}}
        r = client(rows).get("/vip/entitlement", headers={"Authorization": "Bearer tok-vip"})
        self.assertEqual(r.json()["vip"], False)

    def test_empty_allowlist_by_default(self):
        r = client({}).get("/vip/entitlement", headers={"Authorization": "Bearer tok-vip"})
        self.assertEqual(r.json()["vip"], False)

    def test_disabled_unless_flag(self):
        import os
        os.environ.pop("VIP_ACCESS_ENABLED", None)
        self.assertFalse(V.enabled())


if __name__ == "__main__":
    unittest.main()


import os  # noqa: E402
os.environ.setdefault("VIP_SESSION_SECRET", "x" * 40)


class CodeConn:
    def __init__(self, rows): self.rows = rows
    async def fetch(self, q):
        return [r for r in self.rows.values() if not r.get("revoked_at") and not r.get("code_revoked_at") and r.get("code_hash")]
    async def fetchrow(self, q, email):
        r = self.rows.get(email)
        return r if r and not r.get("revoked_at") and not r.get("code_revoked_at") else None


class CodeDB:
    connected = True
    def __init__(self, rows):
        class P:
            def acquire(_):
                class C:
                    async def __aenter__(_): return CodeConn(rows)
                    async def __aexit__(_, *a): return False
                return C()
        self.pool = P()


class VipCodeTests(unittest.TestCase):
    def setUp(self):
        V._attempts.clear()
        self.code = V.generate_code()
        self.rows = {VIP: {"email": VIP, "tier": V.TIER, "code_hash": V.hash_code(self.code)}}
        V.configure(CodeDB(self.rows), verify_token=fake_verify)
        app = FastAPI(); app.include_router(V.router)
        self.c = TestClient(app)

    def test_format(self):
        import re
        self.assertRegex(self.code, r"^VIP-[A-HJKMNP-Z2-9]{4}-[A-HJKMNP-Z2-9]{4}$")
        self.assertFalse(re.search(r"[01OIL]", self.code[4:]))

    def test_hash_not_plaintext_and_verify(self):
        h = self.rows[VIP]["code_hash"]
        self.assertNotIn(self.code[4:8], h)
        self.assertTrue(V.verify_code(self.code.lower().replace("-", " "), h))
        self.assertFalse(V.verify_code("VIP-AAAA-AAAA", h))

    def test_correct_code_gives_long_session(self):
        r = self.c.post("/vip/code", json={"code": self.code})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["source"], "VIP-kode — gratis, satt av Don, første VIP-kunde")
        e = self.c.get("/vip/entitlement", headers={"Authorization": f"Bearer {r.json()['token']}"})
        self.assertEqual(e.json()["tier"], "inner_circle")

    def test_incorrect_code(self):
        self.assertEqual(self.c.post("/vip/code", json={"code": "VIP-AAAA-AAAA"}).status_code, 401)

    def test_revoked_code(self):
        tok = self.c.post("/vip/code", json={"code": self.code}).json()["token"]
        self.rows[VIP]["code_revoked_at"] = datetime.now(timezone.utc)
        self.assertEqual(self.c.post("/vip/code", json={"code": self.code}).status_code, 401)
        self.assertEqual(self.c.get("/vip/entitlement", headers={"Authorization": f"Bearer {tok}"}).status_code, 401)

    def test_rate_limit(self):
        for _ in range(V.RATE_MAX):
            self.c.post("/vip/code", json={"code": "VIP-AAAA-AAAA"})
        self.assertEqual(self.c.post("/vip/code", json={"code": self.code}).status_code, 429)

    def test_tampered_session(self):
        tok = V.issue_session(VIP)
        self.assertIsNone(V.read_session(tok[:-2] + "xx"))
        self.assertIsNone(V.read_session(V.issue_session(VIP, now=time.time() - 400 * 86400)))


import time  # noqa: E402
