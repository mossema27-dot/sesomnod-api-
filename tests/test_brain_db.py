"""
Oraklion Brain v2 — databasetester (T1, T2, T10-SQL, T11, T15, T19-del).
Krever BRAIN_TEST_DSN (lokal Postgres, ALDRI Railway) og asyncpg.
Uten DSN: alle tester hoppes over og rapporteres som NO_SOURCE_ACCESS.

  BRAIN_TEST_DSN=postgresql://localhost/brain_test python3 -m unittest tests/test_brain_db.py -v

Testen kjører migreringen (up) i et eget schema-navn? Nei: migreringen er
oraklion-schema-bundet; testdatabasen skal være en TOM lokal database.
"""
import asyncio
import os
import sys
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

DSN = os.environ.get("BRAIN_TEST_DSN")
try:
    import asyncpg  # noqa: F401
    HAS_ASYNCPG = True
except Exception:  # noqa: BLE001
    HAS_ASYNCPG = False

SKIP = None
if not DSN:
    SKIP = "NO_SOURCE_ACCESS: BRAIN_TEST_DSN ikke satt"
elif not HAS_ASYNCPG:
    SKIP = "NO_SOURCE_ACCESS: asyncpg ikke installert"
elif "railway" in DSN:
    SKIP = "NEKTET: tester kjøres aldri mot Railway"

UP = (ROOT / "migrations" / "2026-09-15_brain_v2_up.sql").read_text()
DOWN = (ROOT / "migrations" / "2026-09-15_brain_v2_down.sql").read_text()
NOW = datetime(2026, 9, 15, 17, 0, tzinfo=timezone.utc)

SNIPER_DDL = """
CREATE TABLE IF NOT EXISTS sniper_bets_v1 (
    id BIGSERIAL PRIMARY KEY, match_id TEXT NOT NULL, league TEXT NOT NULL,
    home_team TEXT NOT NULL, away_team TEXT NOT NULL, kickoff_time TIMESTAMPTZ NOT NULL,
    pick_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(), market TEXT NOT NULL DEFAULT 'OVER_2_5',
    model_prob FLOAT NOT NULL, market_implied_prob FLOAT NOT NULL, edge_pct FLOAT NOT NULL,
    odds_open FLOAT NOT NULL, odds_open_timestamp TIMESTAMPTZ NOT NULL, odds_open_source TEXT NOT NULL DEFAULT 'pinnacle',
    odds_close FLOAT, odds_close_timestamp TIMESTAMPTZ, close_capture_minutes_before INT, clv_source TEXT,
    home_goals INT, away_goals INT, total_goals INT, result TEXT, settled_at TIMESTAMPTZ,
    fixture_status TEXT, market_tier TEXT DEFAULT 'PRIMARY', created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(match_id, market)
);
"""


def run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


@unittest.skipIf(SKIP, SKIP or "")
class TestBrainDB(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import asyncpg
        cls.pool = run(asyncpg.create_pool(DSN, min_size=2, max_size=4))

        async def setup():
            async with cls.pool.acquire() as conn:
                await conn.execute(DOWN)
                await conn.execute("DROP TABLE IF EXISTS sniper_bets_v1; DROP TABLE IF EXISTS sniper_scan_log;")
                await conn.execute(SNIPER_DDL)
                await conn.execute(UP)
        run(setup())

    @classmethod
    def tearDownClass(cls):
        run(cls.pool.close())

    def _reset(self):
        async def r():
            async with self.pool.acquire() as conn:
                await conn.execute(DOWN)
                await conn.execute("TRUNCATE sniper_bets_v1;")
                await conn.execute(UP)
        run(r())

    async def _sniper_row_async(self, match_id="1001", ko_min=180, p=0.60, o=2.00, tier="PRIMARY", odds_age_min=5):
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                "INSERT INTO sniper_bets_v1 (match_id, league, home_team, away_team, kickoff_time, model_prob,"
                " market_implied_prob, edge_pct, odds_open, odds_open_timestamp, result, market_tier)"
                " VALUES ($1,'L','H','A',$2,$3,0.5,5.0,$4,$5,'PENDING',$6) RETURNING id;",
                match_id, NOW + timedelta(minutes=ko_min), p, o, NOW - timedelta(minutes=odds_age_min), tier)

    def _sniper_row(self, *a, **kw):
        return run(self._sniper_row_async(*a, **kw))

    def test_T19_flag_default_off(self):
        self.assertEqual(os.environ.get("BRAIN_V2_JOBS", "off").lower(), "off")

    def test_T1_concurrent_commit_exactly_one(self):
        self._reset()
        self._sniper_row("2001")
        from services.oraklion_brain import engine as E

        async def go():
            async with self.pool.acquire() as conn:
                cfg = await E.load_config(conn)
            await E.ensure_epoch(self.pool, cfg, NOW)
            r = await asyncio.gather(E.scan_and_commit(self.pool, cfg, NOW, False),
                                     E.scan_and_commit(self.pool, cfg, NOW, False))
            async with self.pool.acquire() as conn:
                n_commit = await conn.fetchval("SELECT count(*) FROM oraklion.brain_events WHERE event_type='COMMIT';")
                n_open = await conn.fetchval("SELECT count(*) FROM oraklion.brain_decisions WHERE status='OPEN';")
                problems = await conn.fetch("SELECT * FROM oraklion.brain_verify_chain();")
            return r, n_commit, n_open, problems
        r, n_commit, n_open, problems = run(go())
        self.assertEqual(sum(1 for x in r if "commit" in x), 1)
        self.assertEqual(n_commit, 1)
        self.assertEqual(n_open, 1)
        self.assertEqual(list(problems), [])

    def test_T10_sql_verify_matches_python(self):
        self._reset()
        from services.oraklion_brain import canonical as C
        from services.oraklion_brain.ledger import append_event

        async def go():
            async with self.pool.acquire() as conn:
                for i in range(5):
                    async with conn.transaction():
                        await append_event(conn, event_type="TEST", ts_utc=NOW, payload={"i": i, "p_model": 0.5 + i / 100})
                rows = [dict(r) for r in await conn.fetch(
                    "SELECT seq, prev_hash, hash, canonical FROM oraklion.brain_events ORDER BY seq;")]
                sql_problems = await conn.fetch("SELECT * FROM oraklion.brain_verify_chain();")
                # rollback gir ikke hull i seq
                try:
                    async with conn.transaction():
                        await append_event(conn, event_type="TEST", ts_utc=NOW, payload={"i": 99})
                        raise RuntimeError("force rollback")
                except RuntimeError:
                    pass
                async with conn.transaction():
                    await append_event(conn, event_type="TEST", ts_utc=NOW, payload={"i": 6})
                seqs = [r["seq"] for r in await conn.fetch("SELECT seq FROM oraklion.brain_events ORDER BY seq;")]
            return rows, sql_problems, seqs
        rows, sql_problems, seqs = run(go())
        self.assertEqual(C.verify_rows(rows), [])
        self.assertEqual(list(sql_problems), [])
        self.assertEqual(seqs, [1, 2, 3, 4, 5, 6])

    def test_T11_events_immutable(self):
        self._reset()
        from services.oraklion_brain.ledger import append_event

        async def go():
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    await append_event(conn, event_type="TEST", ts_utc=NOW, payload={})
                errs = []
                for sql in ("UPDATE oraklion.brain_events SET event_type='X' WHERE seq=1;",
                            "DELETE FROM oraklion.brain_events WHERE seq=1;",
                            "TRUNCATE oraklion.brain_events;"):
                    try:
                        await conn.execute(sql)
                        errs.append(None)
                    except Exception as e:  # noqa: BLE001
                        errs.append(getattr(e, "sqlstate", None) or type(e).__name__)
                n = await conn.fetchval("SELECT count(*) FROM oraklion.brain_events;")
            return errs, n
        errs, n = run(go())
        self.assertEqual(errs[:2], ["42501", "42501"])
        self.assertIsNotNone(errs[2], "TRUNCATE må feile")   # PG rapporterer 0A000 for trigger-blokkert TRUNCATE
        self.assertEqual(n, 1)

    def test_T2_T15_suspended_does_not_block_and_settles_from_suspended(self):
        self._reset()
        from services.oraklion_brain import engine as E
        id_a = self._sniper_row("3001", ko_min=120)

        async def go():
            async with self.pool.acquire() as conn:
                cfg = await E.load_config(conn)
            await E.ensure_epoch(self.pool, cfg, NOW)
            ra = await E.scan_and_commit(self.pool, cfg, NOW, False)
            # A utsettes
            async with self.pool.acquire() as conn:
                await conn.execute("UPDATE sniper_bets_v1 SET fixture_status='PST' WHERE id=$1;", id_a)
            t1 = NOW + timedelta(minutes=15)
            s1 = await E.settle_open(self.pool, cfg, t1)
            # B kan committes mens A er SUSPENDED
            await self._sniper_row_async("3002", ko_min=240)
            rb = await E.scan_and_commit(self.pool, cfg, t1, False)
            # A spilles likevel innen 48 t: kilden setter resultat
            t2 = t1 + timedelta(hours=20)
            async with self.pool.acquire() as conn:
                await conn.execute("UPDATE sniper_bets_v1 SET fixture_status='FT', result='WIN', settled_at=$2, "
                                   "home_goals=2, away_goals=1, total_goals=3 WHERE id=$1;", id_a, t2 - timedelta(minutes=40))
            s2 = await E.settle_open(self.pool, cfg, t2)               # første observasjon
            s3 = await E.settle_open(self.pool, cfg, t2 + timedelta(minutes=15))  # andre observasjon → SETTLED
            s4 = await E.settle_open(self.pool, cfg, t2 + timedelta(minutes=30))  # no-op (T2)
            async with self.pool.acquire() as conn:
                sts = await conn.fetch("SELECT fixture_id, status, outcome, closing_missing_reason "
                                       "FROM oraklion.brain_decisions ORDER BY seq;")
                reveals = await conn.fetchval("SELECT count(*) FROM oraklion.brain_events WHERE event_type='REVEAL';")
                problems = await conn.fetch("SELECT * FROM oraklion.brain_verify_chain();")
            return ra, s1, rb, s2, s3, s4, [dict(r) for r in sts], reveals, problems
        ra, s1, rb, s2, s3, s4, sts, reveals, problems = run(go())
        self.assertIn("commit", ra)
        self.assertEqual(s1["suspended"], 1)
        self.assertIn("commit", rb)
        self.assertEqual(s2["waiting"], 2)
        self.assertEqual(s3["settled"], 1)
        self.assertEqual(s4["settled"], 0)
        self.assertEqual(sts[0]["status"], "SETTLED")
        self.assertEqual(sts[0]["outcome"], "WIN")
        self.assertEqual(sts[0]["closing_missing_reason"], "RESCHEDULED")
        self.assertEqual(sts[1]["status"], "OPEN")
        self.assertEqual(reveals, 1)
        self.assertEqual(list(problems), [])


if __name__ == "__main__":
    unittest.main()
