"""
Sniper skyggeregel for tynt datagrunnlag (SHADOW_THIN). Ingen nettverk, ingen DB.
Kjør: python3 -m pytest tests/test_sniper_thin_data.py -q   (Python ≥ 3.10; krever httpx + pandas som i prod)

Dekker: selve regelen og grensen, at skyggepicks lagres med riktig tier gjennom generate_picks, og at de ikke kan
lekke: ingen Telegram, ikke Brain-kandidat, ikke i noen /public/-spørring eller tier-liste utenfor sniper_live.py.
"""
import ast
import asyncio
import sys
import unittest
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services import sniper_live as S  # noqa: E402
from services.oraklion_brain import rules as R  # noqa: E402

LIMIT = S.MIN_TEAM_MATCHES_FOR_PRIMARY
THIN = S.SHADOW_THIN_TIER
PRIMARY = ("PRIMARY", True, None)
BIG5_LOW = ("SHADOW_BIG5", True, "BIG5_LOWER_EDGE")
GLOBAL = ("SHADOW_GLOBAL", False, "GLOBAL_UNCALIBRATED")


class TestRule(unittest.TestCase):
    def test_limit_is_one_positive_int_constant(self):
        self.assertIsInstance(LIMIT, int)
        self.assertGreaterEqual(LIMIT, 1)

    def test_both_teams_at_or_over_limit_keep_their_tier(self):
        for c in (PRIMARY, BIG5_LOW):
            self.assertEqual(S._apply_thin_data_rule(c, LIMIT, LIMIT), c)
            self.assertEqual(S._apply_thin_data_rule(c, LIMIT, 5000), c)

    def test_one_team_under_limit_becomes_shadow_thin(self):
        for c in (PRIMARY, BIG5_LOW):
            for home, away in ((LIMIT - 1, 5000), (5000, LIMIT - 1), (LIMIT - 1, LIMIT - 1), (0, 5000)):
                self.assertEqual(S._apply_thin_data_rule(c, home, away), (THIN, False, "THIN_TEAM_DATA"), (c, home, away))

    def test_unknown_sample_fails_closed(self):
        for c in (PRIMARY, BIG5_LOW):
            for home, away in ((None, 5000), (5000, None), (None, None)):
                self.assertEqual(S._apply_thin_data_rule(c, home, away), (THIN, False, "TEAM_SAMPLE_UNKNOWN"))

    def test_leagues_outside_big5_are_untouched(self):
        self.assertEqual(S._apply_thin_data_rule(GLOBAL, 0, None), GLOBAL)

    def test_thin_tier_is_never_primary_and_never_capped(self):
        self.assertNotEqual(THIN, "PRIMARY")
        self.assertNotIn(THIN, S.SHADOW_TIERS)          # utenfor SHADOW_DAILY_CAP: skal alltid logges
        self.assertLessEqual(len(THIN), 20)             # oraklion_card_drafts.market_tier er VARCHAR(20)


class TestSampleSize(unittest.TestCase):
    def test_counts_home_plus_away(self):
        import pandas as pd
        df = pd.DataFrame({"HomeTeam": ["A", "A", "B", "C"], "AwayTeam": ["B", "C", "A", "A"]})
        with mock.patch("services.football_data_fetcher.get_historical_data", return_value=df):
            self.assertEqual(S._team_match_counts(), {"A": 4, "B": 2, "C": 2})

    def test_lookup(self):
        counts = {"Le Mans": 5, "Lorient": 120}
        self.assertEqual(S._team_sample_size("Le Mans", counts), 5)
        self.assertEqual(S._team_sample_size("Lorient", counts), 120)
        self.assertIsNone(S._team_sample_size("Ukjent Lag", counts))
        self.assertIsNone(S._team_sample_size("Le Mans", {}))
        self.assertIsNone(S._team_sample_size("", counts))
        self.assertIsNone(S._team_sample_size(None, counts))


# ── generate_picks med falske kilder: hva havner faktisk i sniper_bets_v1? ──────────────────────────────────────
COUNTS = {"Arsenal": 120, "Chelsea": 120, "Lorient": 120, "Brest": 120, "Lens": 120, "Freiburg": 120, "Burnley": 80,
          "Le Mans": LIMIT - 1, "Elversberg": 1, "Troyes": LIMIT}
ODDS = 2.00   # implied 0.50 → edge = p − 0.50


class FakePool:
    def __init__(self):
        self.rows = {}

    @asynccontextmanager
    async def acquire(self):
        yield self

    async def execute(self, *a):
        return None

    async def fetchrow(self, sql, *a):
        if "INSERT INTO sniper_bets_v1" not in sql:
            return None
        self.rows[a[0]] = {"home": a[2], "away": a[3], "tier": a[-3], "is_calibrated": a[-2], "reason": a[-1]}
        return {"id": len(self.rows)}


def _fx(fid, league_id, home, away):
    ko = (datetime.now(timezone.utc) + timedelta(hours=30)).isoformat()
    return {"fixture": {"id": fid, "date": ko}, "teams": {"home": {"name": home}, "away": {"name": away}},
            "_league_id": league_id, "_league_name": f"L{league_id}"}


def _run(fixtures, probs, *, counts=COUNTS, shadow_today=0):
    pool, alerts, calls = FakePool(), [], {"counts": 0, "fixtures": 0}

    async def fixtures_once(*a, **k):
        calls["fixtures"] += 1
        return fixtures if calls["fixtures"] == 1 else []

    async def no_odds(*a, **k):
        return []

    async def dc(home, away, _mhp):
        return SimpleNamespace(fallback_used=False, over_25=probs[(home, away)], lambda_home=1.5, lambda_away=1.2)

    async def alert(_pool, payload):
        alerts.append(payload["market_tier"])
        return False

    async def not_paused(_pool):
        return False

    async def shadow_count(_pool):
        return shadow_today

    async def no_log(*a, **k):
        return None

    def team_counts():
        calls["counts"] += 1
        if isinstance(counts, Exception):
            raise counts
        return counts

    with mock.patch.object(S, "fetch_fixtures_for_leagues", fixtures_once), \
            mock.patch.object(S, "fetch_fixture_odds", no_odds), \
            mock.patch.object(S, "_parse_pinnacle_over_25", lambda r: (ODDS, {})), \
            mock.patch.object(S, "_parse_pinnacle_market_intel", lambda r: {}), \
            mock.patch.object(S, "_maybe_alert_first_picks", alert), \
            mock.patch.object(S, "is_sniper_paused", not_paused), \
            mock.patch.object(S, "_shadow_picks_today_count", shadow_count), \
            mock.patch.object(S, "_log_scan_stats", no_log), \
            mock.patch.object(S, "_team_match_counts", team_counts), \
            mock.patch("services.dixon_coles_engine.get_dixon_coles_probs", dc):
        stats = asyncio.run(S.generate_picks(pool, days_ahead=2))
    return pool.rows, stats, alerts, calls


FIXTURES = [
    _fx(1, 39, "Arsenal", "Chelsea"),       # begge kjente, edge 12 %            → PRIMARY
    _fx(2, 61, "Le Mans", "Lorient"),       # hjemmelag under grensen, edge 12 % → SHADOW_THIN
    _fx(3, 61, "Lorient", "Le Mans"),       # bortelag under grensen, edge 7 %   → SHADOW_THIN (ikke SHADOW_BIG5)
    _fx(4, 61, "Brest", "Lens"),            # begge kjente, edge 7 %             → SHADOW_BIG5
    _fx(5, 61, "Nyttlag", "Lens"),          # lag mangler i oppslaget, edge 12 % → SHADOW_THIN (ukjent)
    _fx(6, 61, "Troyes", "Lens"),           # nøyaktig på grensen, edge 12 %     → PRIMARY
    _fx(7, 40, "Burnley", "Elversberg"),    # utenfor Big5, edge 12 %            → SHADOW_GLOBAL (urørt)
    _fx(8, 78, "Elversberg", "Freiburg"),   # tynt lag, edge 35 %                → karantene, lagres ikke
    _fx(9, 78, "Freiburg", "Elversberg"),   # tynt lag, edge 3 %                 → under terskel, lagres ikke
]
PROBS = {("Arsenal", "Chelsea"): 0.62, ("Le Mans", "Lorient"): 0.62, ("Lorient", "Le Mans"): 0.57, ("Brest", "Lens"): 0.57,
         ("Nyttlag", "Lens"): 0.62, ("Troyes", "Lens"): 0.62, ("Burnley", "Elversberg"): 0.62,
         ("Elversberg", "Freiburg"): 0.85, ("Freiburg", "Elversberg"): 0.53}


class TestGeneratePicksFlow(unittest.TestCase):
    def test_every_case_lands_in_the_right_tier(self):
        rows, stats, alerts, calls = _run(FIXTURES, PROBS)
        got = {k: (v["tier"], v["is_calibrated"], v["reason"]) for k, v in rows.items()}
        self.assertEqual(got, {
            "1": ("PRIMARY", True, None),
            "2": (THIN, False, "THIN_TEAM_DATA"),
            "3": (THIN, False, "THIN_TEAM_DATA"),
            "4": ("SHADOW_BIG5", True, "BIG5_LOWER_EDGE"),
            "5": (THIN, False, "TEAM_SAMPLE_UNKNOWN"),
            "6": ("PRIMARY", True, None),
            "7": ("SHADOW_GLOBAL", False, "GLOBAL_UNCALIBRATED"),
        })
        self.assertEqual(alerts, ["PRIMARY", "PRIMARY"])     # Telegram-varsel forsøkes kun for PRIMARY
        self.assertEqual((stats["primary_created"], stats["shadow_thin_created"], stats["shadow_big5_created"],
                          stats["shadow_global_created"], stats["quarantined_high_edge"], stats["low_edge"],
                          stats["picks_created"]), (2, 3, 1, 1, 1, 1, 7))
        self.assertEqual(calls["counts"], 1)                  # datasettet telles én gang per skann

    def test_count_failure_fails_closed_and_is_tried_once(self):
        rows, stats, alerts, calls = _run(FIXTURES, PROBS, counts=RuntimeError("football-data nede"))
        big5 = {k: v["tier"] for k, v in rows.items() if k != "7"}
        self.assertEqual(set(big5.values()), {THIN})          # ingen PRIMARY og ingen SHADOW_BIG5 uten kjent grunnlag
        self.assertEqual({v["reason"] for k, v in rows.items() if k != "7"}, {"TEAM_SAMPLE_UNKNOWN"})
        self.assertEqual((alerts, stats["primary_created"], calls["counts"]), ([], 0, 1))

    def test_thin_picks_are_logged_even_when_the_shadow_cap_is_full(self):
        rows, stats, _alerts, _calls = _run(FIXTURES, PROBS, shadow_today=S.SHADOW_DAILY_CAP)
        self.assertEqual({k: v["tier"] for k, v in rows.items()}, {"1": "PRIMARY", "2": THIN, "3": THIN, "5": THIN, "6": "PRIMARY"})
        self.assertEqual(stats["shadow_thin_created"], 3)

    def test_without_thin_teams_nothing_changes(self):
        fx = [f for f in FIXTURES if f["fixture"]["id"] in (1, 4, 6, 7)]
        rows, stats, alerts, _calls = _run(fx, PROBS)
        self.assertEqual({k: v["tier"] for k, v in rows.items()}, {"1": "PRIMARY", "4": "SHADOW_BIG5", "6": "PRIMARY", "7": "SHADOW_GLOBAL"})
        self.assertEqual((stats["shadow_thin_created"], alerts), (0, ["PRIMARY", "PRIMARY"]))


# ── ingen lekkasje: ikke Telegram, ikke Brain, ikke /public, ikke i andre tier-lister ───────────────────────────
class TestNoLeak(unittest.TestCase):
    def test_real_alert_function_refuses_before_touching_db_or_network(self):
        payload = {"market_tier": THIN, "home_team": "Le Mans", "away_team": "Lorient"}
        self.assertFalse(asyncio.run(S._maybe_alert_first_picks(None, payload)))   # pool=None: ville krasjet ved DB-bruk

    def test_brain_never_commits_a_thin_pick(self):
        now = datetime(2026, 10, 9, 12, 0, tzinfo=timezone.utc)
        cfg = R.BrainConfig()
        self.assertEqual(cfg.candidate_tiers, ("PRIMARY",))
        c = R.Candidate(source_table="sniper_bets_v1", source_id=1, fixture_id="1", league="L", home="H", away="A",
                        kickoff_utc=now + timedelta(hours=20), market_key="OU|2.5|OVER", p_model=0.70, odds=2.00,
                        odds_ts_utc=now - timedelta(minutes=5), odds_ts_basis="column", tier=THIN)
        self.assertEqual(R.evaluate(c, now, cfg), "TIER_NOT_ALLOWED")
        sel = R.select([c], now, cfg)
        self.assertEqual((sel.chosen, sel.passed_n, sel.skip_counts), (None, 0, {"TIER_NOT_ALLOWED": 1}))
        seed = (ROOT / "migrations" / "2026-09-15_brain_v2_up.sql").read_text(encoding="utf-8")
        self.assertIn("('CANDIDATE_TIERS',          'PRIMARY')", seed)

    def test_tier_name_is_used_nowhere_but_in_the_sniper(self):
        """Alle andre lesere velger tier eksplisitt ('PRIMARY', PRIMARY ∪ SHADOW_BIG5, …). Dukker SHADOW_THIN opp i en av dem,
        er det et bevisst valg som skal gjennom denne testen."""
        allowed = {"services/sniper_live.py"}
        hits = []
        for folder in ("", "services", "services/oraklion_brain", "scripts/monitoring", "signals", "core", "routers"):
            d = ROOT / folder
            if not d.is_dir():
                continue
            for f in sorted(d.glob("*.py")):
                rel = f.relative_to(ROOT).as_posix()
                if rel not in allowed and THIN in f.read_text(encoding="utf-8", errors="ignore"):
                    hits.append(rel)
        self.assertEqual(hits, [])

    def test_every_public_endpoint_reading_sniper_rows_filters_on_primary(self):
        src = (ROOT / "main.py").read_text(encoding="utf-8")
        lines = src.split("\n")
        checked = {}
        for node in ast.walk(ast.parse(src)):
            if not isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)):
                continue
            routes = [d.args[0].value for d in node.decorator_list
                      if isinstance(d, ast.Call) and d.args and isinstance(d.args[0], ast.Constant)
                      and isinstance(d.args[0].value, str) and d.args[0].value.startswith("/public/")]
            if not routes:
                continue
            body = "\n".join(lines[node.lineno - 1:node.end_lineno])
            reads = body.count("FROM sniper_bets_v1")
            if reads:
                checked[routes[0]] = (reads, body.count("market_tier = 'PRIMARY'"))
        self.assertGreaterEqual(len(checked), 4, checked)                       # upcoming-edge, eye, network, calibration
        self.assertEqual({r: v for r, v in checked.items() if v[1] < v[0]}, {})


if __name__ == "__main__":
    unittest.main()
