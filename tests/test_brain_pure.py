"""
Oraklion Brain v2 — rene tester (ingen DB, ingen tredjeparts-pakker).
Kjør: python3 -m unittest tests/test_brain_pure.py -v   (eller pytest)

Dekker: T10 (hash/canonical, Python-side), T12 (utvalgsregel), T16 (odds-alder,
kickoff-vindu), T21 (odds-tid ukjent), T22 (statusoverganger), T6 (CLV null/coverage),
T20-form (event_public nøkkelsett).
"""
import json
import sys
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.oraklion_brain import canonical as C  # noqa: E402
from services.oraklion_brain import rules as R  # noqa: E402
from services.oraklion_brain import stats as S  # noqa: E402

NOW = datetime(2026, 9, 15, 17, 0, tzinfo=timezone.utc)
CFG = R.BrainConfig()


def cand(p, o, *, minutes_to_ko=180, odds_age_min=10, market="OU|2.5|OVER", tier="PRIMARY", basis="column"):
    return R.Candidate(
        source_table="sniper_bets_v1", source_id=1, fixture_id="123", league="L", home="H", away="A",
        kickoff_utc=NOW + timedelta(minutes=minutes_to_ko), market_key=market, p_model=p, odds=o,
        odds_ts_utc=(NOW - timedelta(minutes=odds_age_min)) if basis != "unknown" else None,
        odds_ts_basis=basis, tier=tier,
    )


class TestCanonical(unittest.TestCase):
    def test_canonical_is_sorted_compact_and_float_free(self):
        s = C.canonical_json({"b": 1.5, "a": {"z": 0.1234567, "y": [1, 2]}, "p_model": 0.55})
        self.assertEqual(s, '{"a":{"y":[1,2],"z":"0.123457"},"b":"1.500000","p_model":"0.550000"}')
        self.assertNotIn(" ", s)

    def test_hash_chain_and_verify(self):
        rows = []
        prev = C.GENESIS_PREV_HASH
        for seq in range(1, 6):
            pub = C.build_event_public(event_type="TEST", event_id=uuid.uuid4(), seq=seq, ts_utc=NOW,
                                       payload={"n": seq, "p_model": 0.6}, meta_private={"secret": "x"})
            canon = C.canonical_json(pub)
            h = C.compute_hash(prev, canon)
            rows.append({"seq": seq, "prev_hash": prev, "hash": h, "canonical": canon})
            prev = h
        self.assertEqual(C.verify_rows(rows), [])
        # manipulasjon i canonical → hash_mismatch OG neste rad prev_hash mismatch hvis hash rekomputeres
        tampered = [dict(r) for r in rows]
        tampered[2]["canonical"] = tampered[2]["canonical"].replace('"n":3', '"n":30')
        problems = C.verify_rows(tampered)
        self.assertIn({"seq": 3, "problem": "hash_mismatch"}, problems)
        # hull i seq
        gap = [dict(r) for r in rows if r["seq"] != 3]
        self.assertIn({"seq": 4, "problem": "seq_gap"}, C.verify_rows(gap))

    def test_event_public_shape_closed_and_private_bound(self):
        pub = C.build_event_public(event_type="COMMIT", event_id=uuid.uuid4(), seq=1, ts_utc=NOW,
                                   payload={"x": 1}, meta_private={"odds_source": "pinnacle"})
        self.assertEqual(set(pub.keys()), set(C.EVENT_PUBLIC_KEYS))
        self.assertEqual(len(pub["meta_private_hash"]), 64)
        self.assertNotIn("pinnacle", C.canonical_json(pub))
        pub2 = C.build_event_public(event_type="COMMIT", event_id=uuid.UUID(pub["event_id"]), seq=1, ts_utc=NOW,
                                    payload={"x": 1}, meta_private={"odds_source": "other"})
        self.assertNotEqual(pub["meta_private_hash"], pub2["meta_private_hash"])

    def test_naive_datetime_rejected(self):
        with self.assertRaises(ValueError):
            C.canonical_json({"t": datetime(2026, 1, 1)})


class TestRules(unittest.TestCase):
    def test_T12_selection(self):
        c1 = cand(0.62, 1.70)   # ev 0.054
        c2 = cand(0.55, 2.10)   # ev 0.155
        c3 = cand(0.48, 2.60)   # p under min
        c4 = cand(0.56, 1.60)   # ev -0.104
        sel = R.select([c1, c2, c3, c4], NOW, CFG)
        self.assertIs(sel.chosen, c2)
        self.assertEqual(sel.scanned_n, 4)
        self.assertEqual(sel.passed_n, 2)
        self.assertEqual(sel.skip_counts, {"P_BELOW_MIN": 1, "EV_BELOW_MIN": 1})

    def test_T16_lock_age_and_kickoff_window(self):
        self.assertEqual(R.evaluate(cand(0.6, 2.0, odds_age_min=91), NOW, CFG), "STALE_ODDS")
        self.assertIsNone(R.evaluate(cand(0.6, 2.0, odds_age_min=90), NOW, CFG))
        self.assertEqual(R.evaluate(cand(0.6, 2.0, minutes_to_ko=30), NOW, CFG), "KICKOFF_WINDOW")
        self.assertEqual(R.evaluate(cand(0.6, 2.0, minutes_to_ko=49 * 60), NOW, CFG), "KICKOFF_WINDOW")

    def test_T21_odds_ts_unknown_blocks(self):
        self.assertEqual(R.evaluate(cand(0.6, 2.0, basis="unknown"), NOW, CFG), "ODDS_TS_UNVERIFIED")

    def test_market_and_tier_gates(self):
        self.assertEqual(R.evaluate(cand(0.6, 2.0, market="1X2|DRAW"), NOW, CFG), "MARKET_NOT_ALLOWED")
        self.assertEqual(R.evaluate(cand(0.6, 2.0, tier="SHADOW_BIG5"), NOW, CFG), "TIER_NOT_ALLOWED")
        self.assertEqual(R.evaluate(cand(0.6, 2.70), NOW, CFG), "ODDS_OUT_OF_BAND")

    def test_T22_transitions(self):
        for old, new in [("OPEN", "SETTLED"), ("OPEN", "VOID"), ("OPEN", "SUSPENDED"),
                         ("SUSPENDED", "SETTLED"), ("SUSPENDED", "VOID")]:
            R.check_transition(old, new)
        for old, new in [("SUSPENDED", "OPEN"), ("SETTLED", "OPEN"), ("VOID", "SETTLED"), ("OPEN", "OPEN")]:
            with self.assertRaises(R.IllegalTransition):
                R.check_transition(old, new)

    def test_config_from_rows_never_lowers_silently(self):
        cfg = R.BrainConfig.from_rows({"EV_MIN": "0.07", "MARKETS": "OU|2.5|OVER, BTTS|YES"})
        self.assertEqual(cfg.ev_min, 0.07)
        self.assertEqual(cfg.markets, ("OU|2.5|OVER", "BTTS|YES"))
        self.assertEqual(cfg.p_min, 0.55)


class TestStats(unittest.TestCase):
    def _dec(self, seq, outcome, odds, clv_fair=None, odds_close=None, clv_odds=None, day=0):
        t = NOW + timedelta(days=day)
        return {"seq": seq, "status": "SETTLED" if outcome != "VOID" else "VOID", "outcome": outcome,
                "odds_locked": odds, "committed_utc": t - timedelta(hours=3), "settled_utc": t,
                "kickoff_utc": t - timedelta(hours=2), "odds_close": odds_close, "clv_odds_pct": clv_odds,
                "clv_fair_pct": clv_fair, "p_model": 0.6, "brier_model": 0.16, "brier_market": None}

    def test_flat_math_and_drawdown(self):
        ds = [self._dec(1, "WIN", 2.0, day=0), self._dec(2, "LOSS", 1.8, day=1),
              self._dec(3, "LOSS", 1.9, day=2), self._dec(4, "VOID", 2.2, day=3)]
        f = S.compute_flat(ds, 1000, NOW)
        self.assertEqual(f["n_settled"], 3)
        self.assertEqual(f["n_void"], 1)
        self.assertEqual(f["total_staked"], 3000)
        self.assertEqual(f["net"], 1000 - 1000 - 1000)
        self.assertAlmostEqual(f["roi"], -1000 / 3000, places=6)
        self.assertEqual(f["max_drawdown_units"], 2000.0)   # peak 1000 → -1000
        self.assertTrue(f["preliminary"])

    def test_T6_missing_closing_gives_null_not_zero(self):
        ds = [self._dec(1, "WIN", 2.0, clv_fair=None, odds_close=None),
              self._dec(2, "LOSS", 2.0, clv_fair=3.0, odds_close=1.94, clv_odds=3.09, day=1)]
        f = S.compute_flat(ds, 1000, NOW)
        self.assertEqual(f["clv"]["coverage"], 0.5)
        self.assertEqual(f["clv"]["n"], 1)
        self.assertEqual(f["positive_clv_share"], 1.0)
        self.assertIsNone(S.compute_flat([self._dec(1, "WIN", 2.0)], 1000, NOW)["positive_clv_share"])

    def test_clv_and_devig(self):
        self.assertAlmostEqual(S.clv_odds_pct(2.00, 1.90), 5.263157, places=5)
        fair = S.devig_multiplicative({"OVER": 1.90, "UNDER": 1.95})
        self.assertAlmostEqual(sum(fair.values()), 1.0, places=9)
        self.assertAlmostEqual(S.clv_fair_pct(2.00, fair["OVER"]), (2.0 * fair["OVER"] - 1) * 100, places=9)

    def test_serializable(self):
        f = S.compute_flat([self._dec(1, "WIN", 2.0)], 1000, NOW)
        json.dumps(f, default=str)


if __name__ == "__main__":
    unittest.main()
