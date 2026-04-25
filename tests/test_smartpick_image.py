"""
Standalone smoke test for services/smartpick_image_generator.generate_smartpick_image.

Run from repo root:
    python3 tests/test_smartpick_image.py

Writes:
    /tmp/smartpick_atomic.png   — ATOMIC tier sample
    /tmp/smartpick_edge.png     — EDGE tier sample
    /tmp/smartpick_empty.png    — missing-fields sample (fallback path)
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.smartpick_image_generator import generate_smartpick_image


def _sample_payload(tier: str, atomic_score: int, market: str, odds: float, edge: float, ev: float) -> dict:
    return {
        "pick_id": 1001,
        "match": {
            "home_team": "Arsenal",
            "away_team": "Newcastle United",
            "league": "Premier League",
            "kickoff_oslo": "20:00",
        },
        "selection": {
            "market": market,
            "odds": odds,
        },
        "math": {
            "tier": tier,
            "atomic_score": atomic_score,
            "edge_pct": edge,
            "ev_pct": ev,
            "model_prob": 0.62,
            "market_prob": 0.54,
            "kelly_pct": 2.4,
        },
    }


def main() -> int:
    cases = [
        ("atomic", _sample_payload("ATOMIC", 8, "Over 2.5 mål", 1.85, 12.4, 11.2), "/tmp/smartpick_atomic.png"),
        ("edge", _sample_payload("EDGE", 6, "BTTS Ja", 2.10, 8.3, 7.5), "/tmp/smartpick_edge.png"),
        ("empty", {}, "/tmp/smartpick_empty.png"),
        ("long_teams", _sample_payload(
            "ATOMIC", 9,
            "Over 3.5 mål", 3.25, 14.9, 13.7,
        ) | {"match": {
            "home_team": "Borussia Mönchengladbach",
            "away_team": "VfL Wolfsburg-Braunschweig",
            "league": "Bundesliga · Matchday 32",
            "kickoff_oslo": "15:30",
        }}, "/tmp/smartpick_long.png"),
    ]

    failures = 0
    for label, payload, out_path in cases:
        try:
            png = generate_smartpick_image(payload)
            assert png.startswith(b"\x89PNG\r\n\x1a\n"), "not a PNG"
            with open(out_path, "wb") as f:
                f.write(png)
            print(f"  [OK]  {label:10s} {len(png):>7d} bytes -> {out_path}")
        except Exception as e:
            failures += 1
            print(f"  [FAIL] {label:10s} {type(e).__name__}: {e}")

    if failures:
        print(f"FAIL: {failures} case(s)")
        return 1
    print("PASS: all cases rendered")
    return 0


if __name__ == "__main__":
    sys.exit(main())
