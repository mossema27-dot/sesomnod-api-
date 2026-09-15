"""
Flat-statistikk, CLV og Brier (brev §3.1, §4.2, §4.3). Ren Python, ingen I/O.

Inndata er en liste av dict med feltene fra oraklion.brain_decisions.
Alle tall merkes preliminary når n_settled < 30 eller n_days < 10.
Ingen terskel utløser påstander. Bootstrap-intervall er M2 (ikke her).
"""
from __future__ import annotations

from datetime import datetime, timezone
from statistics import mean, median
from typing import Optional
from zoneinfo import ZoneInfo

OSLO = ZoneInfo("Europe/Oslo")
PRELIM_MIN_N = 30
PRELIM_MIN_DAYS = 10


def devig_multiplicative(odds: dict) -> dict:
    """p_fair_k = (1/o_k) / sum_j(1/o_j). odds: {utfall: desimalodds}."""
    inv = {k: 1.0 / float(v) for k, v in odds.items() if v and float(v) > 1.0}
    s = sum(inv.values())
    if s <= 0:
        return {}
    return {k: v / s for k, v in inv.items()}


def clv_odds_pct(odds_locked: float, odds_close: float) -> float:
    return (float(odds_locked) / float(odds_close) - 1.0) * 100.0


def clv_fair_pct(odds_locked: float, p_fair_close: float) -> float:
    return (float(odds_locked) * float(p_fair_close) - 1.0) * 100.0


def pnl(outcome: str, odds_locked: float, flat_stake: int) -> float:
    if outcome == "WIN":
        return flat_stake * (float(odds_locked) - 1.0)
    if outcome == "LOSS":
        return -float(flat_stake)
    return 0.0  # VOID


def _f(x) -> Optional[float]:
    return None if x is None else float(x)


def compute_flat(decisions: list[dict], flat_stake: int, now: Optional[datetime] = None) -> dict:
    """
    decisions: rader med status, outcome, odds_locked, committed_utc, settled_utc,
    seq, odds_close, p_fair_close, p_model, clv_fair_pct, clv_odds_pct.
    """
    now = now or datetime.now(timezone.utc)
    settled = [d for d in decisions if d.get("status") == "SETTLED" and d.get("outcome") in ("WIN", "LOSS")]
    voids = [d for d in decisions if d.get("status") == "VOID" or d.get("outcome") == "VOID"]
    opens = [d for d in decisions if d.get("status") == "OPEN"]
    suspended = [d for d in decisions if d.get("status") == "SUSPENDED"]

    settled.sort(key=lambda d: (d["settled_utc"], int(d["seq"])))
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    curve = []
    for d in settled:
        equity += pnl(d["outcome"], d["odds_locked"], flat_stake)
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
        curve.append({"seq": int(d["seq"]), "settled_utc": d["settled_utc"], "equity": round(equity, 2)})

    n_settled = len(settled)
    total_staked = flat_stake * n_settled
    net = equity
    roi = (net / total_staked) if total_staked else None
    wins = sum(1 for d in settled if d["outcome"] == "WIN")

    with_close = [d for d in settled if d.get("odds_close") is not None]
    with_fair = [d for d in settled if d.get("clv_fair_pct") is not None]
    clv_odds_vals = [float(d["clv_odds_pct"]) for d in with_close if d.get("clv_odds_pct") is not None]
    clv_fair_vals = [float(d["clv_fair_pct"]) for d in with_fair]

    brier_rows = [d for d in settled if d.get("brier_model") is not None and d.get("brier_market") is not None]
    brier_model = mean(float(d["brier_model"]) for d in brier_rows) if brier_rows else None
    brier_market = mean(float(d["brier_market"]) for d in brier_rows) if brier_rows else None

    committed = [d["committed_utc"] for d in decisions if d.get("committed_utc")]
    settled_ts = [d["settled_utc"] for d in settled]
    period_start = min(committed) if committed else None
    period_end = max(settled_ts) if settled_ts else None
    days = sorted({d["kickoff_utc"].astimezone(OSLO).date() for d in settled if d.get("kickoff_utc")})
    n_days = len(days)

    return {
        "n_settled": n_settled,
        "n_win": wins,
        "n_loss": n_settled - wins,
        "n_void": len(voids),
        "n_open": len(opens),
        "n_suspended": len(suspended),
        "flat_stake": flat_stake,
        "total_staked": total_staked,
        "net": round(net, 2),
        "roi": round(roi, 6) if roi is not None else None,
        "max_drawdown_units": round(max_dd, 2),
        "max_drawdown_pct_of_staked": round(max_dd / total_staked, 6) if total_staked else None,
        "hit_rate": round(wins / n_settled, 6) if n_settled else None,
        "clv": {
            "n": len(clv_fair_vals),
            "coverage": round(len(with_close) / n_settled, 6) if n_settled else None,
            "coverage_fair": round(len(with_fair) / n_settled, 6) if n_settled else None,
            "median_fair_pct": round(median(clv_fair_vals), 4) if clv_fair_vals else None,
            "mean_fair_pct": round(mean(clv_fair_vals), 4) if clv_fair_vals else None,
            "median_odds_pct": round(median(clv_odds_vals), 4) if clv_odds_vals else None,
            "mean_odds_pct": round(mean(clv_odds_vals), 4) if clv_odds_vals else None,
            "ci": None,  # blokk-bootstrap er M2
        },
        # Etikett i UI, ordrett: "andel med positiv CLV mot closing-referansen".
        "positive_clv_share": round(sum(1 for v in clv_fair_vals if v > 0) / len(clv_fair_vals), 6)
        if clv_fair_vals else None,
        "positive_clv_share_odds": round(sum(1 for v in clv_odds_vals if v > 0) / len(clv_odds_vals), 6)
        if clv_odds_vals else None,
        "brier": {
            "model": round(brier_model, 6) if brier_model is not None else None,
            "market": round(brier_market, 6) if brier_market is not None else None,
            "diff": round(brier_model - brier_market, 6) if brier_rows else None,
            "n": len(brier_rows),
            "coverage": round(len(brier_rows) / n_settled, 6) if n_settled else None,
            "kind": "binary_on_selection",
        },
        "period": {"start_utc": period_start, "end_utc": period_end, "n_days": n_days},
        "preliminary": (n_settled < PRELIM_MIN_N) or (n_days < PRELIM_MIN_DAYS),
        "equity_curve": curve,
    }


def brier_binary(p: float, y: int) -> float:
    return (float(p) - float(y)) ** 2
