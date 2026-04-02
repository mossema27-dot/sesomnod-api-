"""
Backtest engine for no-vig value betting strategy.
Uses football-data.co.uk historical data with Pinnacle/Bet365 odds.
"""
import logging
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger("sesomnod.backtest")

EDGE_THRESHOLD = 0.02   # 2% minimum edge
MIN_CONFIDENCE = 0.35   # min true prob to consider
HALF_KELLY_CAP = 0.10   # max 10% stake per pick


@dataclass
class BacktestPickResult:
    """Single pick result from backtest."""
    match_date: str
    home_team: str
    away_team: str
    league: str
    predicted_outcome: str
    actual_outcome: str
    predicted_prob: float
    closing_odds: float
    clv: float
    brier_contribution: float
    profit_units: float
    cumulative_profit: float
    was_correct: bool


@dataclass
class BacktestSummary:
    """Aggregated backtest results."""
    total_matches_scanned: int
    qualified_picks: int
    hit_rate: float
    roi_pct: float
    avg_clv: float
    avg_brier: float
    max_drawdown_pct: float
    total_profit_units: float
    picks: list = field(default_factory=list)


def _get_odds(row: pd.Series, outcome: str) -> Optional[float]:
    """Get Pinnacle odds, fall back to Bet365. outcome: H/D/A."""
    pinnacle = {"H": "PSH", "D": "PSD", "A": "PSA"}
    bet365 = {"H": "B365H", "D": "B365D", "A": "B365A"}
    for col in [pinnacle.get(outcome), bet365.get(outcome)]:
        if col and col in row.index:
            try:
                v = float(row[col])
                if v > 1.01 and not np.isnan(v):
                    return v
            except (ValueError, TypeError):
                continue
    return None


def _no_vig(h: float, d: float, a: float) -> tuple:
    """Remove bookmaker margin. Returns true probs summing to 1."""
    tot = 1 / h + 1 / d + 1 / a
    return (1 / h) / tot, (1 / d) / tot, (1 / a) / tot


def run_backtest(
    df: pd.DataFrame,
    league_name: str = "All Leagues",
) -> BacktestSummary:
    """
    Backtest no-vig value betting strategy on historical data.

    Strategy: compute no-vig probs from Pinnacle odds.
    Bet when edge > 6% and prob > 45%.
    Stake: half-Kelly capped at 10%.

    Args:
        df: DataFrame from football_data_fetcher with odds columns
        league_name: Label for this backtest run

    Returns:
        BacktestSummary with full results
    """
    df = df.sort_values("Date").reset_index(drop=True)

    picks: list[BacktestPickResult] = []
    cumulative = 0.0
    peak = 0.0
    max_dd = 0.0
    total_brier = 0.0
    outcome_labels = {"H": "HOME_WIN", "D": "DRAW", "A": "AWAY_WIN"}

    for _, row in df.iterrows():
        try:
            fthg, ftag = int(row["FTHG"]), int(row["FTAG"])
        except (ValueError, TypeError):
            continue

        actual = "H" if fthg > ftag else ("D" if fthg == ftag else "A")

        # Pinnacle = sharp market → true probabilities
        # Bet365 = soft market → where value exists
        def _safe_odds(col_name: str) -> float:
            if col_name in row.index:
                try:
                    v = float(row[col_name])
                    if v > 1.01 and not np.isnan(v):
                        return v
                except (ValueError, TypeError):
                    pass
            return 0.0

        # Bet365 odds (soft market — where we bet)
        b365_h = _safe_odds("B365H")
        b365_d = _safe_odds("B365D")
        b365_a = _safe_odds("B365A")
        if not all([b365_h > 1, b365_d > 1, b365_a > 1]):
            continue

        # Pinnacle no-vig = true probabilities (sharp benchmark)
        pin_h = _safe_odds("PSH")
        pin_d = _safe_odds("PSD")
        pin_a = _safe_odds("PSA")

        if all([pin_h > 1, pin_d > 1, pin_a > 1]):
            overround = 1 / pin_h + 1 / pin_d + 1 / pin_a
            true_h = (1 / pin_h) / overround
            true_d = (1 / pin_d) / overround
            true_a = (1 / pin_a) / overround
        else:
            # Fall back to Bet365 no-vig if no Pinnacle
            overround = 1 / b365_h + 1 / b365_d + 1 / b365_a
            true_h = (1 / b365_h) / overround
            true_d = (1 / b365_d) / overround
            true_a = (1 / b365_a) / overround

        # Edge = true_prob × bet365_odds - 1
        best = None
        best_edge = 0.0
        for code, true_prob, b365_odds in [
            ("H", true_h, b365_h),
            ("D", true_d, b365_d),
            ("A", true_a, b365_a),
        ]:
            if b365_odds <= 1.01:
                continue
            if true_prob < MIN_CONFIDENCE:
                continue
            edge = true_prob * b365_odds - 1
            if edge > best_edge:
                best_edge = edge
                best = (code, true_prob, b365_odds)

        if best is None or best_edge < EDGE_THRESHOLD:
            continue

        code, prob, odds = best
        was_correct = code == actual

        b = odds - 1
        raw_k = (prob * b - (1 - prob)) / b
        stake = min(max(raw_k * 0.5, 0.0), HALF_KELLY_CAP)

        profit = stake * b if was_correct else -stake
        cumulative += profit

        if cumulative > peak:
            peak = cumulative
        dd = peak - cumulative
        if dd > max_dd:
            max_dd = dd

        clv = round((prob * odds - 1) * 100, 2)
        brier = round((prob - (1.0 if was_correct else 0.0)) ** 2, 4)
        total_brier += brier

        picks.append(BacktestPickResult(
            match_date=str(row.get("Date", ""))[:10],
            home_team=str(row.get("HomeTeam", "")),
            away_team=str(row.get("AwayTeam", "")),
            league=league_name,
            predicted_outcome=outcome_labels[code],
            actual_outcome=outcome_labels[actual],
            predicted_prob=round(prob, 4),
            closing_odds=round(odds, 3),
            clv=clv,
            brier_contribution=brier,
            profit_units=round(profit, 4),
            cumulative_profit=round(cumulative, 4),
            was_correct=was_correct,
        ))

    n = len(picks)
    if n == 0:
        return BacktestSummary(
            total_matches_scanned=len(df),
            qualified_picks=0,
            hit_rate=0.0, roi_pct=0.0, avg_clv=0.0,
            avg_brier=0.0, max_drawdown_pct=0.0,
            total_profit_units=0.0, picks=[],
        )

    correct = sum(1 for p in picks if p.was_correct)
    max_dd_pct = (max_dd / peak * 100) if peak > 0 else 0.0

    return BacktestSummary(
        total_matches_scanned=len(df),
        qualified_picks=n,
        hit_rate=round(correct / n, 4),
        roi_pct=round(cumulative / n * 100, 2),
        avg_clv=round(float(np.mean([p.clv for p in picks])), 2),
        avg_brier=round(total_brier / n, 4),
        max_drawdown_pct=round(max_dd_pct, 2),
        total_profit_units=round(cumulative, 4),
        picks=picks,
    )
