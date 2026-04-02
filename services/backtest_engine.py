"""
Backtest engine for SesomNod.

Strategy: Rolling Poisson model per league vs Bet365 odds.
For each match, fit model on previous ROLLING_WINDOW matches
from the SAME league only. No lookahead bias.
Bet when model_prob x B365_odds - 1 >= EDGE_THRESHOLD.
"""

import math
import logging
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger("sesomnod.backtest")

# ── Hyperparameters ──────────────────────────────────────────
EDGE_THRESHOLD = 0.20    # 20% minimum model edge vs Bet365
MIN_CONFIDENCE = 0.30    # minimum model probability to consider
HALF_KELLY_CAP = 0.10    # max stake fraction per pick
ROLLING_WINDOW = 38      # matches of history per league
MAX_GOALS      = 6       # grid size for Poisson probability


# ── Data classes ─────────────────────────────────────────────

@dataclass
class BacktestPickResult:
    """Single pick result from backtest."""
    match_date: str
    home_team: str
    away_team: str
    league: str
    predicted_outcome: str   # HOME_WIN / DRAW / AWAY_WIN
    actual_outcome: str
    model_prob: float
    bet365_odds: float
    edge_pct: float
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


# ── Internal helpers ─────────────────────────────────────────

def _poisson_pmf(lam: float, k: int) -> float:
    """P(X = k) for Poisson(lambda). Safe against overflow."""
    try:
        if lam <= 0:
            return 1.0 if k == 0 else 0.0
        return (lam ** k) * math.exp(-lam) / math.factorial(k)
    except (ValueError, OverflowError):
        return 0.0


def _fit_model(history: pd.DataFrame) -> dict:
    """
    Fit attack/defense strength model on historical matches.
    Returns dict with home_avg, away_avg, attack, defense.
    Empty dict if insufficient data.
    """
    if len(history) < 10:
        return {}

    home_avg = float(history["FTHG"].mean())
    away_avg = float(history["FTAG"].mean())

    if home_avg <= 0 or away_avg <= 0:
        return {}

    league_avg = (home_avg + away_avg) / 2.0
    teams = set(
        history["HomeTeam"].tolist() + history["AwayTeam"].tolist()
    )

    attack: dict[str, float] = {}
    defense: dict[str, float] = {}

    for team in teams:
        h = history[history["HomeTeam"] == team]
        a = history[history["AwayTeam"] == team]

        scored: list[float] = []
        conceded: list[float] = []

        if len(h) > 0:
            scored.extend(h["FTHG"].tolist())
            conceded.extend(h["FTAG"].tolist())
        if len(a) > 0:
            scored.extend(a["FTAG"].tolist())
            conceded.extend(a["FTHG"].tolist())

        if len(scored) >= 3:
            attack[team] = float(np.mean(scored)) / league_avg
            defense[team] = float(np.mean(conceded)) / league_avg

    return {
        "home_avg": home_avg,
        "away_avg": away_avg,
        "attack": attack,
        "defense": defense,
    }


def _match_probs(
    model: dict,
    home_team: str,
    away_team: str,
) -> tuple:
    """
    Compute (P_home_win, P_draw, P_away_win) via Poisson grid.
    Falls back to (0.45, 0.27, 0.28) if model is empty.
    """
    if not model:
        return (0.45, 0.27, 0.28)

    ha = model["home_avg"]
    aa = model["away_avg"]
    atk = model["attack"]
    dfn = model["defense"]

    home_xg = ha * atk.get(home_team, 1.0) * dfn.get(away_team, 1.0)
    away_xg = aa * atk.get(away_team, 1.0) * dfn.get(home_team, 1.0)

    home_xg = float(np.clip(home_xg, 0.3, 5.0))
    away_xg = float(np.clip(away_xg, 0.2, 5.0))

    hw = dw = aw = 0.0
    for i in range(MAX_GOALS + 1):
        p_i = _poisson_pmf(home_xg, i)
        for j in range(MAX_GOALS + 1):
            p = p_i * _poisson_pmf(away_xg, j)
            if i > j:
                hw += p
            elif i == j:
                dw += p
            else:
                aw += p

    total = hw + dw + aw
    if total <= 0:
        return (0.45, 0.27, 0.28)

    return (hw / total, dw / total, aw / total)


def _get_b365(row: pd.Series, outcome: str) -> Optional[float]:
    """Get Bet365 decimal odds for outcome H/D/A."""
    col_map = {"H": "B365H", "D": "B365D", "A": "B365A"}
    col = col_map.get(outcome)
    if not col or col not in row.index:
        return None
    try:
        v = float(row[col])
        return v if (v > 1.01 and not math.isnan(v)) else None
    except (ValueError, TypeError):
        return None


# ── Public API ───────────────────────────────────────────────

def run_backtest(
    df: pd.DataFrame,
    league_name: str = "All Leagues",
) -> BacktestSummary:
    """
    Backtest rolling Poisson model vs Bet365 odds.

    For each match:
      1. Take previous ROLLING_WINDOW matches from SAME league.
      2. Fit attack/defense Poisson model on that history.
      3. Compute P(home), P(draw), P(away) via Poisson grid.
      4. Compare each probability against Bet365 odds.
      5. Bet the outcome with highest edge if >= EDGE_THRESHOLD.
      6. Size bet with half-Kelly capped at HALF_KELLY_CAP.
    """
    required = ["HomeTeam", "AwayTeam", "FTHG", "FTAG", "Date"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        logger.error("Backtest: missing required columns: %s", missing)
        return BacktestSummary(
            total_matches_scanned=0, qualified_picks=0,
            hit_rate=0.0, roi_pct=0.0, avg_clv=0.0,
            avg_brier=0.0, max_drawdown_pct=0.0,
            total_profit_units=0.0,
        )

    has_b365 = all(c in df.columns for c in ["B365H", "B365D", "B365A"])
    if not has_b365:
        logger.warning("Backtest: no Bet365 odds columns in data")
        return BacktestSummary(
            total_matches_scanned=len(df), qualified_picks=0,
            hit_rate=0.0, roi_pct=0.0, avg_clv=0.0,
            avg_brier=0.0, max_drawdown_pct=0.0,
            total_profit_units=0.0,
        )

    has_league_col = "League" in df.columns
    df = df.sort_values("Date").reset_index(drop=True)

    outcome_labels = {"H": "HOME_WIN", "D": "DRAW", "A": "AWAY_WIN"}
    picks: list[BacktestPickResult] = []
    cumulative = 0.0
    peak = 0.0
    max_dd = 0.0
    total_brier = 0.0

    for idx, row in df.iterrows():
        try:
            fthg = int(row["FTHG"])
            ftag = int(row["FTAG"])
        except (ValueError, TypeError):
            continue

        actual = "H" if fthg > ftag else ("D" if fthg == ftag else "A")

        # Build league-filtered history (no lookahead)
        if has_league_col:
            row_league = str(row.get("League", ""))
            if row_league:
                league_mask = df["League"] == row_league
                league_df = df[league_mask].reset_index(drop=True)
                match_mask = (
                    (league_df["HomeTeam"] == row.get("HomeTeam"))
                    & (league_df["AwayTeam"] == row.get("AwayTeam"))
                    & (league_df["Date"] == row.get("Date"))
                )
                match_positions = league_df.index[match_mask].tolist()
                if match_positions:
                    cidx = match_positions[0]
                    hist_start = max(0, cidx - ROLLING_WINDOW)
                    history = league_df.iloc[hist_start:cidx]
                else:
                    hist_start = max(0, int(idx) - ROLLING_WINDOW)
                    history = df.iloc[hist_start:int(idx)]
            else:
                hist_start = max(0, int(idx) - ROLLING_WINDOW)
                history = df.iloc[hist_start:int(idx)]
        else:
            hist_start = max(0, int(idx) - ROLLING_WINDOW)
            history = df.iloc[hist_start:int(idx)]

        # Fit model and get probabilities
        model = _fit_model(history)
        hw_p, d_p, aw_p = _match_probs(
            model,
            str(row.get("HomeTeam", "")),
            str(row.get("AwayTeam", "")),
        )

        # Find best edge vs Bet365
        best = None
        best_edge = 0.0

        for outcome_code, model_prob in [("H", hw_p), ("D", d_p), ("A", aw_p)]:
            if model_prob < MIN_CONFIDENCE:
                continue
            b365 = _get_b365(row, outcome_code)
            if b365 is None:
                continue
            edge = model_prob * b365 - 1
            if edge >= EDGE_THRESHOLD and edge > best_edge:
                best_edge = edge
                best = (outcome_code, model_prob, b365)

        if best is None:
            continue

        code, prob, odds = best
        was_correct = code == actual

        # Half-Kelly stake
        b = odds - 1.0
        raw_k = (prob * b - (1.0 - prob)) / b
        stake = float(np.clip(raw_k * 0.5, 0.0, HALF_KELLY_CAP))
        profit = stake * b if was_correct else -stake

        cumulative += profit
        if cumulative > peak:
            peak = cumulative
        dd = peak - cumulative
        if dd > max_dd:
            max_dd = dd

        brier = (prob - (1.0 if was_correct else 0.0)) ** 2
        total_brier += brier

        picks.append(BacktestPickResult(
            match_date=str(row.get("Date", ""))[:10],
            home_team=str(row.get("HomeTeam", "")),
            away_team=str(row.get("AwayTeam", "")),
            league=str(row.get("League", league_name)),
            predicted_outcome=outcome_labels[code],
            actual_outcome=outcome_labels[actual],
            model_prob=round(prob, 4),
            bet365_odds=round(odds, 3),
            edge_pct=round(best_edge * 100, 2),
            brier_contribution=round(brier, 4),
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
            total_profit_units=0.0,
        )

    correct = sum(1 for p in picks if p.was_correct)
    max_dd_pct = (max_dd / peak * 100.0) if peak > 0 else 0.0
    avg_clv = float(np.mean([p.edge_pct for p in picks]))

    logger.info(
        "Backtest complete: %d picks from %d matches | "
        "hit=%s | ROI=%.1f%% | CLV=%.1f%%",
        n, len(df), f"{correct/n:.1%}",
        cumulative / n * 100, avg_clv,
    )

    return BacktestSummary(
        total_matches_scanned=len(df),
        qualified_picks=n,
        hit_rate=round(correct / n, 4),
        roi_pct=round(cumulative / n * 100, 2),
        avg_clv=round(avg_clv, 2),
        avg_brier=round(total_brier / n, 4),
        max_drawdown_pct=round(max_dd_pct, 2),
        total_profit_units=round(cumulative, 4),
        picks=picks,
    )
