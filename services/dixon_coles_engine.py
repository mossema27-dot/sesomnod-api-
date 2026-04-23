"""
Dixon-Coles probability engine for football match prediction.
Uses penaltyblog library for model fitting and prediction.
Model is cached for 24 hours and fitted on ~3000+ historical matches.
"""
import asyncio
import logging
import time
import traceback
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

logger = logging.getLogger("sesomnod.dixon_coles")

# ── Module-level model cache ─────────────────────────────────────────────────
_fitted_model = None
_model_fitted_at: float = 0.0
_available_teams: list[str] = []
MODEL_CACHE_HOURS = 24
MODEL_CACHE_SECONDS = MODEL_CACHE_HOURS * 3600


@dataclass
class DixonColesResult:
    """Result from Dixon-Coles model prediction."""
    home_win_prob: float
    draw_prob: float
    away_win_prob: float
    btts_prob: float
    model_edge_vs_market: float  # model_prob - pinnacle_no_vig_prob
    home_team_found_in_data: bool
    away_team_found_in_data: bool
    data_sample_size: int
    fallback_used: bool

    def to_dict(self) -> dict:
        """Convert to JSON-serializable dict with rounded values."""
        return {
            "home_win_prob": round(self.home_win_prob, 4),
            "draw_prob": round(self.draw_prob, 4),
            "away_win_prob": round(self.away_win_prob, 4),
            "btts_prob": round(self.btts_prob, 4),
            "model_edge_vs_market": round(self.model_edge_vs_market, 4),
            "home_team_found_in_data": self.home_team_found_in_data,
            "away_team_found_in_data": self.away_team_found_in_data,
            "data_sample_size": self.data_sample_size,
            "fallback_used": self.fallback_used,
        }


def _fallback_result(market_home_prob: float, data_size: int = 0) -> DixonColesResult:
    """Return a fallback result using market probabilities."""
    draw_est = 0.25
    away_est = max(0.05, 1.0 - market_home_prob - draw_est)
    home_est = max(0.05, 1.0 - away_est - draw_est)
    return DixonColesResult(
        home_win_prob=round(home_est, 4),
        draw_prob=round(draw_est, 4),
        away_win_prob=round(away_est, 4),
        btts_prob=0.50,
        model_edge_vs_market=0.0,
        home_team_found_in_data=False,
        away_team_found_in_data=False,
        data_sample_size=data_size,
        fallback_used=True,
    )


def _fit_model_sync() -> tuple:
    """
    Synchronous model fitting — runs in thread via asyncio.to_thread.
    Returns (model, available_teams, data_size).
    """
    from penaltyblog.models import DixonColesGoalModel, dixon_coles_weights
    from services.football_data_fetcher import get_historical_data

    df = get_historical_data()
    data_size = len(df)
    logger.info("Fitting Dixon-Coles model on %d matches...", data_size)

    # penaltyblog>=1.0 expects weights as a per-match array, not a scalar xi.
    # Passing a scalar raises "len() of unsized object" in BaseGoalsModel.__init__.
    try:
        weights = dixon_coles_weights(df["Date"].tolist(), xi=0.0018)
    except Exception as e:
        logger.warning("dixon_coles_weights failed (%s); fitting with uniform weights", e)
        weights = None

    model = DixonColesGoalModel(
        goals_home=df["FTHG"].tolist(),
        goals_away=df["FTAG"].tolist(),
        teams_home=df["HomeTeam"].tolist(),
        teams_away=df["AwayTeam"].tolist(),
        weights=weights,
    )
    model.fit()

    teams = sorted(set(df["HomeTeam"].tolist() + df["AwayTeam"].tolist()))
    logger.info("Dixon-Coles model fitted. %d teams in dataset.", len(teams))
    return model, teams, data_size


async def _ensure_model():
    """Ensure model is fitted and cached. Fits in background thread if needed."""
    global _fitted_model, _model_fitted_at, _available_teams

    if _fitted_model is not None and (time.time() - _model_fitted_at) < MODEL_CACHE_SECONDS:
        return

    logger.info("Dixon-Coles model cache expired or empty. Fitting new model...")
    try:
        model, teams, _ = await asyncio.to_thread(_fit_model_sync)
        _fitted_model = model
        _available_teams = teams
        _model_fitted_at = time.time()
        logger.info("Dixon-Coles model cached. %d teams available.", len(teams))
    except Exception as e:
        logger.error("Dixon-Coles model fitting FAILED: %s", e)
        _fitted_model = None
        _available_teams = []


async def get_dixon_coles_probs(
    home_team: str,
    away_team: str,
    market_home_prob: float,
) -> DixonColesResult:
    """
    Get Dixon-Coles model probabilities for a match.
    Falls back to market probabilities if teams not found or model fails.
    Never raises — always returns a result.

    Args:
        home_team: Home team name (from API)
        away_team: Away team name (from API)
        market_home_prob: Pinnacle no-vig home win probability (0-1)

    Returns:
        DixonColesResult with probabilities and metadata
    """
    try:
        from services.team_normalizer import find_best_team_match
        from services.football_data_fetcher import get_historical_data, FootballDataFetchError

        # Ensure model is fitted
        await _ensure_model()

        if _fitted_model is None:
            logger.warning("Dixon-Coles model is None after fitting attempt")
            return _fallback_result(market_home_prob)

        # Get data size for metadata
        try:
            data_size = len(get_historical_data())
        except FootballDataFetchError:
            data_size = 0

        # Normalize team names
        home_matched = find_best_team_match(home_team, _available_teams)
        away_matched = find_best_team_match(away_team, _available_teams)

        home_found = home_matched is not None
        away_found = away_matched is not None

        if not home_found and not away_found:
            logger.warning(
                "Neither team found in dataset: %s, %s. Using market fallback.",
                home_team, away_team,
            )
            return DixonColesResult(
                home_win_prob=round(market_home_prob, 4),
                draw_prob=0.25,
                away_win_prob=round(max(0.05, 1.0 - market_home_prob - 0.25), 4),
                btts_prob=0.50,
                model_edge_vs_market=0.0,
                home_team_found_in_data=False,
                away_team_found_in_data=False,
                data_sample_size=data_size,
                fallback_used=True,
            )

        if not home_found:
            logger.warning("Home team not found: %s. Attempting with away only.", home_team)
            return _fallback_result(market_home_prob, data_size)
        if not away_found:
            logger.warning("Away team not found: %s. Attempting with home only.", away_team)
            return _fallback_result(market_home_prob, data_size)

        # Predict using model (CPU-bound, run in thread)
        def _predict():
            return _fitted_model.predict(home_matched, away_matched)

        prob_grid = await asyncio.to_thread(_predict)

        home_win = float(prob_grid.home_win)
        draw = float(prob_grid.draw)
        away_win = float(prob_grid.away_win)

        btts_prob = float(prob_grid.btts_yes)
        btts_prob = max(0.0, min(1.0, btts_prob))

        # Edge vs market
        model_edge = home_win - market_home_prob

        result = DixonColesResult(
            home_win_prob=round(home_win, 4),
            draw_prob=round(draw, 4),
            away_win_prob=round(away_win, 4),
            btts_prob=round(btts_prob, 4),
            model_edge_vs_market=round(model_edge, 4),
            home_team_found_in_data=home_found,
            away_team_found_in_data=away_found,
            data_sample_size=data_size,
            fallback_used=False,
        )

        logger.info(
            "Dixon-Coles: %s vs %s → H=%.1f%% D=%.1f%% A=%.1f%% BTTS=%.1f%% edge=%.2f%%",
            home_matched, away_matched,
            home_win * 100, draw * 100, away_win * 100, btts_prob * 100,
            model_edge * 100,
        )
        return result

    except Exception:
        logger.error("Dixon-Coles prediction failed:\n%s", traceback.format_exc())
        return _fallback_result(market_home_prob)
