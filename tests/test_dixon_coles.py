"""Regression tests for dixon_coles_engine."""
import random
import pandas as pd
import pytest

from penaltyblog.models import DixonColesGoalModel, dixon_coles_weights


def _synthetic_df(n: int = 400) -> pd.DataFrame:
    random.seed(0)
    teams = [f"T{i}" for i in range(20)]
    rows = []
    base = pd.Timestamp("2024-01-01")
    for i in range(n):
        h, a = random.sample(teams, 2)
        rows.append(
            {
                "HomeTeam": h,
                "AwayTeam": a,
                "FTHG": random.randint(0, 4),
                "FTAG": random.randint(0, 4),
                "Date": base + pd.Timedelta(days=i // 5),
            }
        )
    return pd.DataFrame(rows)


def test_scalar_weight_reproduces_bug():
    """Passing a scalar weight must raise — the exact bug we fixed."""
    df = _synthetic_df()
    with pytest.raises(TypeError, match="len"):
        DixonColesGoalModel(
            df["FTHG"].tolist(),
            df["FTAG"].tolist(),
            df["HomeTeam"].tolist(),
            df["AwayTeam"].tolist(),
            weights=0.0018,
        )


def test_dixon_coles_weights_array_fits():
    """The fix: pass per-match weights from dixon_coles_weights(dates)."""
    df = _synthetic_df()
    w = dixon_coles_weights(df["Date"].tolist(), xi=0.0018)
    assert w.shape == (len(df),)
    model = DixonColesGoalModel(
        df["FTHG"].tolist(),
        df["FTAG"].tolist(),
        df["HomeTeam"].tolist(),
        df["AwayTeam"].tolist(),
        weights=w,
    )
    model.fit()
    assert model.fitted is True

    grid = model.predict("T0", "T1")
    assert 0.0 <= grid.home_win <= 1.0
    assert 0.0 <= grid.btts_yes <= 1.0
