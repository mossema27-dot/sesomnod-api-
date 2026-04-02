"""
XGBoost training pipeline. Builds features from football-data.co.uk CSVs.
"""
import logging
from typing import Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger("sesomnod.xgb_training")

FEATURE_COLS = [
    "home_attack_str",
    "home_defense_weakness",
    "away_attack_str",
    "away_defense_weakness",
    "home_form",
    "away_form",
    "h2h_home_winrate",
]

FEATURE_LABELS = {
    "home_attack_str":       "Hjemmelag angrep",
    "home_defense_weakness": "Hjemmelag forsvar",
    "away_attack_str":       "Bortelag angrep",
    "away_defense_weakness": "Bortelag forsvar",
    "home_form":             "Hjemmelag form",
    "away_form":             "Bortelag form",
    "h2h_home_winrate":      "Head-to-head",
}


def _rolling_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Add rolling 5-game stats per team from historical data."""
    df = df.sort_values("Date").reset_index(drop=True)

    # Outcome: 2=home win, 1=draw, 0=away win
    df["outcome"] = np.where(
        df["FTHG"] > df["FTAG"], 2,
        np.where(df["FTHG"] == df["FTAG"], 1, 0),
    )

    rows: list[dict] = []
    for idx, row in df.iterrows():
        ht = row["HomeTeam"]
        at = row["AwayTeam"]
        date = row["Date"]

        past_home = df[(df["HomeTeam"] == ht) & (df["Date"] < date)].tail(5)
        past_away = df[(df["AwayTeam"] == at) & (df["Date"] < date)].tail(5)

        home_att = float(past_home["FTHG"].mean()) if len(past_home) > 0 else 1.2
        home_def = float(past_home["FTAG"].mean()) if len(past_home) > 0 else 1.2
        away_att = float(past_away["FTAG"].mean()) if len(past_away) > 0 else 1.0
        away_def = float(past_away["FTHG"].mean()) if len(past_away) > 0 else 1.0

        if len(past_home) > 0:
            home_pts = past_home["outcome"].apply(
                lambda x: 3 if x == 2 else (1 if x == 1 else 0)
            ).sum()
            home_form = home_pts / (len(past_home) * 3)
        else:
            home_form = 0.5

        if len(past_away) > 0:
            away_pts = past_away["outcome"].apply(
                lambda x: 3 if x == 0 else (1 if x == 1 else 0)
            ).sum()
            away_form = away_pts / (len(past_away) * 3)
        else:
            away_form = 0.5

        h2h = df[
            (
                ((df["HomeTeam"] == ht) & (df["AwayTeam"] == at))
                | ((df["HomeTeam"] == at) & (df["AwayTeam"] == ht))
            )
            & (df["Date"] < date)
        ].tail(5)

        if len(h2h) > 0:
            home_wins = len(
                h2h[
                    ((h2h["HomeTeam"] == ht) & (h2h["FTHG"] > h2h["FTAG"]))
                    | ((h2h["AwayTeam"] == ht) & (h2h["FTAG"] > h2h["FTHG"]))
                ]
            )
            h2h_rate = home_wins / len(h2h)
        else:
            h2h_rate = 0.5

        rows.append({
            "home_attack_str":       round(float(home_att), 4),
            "home_defense_weakness": round(float(home_def), 4),
            "away_attack_str":       round(float(away_att), 4),
            "away_defense_weakness": round(float(away_def), 4),
            "home_form":             round(float(home_form), 4),
            "away_form":             round(float(away_form), 4),
            "h2h_home_winrate":      round(float(h2h_rate), 4),
            "outcome":               int(row["outcome"]),
        })

    return pd.DataFrame(rows)


def build_training_dataset(
    df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Build XGBoost training dataset from football-data.co.uk DataFrame.

    Args:
        df: DataFrame with columns HomeTeam, AwayTeam, FTHG, FTAG, Date

    Returns:
        (X, y) ready for XGBoost training
    """
    if len(df) < 100:
        raise ValueError(
            f"Insufficient data: {len(df)} rows. Need at least 100 matches."
        )

    features_df = _rolling_stats(df)
    features_df = features_df.dropna()

    if len(features_df) < 100:
        raise ValueError(
            f"After feature engineering: only {len(features_df)} rows."
        )

    X = features_df[FEATURE_COLS]
    y = features_df["outcome"]

    logger.info(
        "Training dataset: %d matches. Classes: %s",
        len(features_df),
        y.value_counts().to_dict(),
    )
    return X, y
