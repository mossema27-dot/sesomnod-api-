"""
Extract XGBoost features from live picks using historical data.
"""
import logging

import pandas as pd

from services.football_data_fetcher import get_historical_data
from services.team_normalizer import find_best_team_match

logger = logging.getLogger("sesomnod.feature_extractor")


def extract_features_for_pick(
    home_team: str,
    away_team: str,
) -> dict:
    """
    Extract XGBoost features for a live pick using historical data.
    Returns dict with all 7 feature keys.
    Always returns valid features — uses safe defaults if data missing.
    """
    defaults = {
        "home_attack_str": 1.2,
        "home_defense_weakness": 1.2,
        "away_attack_str": 1.0,
        "away_defense_weakness": 1.0,
        "home_form": 0.5,
        "away_form": 0.5,
        "h2h_home_winrate": 0.45,
    }

    try:
        df = get_historical_data()
        if df is None or len(df) == 0:
            return defaults

        available_teams = list(set(
            df["HomeTeam"].tolist() + df["AwayTeam"].tolist()
        ))

        ht = find_best_team_match(home_team, available_teams)
        at = find_best_team_match(away_team, available_teams)

        if ht is None or at is None:
            logger.info(
                "Teams not found in historical data: %s, %s",
                home_team, away_team,
            )
            return defaults

        df_sorted = df.sort_values("Date")

        home_games = df_sorted[df_sorted["HomeTeam"] == ht].tail(10)
        away_games = df_sorted[df_sorted["AwayTeam"] == at].tail(10)

        if len(home_games) == 0 or len(away_games) == 0:
            return defaults

        home_att = float(home_games["FTHG"].mean())
        home_def = float(home_games["FTAG"].mean())
        away_att = float(away_games["FTAG"].mean())
        away_def = float(away_games["FTHG"].mean())

        def calc_form(games_df: pd.DataFrame, is_home: bool) -> float:
            last5 = games_df.tail(5)
            if len(last5) == 0:
                return 0.5
            pts = 0
            for _, g in last5.iterrows():
                if is_home:
                    if g["FTHG"] > g["FTAG"]:
                        pts += 3
                    elif g["FTHG"] == g["FTAG"]:
                        pts += 1
                else:
                    if g["FTAG"] > g["FTHG"]:
                        pts += 3
                    elif g["FTHG"] == g["FTAG"]:
                        pts += 1
            return pts / (len(last5) * 3)

        home_form = calc_form(home_games, is_home=True)
        away_form = calc_form(away_games, is_home=False)

        h2h = df_sorted[
            ((df_sorted["HomeTeam"] == ht) & (df_sorted["AwayTeam"] == at))
            | ((df_sorted["HomeTeam"] == at) & (df_sorted["AwayTeam"] == ht))
        ].tail(5)

        if len(h2h) > 0:
            hw = len(
                h2h[
                    ((h2h["HomeTeam"] == ht) & (h2h["FTHG"] > h2h["FTAG"]))
                    | ((h2h["AwayTeam"] == ht) & (h2h["FTAG"] > h2h["FTHG"]))
                ]
            )
            h2h_rate = hw / len(h2h)
        else:
            h2h_rate = 0.45

        return {
            "home_attack_str": round(home_att, 4),
            "home_defense_weakness": round(home_def, 4),
            "away_attack_str": round(away_att, 4),
            "away_defense_weakness": round(away_def, 4),
            "home_form": round(home_form, 4),
            "away_form": round(away_form, 4),
            "h2h_home_winrate": round(h2h_rate, 4),
        }

    except Exception as e:
        logger.warning(
            "Feature extraction failed for %s vs %s: %s",
            home_team, away_team, e,
        )
        return defaults
