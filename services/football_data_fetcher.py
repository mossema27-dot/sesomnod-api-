"""
Fetch historical match data from football-data.co.uk for Dixon-Coles model.
Covers 5 top European leagues, 2 seasons each (10 CSVs).
Results are cached for 24 hours.
"""
import logging
import time
from io import StringIO

import pandas as pd
import requests

logger = logging.getLogger("sesomnod.football_data")

# ── CSV sources: current season + previous season per league ─────────────────
LEAGUE_CSVS = [
    # EPL
    "https://www.football-data.co.uk/mmz4281/2425/E0.csv",
    "https://www.football-data.co.uk/mmz4281/2324/E0.csv",
    # La Liga
    "https://www.football-data.co.uk/mmz4281/2425/SP1.csv",
    "https://www.football-data.co.uk/mmz4281/2324/SP1.csv",
    # Bundesliga
    "https://www.football-data.co.uk/mmz4281/2425/D1.csv",
    "https://www.football-data.co.uk/mmz4281/2324/D1.csv",
    # Serie A
    "https://www.football-data.co.uk/mmz4281/2425/I1.csv",
    "https://www.football-data.co.uk/mmz4281/2324/I1.csv",
    # Ligue 1
    "https://www.football-data.co.uk/mmz4281/2425/F1.csv",
    "https://www.football-data.co.uk/mmz4281/2324/F1.csv",
]

REQUIRED_COLUMNS = ["HomeTeam", "AwayTeam", "FTHG", "FTAG", "Date"]
ODDS_COLUMNS = ["PSH", "PSD", "PSA", "B365H", "B365D", "B365A"]
KEEP_COLUMNS = REQUIRED_COLUMNS + ODDS_COLUMNS
CACHE_TTL_SECONDS = 86400  # 24 hours

# ── Module-level cache ───────────────────────────────────────────────────────
_cached_data: pd.DataFrame | None = None
_cached_at: float = 0.0


class FootballDataFetchError(Exception):
    """Raised when all CSV fetches fail."""


def _fetch_single_csv(url: str) -> pd.DataFrame | None:
    """Fetch and parse a single CSV. Returns None on failure."""
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        df = pd.read_csv(StringIO(resp.text), encoding="utf-8", on_bad_lines="skip")
        # Must have the 5 required columns; odds columns are optional
        missing_required = [c for c in REQUIRED_COLUMNS if c not in df.columns]
        if missing_required:
            logger.warning("CSV %s missing required columns: %s", url, missing_required)
            return None
        available = [c for c in KEEP_COLUMNS if c in df.columns]
        df = df[available].copy()
        # Clean goal columns
        df["FTHG"] = pd.to_numeric(df["FTHG"], errors="coerce")
        df["FTAG"] = pd.to_numeric(df["FTAG"], errors="coerce")
        df.dropna(subset=["FTHG", "FTAG"], inplace=True)
        df["FTHG"] = df["FTHG"].astype(int)
        df["FTAG"] = df["FTAG"].astype(int)
        # Parse dates (handle DD/MM/YY and DD/MM/YYYY)
        df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
        df.dropna(subset=["Date"], inplace=True)
        logger.info("Fetched %d matches from %s", len(df), url.split("/")[-1])
        return df
    except requests.RequestException as e:
        logger.warning("Failed to fetch %s: %s", url, e)
        return None
    except (pd.errors.ParserError, ValueError, KeyError) as e:
        logger.warning("Failed to parse %s: %s", url, e)
        return None


def get_historical_data() -> pd.DataFrame:
    """
    Returns combined historical match data from football-data.co.uk.
    Columns: HomeTeam, AwayTeam, FTHG, FTAG, Date
    Cached for 24 hours.
    """
    global _cached_data, _cached_at

    if _cached_data is not None and (time.time() - _cached_at) < CACHE_TTL_SECONDS:
        logger.info("Using cached historical data (%d matches)", len(_cached_data))
        return _cached_data

    frames: list[pd.DataFrame] = []
    for url in LEAGUE_CSVS:
        df = _fetch_single_csv(url)
        if df is not None and len(df) > 0:
            frames.append(df)

    if not frames:
        raise FootballDataFetchError(
            "All 10 CSV fetches failed. Cannot build Dixon-Coles model. "
            "Check network connectivity and football-data.co.uk availability."
        )

    combined = pd.concat(frames, ignore_index=True)
    combined.sort_values("Date", inplace=True)
    combined.reset_index(drop=True, inplace=True)

    _cached_data = combined
    _cached_at = time.time()
    logger.info("Historical data loaded: %d matches from %d sources", len(combined), len(frames))
    return combined
