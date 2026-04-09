"""
API-Football fixture fetcher for SesomNod.

Fetches all football fixtures for a given date from API-Football (via RapidAPI),
normalises them with competition tier labels (UCL_UEL / TOP5 / OTHER), and caches
in the api_football_cache table so we stay within the 100 free requests/day budget.

Env var:  FOOTBALL_API_KEY  (X-RapidAPI-Key from RapidAPI dashboard)
Table:    api_football_cache (created by ensure_tables in main.py)

NEVER import Pinnacle/odds/picks logic here — this file is fixture-list ONLY.
"""
import json
import logging
import os
from datetime import datetime, timezone

import asyncpg
import httpx

logger = logging.getLogger("sesomnod.api_football")

# ────────────────────────────────────────────────────────────────────
# Tier mappings — which API-Football league IDs belong to which tier.
# These are API-Football (api-sports.io) internal IDs.
# ────────────────────────────────────────────────────────────────────

# Highest priority — European club competitions
UCL_UEL_IDS = frozenset({
    2,    # UEFA Champions League
    3,    # UEFA Europa League
    848,  # UEFA Conference League
    531,  # UEFA Super Cup
})

# Top-tier domestic leagues (matches SCAN_LEAGUES overlap in main.py)
TOP5_IDS = frozenset({
    39,   # English Premier League
    40,   # English Championship
    61,   # French Ligue 1
    78,   # German Bundesliga
    79,   # German 2. Bundesliga
    135,  # Italian Serie A
    136,  # Italian Serie B
    140,  # Spanish La Liga
    141,  # Spanish La Liga 2
    88,   # Eredivisie (Nederland)
    94,   # Primeira Liga (Portugal)
    144,  # Belgian Pro League
    113,  # Allsvenskan (Sverige)
    103,  # Eliteserien (Norge) — viktig for norsk marked
    119,  # Superliga (Danmark)
})


def _classify_tier(league_id: int) -> str:
    if league_id in UCL_UEL_IDS:
        return "UCL_UEL"
    if league_id in TOP5_IDS:
        return "TOP5"
    return "OTHER"


_TIER_ORDER = {"UCL_UEL": 0, "TOP5": 1, "OTHER": 2}


async def fetch_todays_fixtures_api_football(
    date_str: str,
    db_pool: asyncpg.Pool,
    force_refresh: bool = False,
) -> dict:
    """
    Henter alle fotballkamper for en gitt dato fra API-Football.

    Strategi:
      1. Sjekk cache (api_football_cache) for date_str
      2. Hvis cache finnes og force_refresh=False: returner cached data
      3. Hvis cache mangler eller force_refresh=True:
         - Kall API-Football /fixtures?date=date_str&timezone=Europe/Oslo
         - Transformer til normalisert format
         - Lagre til cache
         - Returner data
      4. Ved API-feil: returner cached data hvis tilgjengelig,
         ellers returner tom liste med error-flagg

    Args:
        date_str: "YYYY-MM-DD" format
        db_pool: asyncpg connection pool
        force_refresh: True overstyrer cache

    Returns:
        {
          "date": "YYYY-MM-DD",
          "source": "api_football" | "cache" | "error_fallback",
          "fetched_at": "ISO8601",
          "total": int,
          "by_tier": {"UCL_UEL": int, "TOP5": int, "OTHER": int},
          "fixtures": [NormalizedFixture, ...]
        }
    """

    # ── CACHE-SJEKK ──────────────────────────────────────────────────
    # asyncpg requires datetime.date for DATE columns, not a string
    from datetime import date as _date
    cache_date = _date.fromisoformat(date_str)

    async with db_pool.acquire() as conn:
        cached = await conn.fetchrow(
            "SELECT fixtures_json, fetched_at FROM api_football_cache "
            "WHERE cache_date = $1",
            cache_date,
        )

    if cached and not force_refresh:
        data = json.loads(cached["fixtures_json"])
        data["source"] = "cache"
        data["cached_at"] = cached["fetched_at"].isoformat()
        return data

    # ── API-KALL ─────────────────────────────────────────────────────
    api_key = os.environ.get("FOOTBALL_API_KEY", "")
    if not api_key:
        logger.warning("[API-Football] FOOTBALL_API_KEY ikke satt")
        if cached:
            data = json.loads(cached["fixtures_json"])
            data["source"] = "error_fallback"
            data["error"] = "FOOTBALL_API_KEY_MISSING"
            return data
        return {
            "date": date_str,
            "source": "error_fallback",
            "error": "FOOTBALL_API_KEY_MISSING",
            "total": 0,
            "by_tier": {"UCL_UEL": 0, "TOP5": 0, "OTHER": 0},
            "fixtures": [],
        }

    url = "https://v3.football.api-sports.io/fixtures"
    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "v3.football.api-sports.io",
    }
    params = {"date": date_str, "timezone": "Europe/Oslo"}

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(url, headers=headers, params=params)
            r.raise_for_status()
            raw = r.json()
    except Exception as e:
        logger.warning(f"[API-Football] Kall feilet: {e}")
        if cached:
            data = json.loads(cached["fixtures_json"])
            data["source"] = "error_fallback"
            data["error"] = str(e)
            return data
        return {
            "date": date_str,
            "source": "error_fallback",
            "error": str(e),
            "total": 0,
            "by_tier": {"UCL_UEL": 0, "TOP5": 0, "OTHER": 0},
            "fixtures": [],
        }

    # ── NORMALISERING ────────────────────────────────────────────────
    normalized = []
    for item in raw.get("response", []):
        try:
            league_id = item["league"]["id"]
            normalized.append({
                "api_football_id":  item["fixture"]["id"],
                "home_team":        item["teams"]["home"]["name"],
                "away_team":        item["teams"]["away"]["name"],
                "kickoff":          item["fixture"]["date"],
                "league":           item["league"]["name"],
                "league_id":        league_id,
                "league_country":   item["league"]["country"],
                "competition_tier": _classify_tier(league_id),
                "status":           item["fixture"]["status"]["short"],
                "source":           "api-football",
            })
        except (KeyError, TypeError) as exc:
            logger.warning(f"[API-Football] Normaliseringsfeil: {exc}")
            continue

    # Sorter: UCL_UEL → TOP5 → OTHER
    normalized.sort(key=lambda x: _TIER_ORDER.get(x["competition_tier"], 3))

    by_tier = {
        "UCL_UEL": sum(1 for f in normalized if f["competition_tier"] == "UCL_UEL"),
        "TOP5":    sum(1 for f in normalized if f["competition_tier"] == "TOP5"),
        "OTHER":   sum(1 for f in normalized if f["competition_tier"] == "OTHER"),
    }

    result = {
        "date":       date_str,
        "source":     "api_football",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "total":      len(normalized),
        "by_tier":    by_tier,
        "fixtures":   normalized,
    }

    # ── LAGRE TIL CACHE ──────────────────────────────────────────────
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO api_football_cache
              (cache_date, fixtures_json, fetched_at, total_fixtures)
            VALUES ($1, $2, NOW(), $3)
            ON CONFLICT (cache_date) DO UPDATE SET
              fixtures_json  = EXCLUDED.fixtures_json,
              fetched_at     = EXCLUDED.fetched_at,
              total_fixtures = EXCLUDED.total_fixtures
            """,
            cache_date,
            json.dumps(result),
            len(normalized),
        )

    logger.info(
        f"[API-Football] {date_str}: {len(normalized)} fixtures hentet "
        f"(UCL_UEL={by_tier['UCL_UEL']}, TOP5={by_tier['TOP5']}, "
        f"OTHER={by_tier['OTHER']})"
    )
    return result
