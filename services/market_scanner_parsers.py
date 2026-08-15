"""
Pure parsers for market_scanner ingestion responses.

Extracted from market_scanner.py so they can be unit-tested without pulling
in httpx or async infrastructure. Each function takes primitives
(status_code + body_text) and returns a well-typed result.

Contract for every parser here:
  - Never raises. All error paths return the caller's expected "empty" value
    ([] for odds, None for model probs) and log at WARNING or ERROR.
  - Structured log messages include league / match identity + body length so
    Sentry / grep can attribute failures to a specific feed.
  - Silent failover: the ingestion pipeline receives a valid empty result,
    never a crash — matches the "if a feed drops or returns malformed JSON,
    trigger a silent failover, not crash" rule.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def parse_odds_response(status_code: int, body_text: str, league: str) -> list[dict]:
    """
    Parse a response from the Odds API v4 (`/v4/sports/{league}/odds/`).

    Returns [] on any error class: non-200 HTTP, empty body, malformed JSON,
    or JSON body that isn't a list. Callers must treat [] as "no data for
    this league this cycle" — never a hard failure.
    """
    if status_code == 429:
        logger.warning(
            "Odds API %s: rate limited (status=429, body_len=%d)",
            league, len(body_text),
        )
        return []
    if status_code != 200:
        logger.warning(
            "Odds API %s: HTTP %d (body_len=%d)",
            league, status_code, len(body_text),
        )
        return []
    if not body_text:
        logger.warning("Odds API %s: empty body on 200", league)
        return []

    try:
        data = json.loads(body_text)
    except json.JSONDecodeError as e:
        logger.error(
            "Odds API %s: malformed JSON at pos %d (body_len=%d, head=%r)",
            league, e.pos, len(body_text), body_text[:80],
        )
        return []

    if not isinstance(data, list):
        logger.error(
            "Odds API %s: expected list, got %s (body_len=%d)",
            league, type(data).__name__, len(body_text),
        )
        return []

    return data


def parse_model_response(
    status_code: int,
    body_text: str,
    home: str,
    away: str,
) -> Optional[dict]:
    """
    Parse a response from the internal `/score-match` endpoint.

    Returns None on any error class (non-200, empty, malformed, non-dict) or
    when the model explicitly reports `fallback_used=True`. Caller treats
    None as "no valid model prediction → skip this match."
    """
    if status_code != 200:
        logger.debug(
            "Model prob %s vs %s: HTTP %d", home, away, status_code,
        )
        return None
    if not body_text:
        logger.debug(
            "Model prob %s vs %s: empty body on 200", home, away,
        )
        return None

    try:
        data = json.loads(body_text)
    except json.JSONDecodeError as e:
        logger.error(
            "Model prob %s vs %s: malformed JSON at pos %d (head=%r)",
            home, away, e.pos, body_text[:80],
        )
        return None

    if not isinstance(data, dict):
        logger.error(
            "Model prob %s vs %s: expected dict, got %s",
            home, away, type(data).__name__,
        )
        return None

    if data.get("fallback_used"):
        logger.debug(
            "Model prob %s vs %s: fallback_used=True → skip", home, away,
        )
        return None

    return data
