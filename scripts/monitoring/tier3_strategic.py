"""TIER 3 — strategic monitoring, runs every 60 minutes.

Two checks (kept narrow on purpose):
1. clv_rolling_avg — rolling avg over last 30 settled PRIMARY picks
                     with clv_close_pct. Bad = avg < +1%.
2. clv_consecutive_negative — 5+ consecutive negative CLV closes. Bad.

Edge-erosion vs Big5 historical benchmark is intentionally NOT included —
benchmark requires settled volume that does not yet exist. Will be added
once the first 30+ aggregate completes (target late May / early June).
"""

from __future__ import annotations

import logging
from typing import Any

from .alerts import TIER3_LOG, record_bad, record_good, _append_log

logger = logging.getLogger(__name__)

# Thresholds.
ROLLING_WINDOW = 30
ROLLING_MIN_AVG_PCT = 1.0  # below this → degradation
CONSECUTIVE_NEGATIVE_THRESHOLD = 5
MIN_SAMPLE_FOR_ROLLING = 10  # don't alert on tiny samples


async def _check_rolling_clv(db_pool) -> tuple[bool, str, dict]:
    """Bad if avg(clv_close_pct) over last 30 PRIMARY picks < +1%."""
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT clv_close_pct
                FROM sniper_bets_v1
                WHERE market_tier = 'PRIMARY'
                  AND clv_close_pct IS NOT NULL
                ORDER BY id DESC
                LIMIT $1
                """,
                ROLLING_WINDOW,
            )
        clvs = [float(r["clv_close_pct"]) for r in rows]
        n = len(clvs)
        meta = {"sample_size": n, "window": ROLLING_WINDOW}
        if n < MIN_SAMPLE_FOR_ROLLING:
            return True, f"insufficient sample ({n}<{MIN_SAMPLE_FOR_ROLLING}), skipping", meta
        avg = sum(clvs) / n
        meta["avg_clv_pct"] = round(avg, 3)
        if avg < ROLLING_MIN_AVG_PCT:
            return False, f"rolling avg CLV {avg:.2f}% over last {n} (threshold +{ROLLING_MIN_AVG_PCT}%)", meta
        return True, f"rolling avg CLV +{avg:.2f}% over last {n}", meta
    except Exception as e:
        return True, f"clv_rolling query error (skip): {e}", {"error": str(e)[:200]}


async def _check_consecutive_negative(db_pool) -> tuple[bool, str, dict]:
    """Bad if last N closes (newest first) are all negative."""
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT clv_close_pct
                FROM sniper_bets_v1
                WHERE market_tier = 'PRIMARY'
                  AND clv_close_pct IS NOT NULL
                ORDER BY id DESC
                LIMIT $1
                """,
                CONSECUTIVE_NEGATIVE_THRESHOLD,
            )
        if len(rows) < CONSECUTIVE_NEGATIVE_THRESHOLD:
            return True, f"only {len(rows)} closed (need {CONSECUTIVE_NEGATIVE_THRESHOLD})", {}
        all_negative = all(float(r["clv_close_pct"]) < 0 for r in rows)
        meta = {"latest_clvs": [round(float(r["clv_close_pct"]), 2) for r in rows]}
        if all_negative:
            return False, f"last {CONSECUTIVE_NEGATIVE_THRESHOLD} closes all negative: {meta['latest_clvs']}", meta
        return True, f"recent CLV mix: {meta['latest_clvs']}", meta
    except Exception as e:
        return True, f"consecutive_negative query error (skip): {e}", {"error": str(e)[:200]}


async def run_tier3_check(db_state, db_pool) -> dict[str, Any]:
    """Top-level entry called from main.py scheduler job. Never raises."""
    results: dict[str, Any] = {}

    if not db_state.connected or not db_pool:
        msg = "DB offline — tier3 skipped"
        _append_log(TIER3_LOG, "info", "tier3.skipped", msg)
        return {"skipped": msg}

    try:
        ok, msg, meta = await _check_rolling_clv(db_pool)
        results["clv_rolling"] = {"ok": ok, "message": msg, **meta}
        if ok:
            record_good("tier3.clv_rolling")
            _append_log(TIER3_LOG, "info", "tier3.clv_rolling", msg)
        else:
            record_bad(
                "tier3.clv_rolling",
                log_path=TIER3_LOG,
                severity="warning",
                message=msg,
                telegram_prefix="⚠ SesomNod CLV degradation",
            )
    except Exception as e:
        logger.error(f"[tier3] rolling_clv crashed: {e}")
        results["clv_rolling"] = {"ok": False, "error": str(e)[:200]}

    try:
        ok, msg, meta = await _check_consecutive_negative(db_pool)
        results["clv_consecutive_negative"] = {"ok": ok, "message": msg, **meta}
        if ok:
            record_good("tier3.clv_consec_neg")
            _append_log(TIER3_LOG, "info", "tier3.clv_consec_neg", msg)
        else:
            record_bad(
                "tier3.clv_consec_neg",
                log_path=TIER3_LOG,
                severity="warning",
                message=msg,
                telegram_prefix="⚠ SesomNod CLV streak alert",
            )
    except Exception as e:
        logger.error(f"[tier3] consecutive_negative crashed: {e}")
        results["clv_consecutive_negative"] = {"ok": False, "error": str(e)[:200]}

    return results
