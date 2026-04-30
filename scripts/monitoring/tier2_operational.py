"""TIER 2 — operational monitoring, runs every 15 minutes.

Three checks:
1. scheduler_lag — any job with last_success=False
2. db_pool       — db_state.connected
3. primary_picks — at least 1 PRIMARY pick with kickoff in the future

Pinnacle reachability is NOT checked directly. It's implicitly tested by
sniper_odds_t60 / sniper_odds_close jobs landing in scheduler_lag.

Each check is independent: a single bad check increments a counter.
Two consecutive bad checks trigger a Telegram alert (see alerts.py).
"""

from __future__ import annotations

import logging
from typing import Any

from .alerts import TIER2_LOG, record_bad, record_good, _append_log

logger = logging.getLogger(__name__)

# Jobs that are intentionally rare (weekly/monthly) and should not trigger
# scheduler_lag alerts based on last_success=None.
_RARE_JOBS = {
    "clv_rapport",  # weekly Monday 08:00 UTC — last_run=None outside Mondays
}


async def _check_scheduler(scheduler, run_history: dict) -> tuple[bool, str]:
    """Return (ok, message). Bad = any job with last_success=False or paused."""
    failed = []
    paused = []
    for job in scheduler.get_jobs():
        if job.next_run_time is None and job.id not in _RARE_JOBS:
            paused.append(job.id)
            continue
        history = run_history.get(job.id) or {}
        last_success = history.get("last_success")
        # last_success is None if never run yet (acceptable for new jobs).
        if last_success is False:
            failed.append(f"{job.id}({history.get('last_error', 'no error msg')[:80]})")
    parts = []
    if paused:
        parts.append(f"paused: {', '.join(paused)}")
    if failed:
        parts.append(f"failed: {', '.join(failed)}")
    if parts:
        return False, " | ".join(parts)
    return True, f"all {len(scheduler.get_jobs())} jobs healthy"


async def _check_db(db_state) -> tuple[bool, str]:
    if not db_state.connected:
        return False, f"DB disconnected (consecutive_failures={db_state.consecutive_failures})"
    if db_state.consecutive_failures > 0:
        return False, f"DB intermittent failures: {db_state.consecutive_failures}"
    return True, "pool healthy"


async def _check_primary_picks(db_pool) -> tuple[bool, str]:
    """At least one PRIMARY pick with kickoff in future. Bad = 0."""
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT COUNT(*) AS n
                FROM sniper_bets_v1
                WHERE market_tier = 'PRIMARY'
                  AND kickoff_time > NOW()
                  AND result = 'PENDING'
                """
            )
        n = int(row["n"] or 0) if row else 0
        if n == 0:
            return False, "0 future PRIMARY picks pending (expected ≥1 before settlement window)"
        return True, f"{n} future PRIMARY picks pending"
    except Exception as e:
        return False, f"sniper_bets_v1 query error: {e}"


async def run_tier2_check(scheduler, db_state, db_pool, run_history: dict | None = None) -> dict[str, Any]:
    """Top-level entry called from main.py scheduler job. Never raises."""
    results: dict[str, Any] = {}
    run_history = run_history or {}

    try:
        ok, msg = await _check_scheduler(scheduler, run_history)
        results["scheduler"] = {"ok": ok, "message": msg}
        if ok:
            record_good("tier2.scheduler")
            _append_log(TIER2_LOG, "info", "tier2.scheduler", msg)
        else:
            record_bad(
                "tier2.scheduler",
                log_path=TIER2_LOG,
                severity="warning",
                message=msg,
                telegram_prefix="⚠ SesomNod scheduler degraded",
            )
    except Exception as e:
        logger.error(f"[tier2] scheduler check crashed: {e}")
        results["scheduler"] = {"ok": False, "error": str(e)[:200]}

    try:
        ok, msg = await _check_db(db_state)
        results["db"] = {"ok": ok, "message": msg}
        if ok:
            record_good("tier2.db_pool")
            _append_log(TIER2_LOG, "info", "tier2.db_pool", msg)
        else:
            record_bad(
                "tier2.db_pool",
                log_path=TIER2_LOG,
                severity="critical",
                message=msg,
                telegram_prefix="🚨 SesomNod DB pool down",
            )
    except Exception as e:
        logger.error(f"[tier2] db check crashed: {e}")
        results["db"] = {"ok": False, "error": str(e)[:200]}

    try:
        if not db_state.connected or not db_pool:
            results["primary_picks"] = {"ok": False, "skipped": "DB offline"}
        else:
            ok, msg = await _check_primary_picks(db_pool)
            results["primary_picks"] = {"ok": ok, "message": msg}
            if ok:
                record_good("tier2.primary_picks")
                _append_log(TIER2_LOG, "info", "tier2.primary_picks", msg)
            else:
                record_bad(
                    "tier2.primary_picks",
                    log_path=TIER2_LOG,
                    severity="warning",
                    message=msg,
                    telegram_prefix="⚠ SesomNod pick pipeline anomaly",
                )
    except Exception as e:
        logger.error(f"[tier2] primary_picks check crashed: {e}")
        results["primary_picks"] = {"ok": False, "error": str(e)[:200]}

    return results
