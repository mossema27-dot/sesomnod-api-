"""Shared alert primitives for tier2 + tier3 monitoring.

Design principles
-----------------
- Two consecutive bad checks before Telegram fires (false positive armor).
- Rate limit: max 3 Telegram alerts per category per hour.
- Every check is appended to a log file regardless of severity.
- Module-level state is in-process; resets on Railway restart (acceptable).
"""

from __future__ import annotations

import logging
import os
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# Persisted relative to this file. Railway writable, .gitignored.
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TIER2_LOG = os.path.join(_BASE_DIR, "alerts.log")
TIER3_LOG = os.path.join(_BASE_DIR, "strategic_alerts.log")

# In-process state (not persisted across deploys).
_consecutive_bad: dict[str, int] = defaultdict(int)
_alert_timestamps: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=3))

# Hard limits to prevent Telegram spam.
RATE_LIMIT_WINDOW_SEC = 3600  # 1 hour
RATE_LIMIT_MAX_PER_WINDOW = 3
CONSECUTIVE_BAD_THRESHOLD = 2  # only alert on 2nd consecutive failure


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _append_log(log_path: str, severity: str, category: str, message: str) -> None:
    """Append a single line to the rotating-style log."""
    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"{_now_iso()} [{severity.upper()}] {category} :: {message}\n")
    except OSError as e:
        logger.warning(f"[alerts] log write failed: {e}")


def _telegram_send(message: str) -> bool:
    """Best-effort Telegram delivery. Never raises. Returns True on 200 OK."""
    token = os.environ.get("TELEGRAM_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        logger.info("[alerts] Telegram skipped: token/chat_id missing")
        return False
    try:
        with httpx.Client(timeout=10) as client:
            r = client.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": message},
            )
        if r.status_code == 200:
            return True
        logger.warning(f"[alerts] Telegram non-200: {r.status_code} {r.text[:200]}")
        return False
    except Exception as e:
        logger.warning(f"[alerts] Telegram delivery error: {e}")
        return False


def record_good(category: str) -> None:
    """Mark a check as healthy. Resets consecutive-bad counter."""
    if _consecutive_bad[category] > 0:
        logger.info(f"[alerts] {category} recovered (was {_consecutive_bad[category]})")
    _consecutive_bad[category] = 0


def record_bad(
    category: str,
    *,
    log_path: str,
    severity: str = "warning",
    message: str,
    telegram_prefix: Optional[str] = None,
) -> bool:
    """Mark a check as failing. Returns True if a Telegram alert was sent.

    - First failure: log only, increment counter.
    - Second consecutive failure: log + Telegram (if rate-limit allows).
    - Subsequent failures: log only.
    """
    _consecutive_bad[category] += 1
    count = _consecutive_bad[category]
    _append_log(log_path, severity, category, f"(consec={count}) {message}")

    if count < CONSECUTIVE_BAD_THRESHOLD:
        return False
    if count > CONSECUTIVE_BAD_THRESHOLD:
        # Already alerted on 2nd; don't spam on 3rd, 4th, etc.
        return False

    # Rate-limit gate: no more than RATE_LIMIT_MAX_PER_WINDOW in window.
    now = time.time()
    history = _alert_timestamps[category]
    while history and (now - history[0]) > RATE_LIMIT_WINDOW_SEC:
        history.popleft()
    if len(history) >= RATE_LIMIT_MAX_PER_WINDOW:
        logger.info(f"[alerts] {category} rate-limited ({len(history)} in window)")
        return False

    prefix = telegram_prefix or f"⚠ SesomNod {severity.upper()}"
    body = f"{prefix}\n[{category}]\n{message}"
    sent = _telegram_send(body)
    if sent:
        history.append(now)
    return sent


def state_snapshot() -> dict:
    """Inspection helper for diagnostics."""
    return {
        "consecutive_bad": dict(_consecutive_bad),
        "recent_alerts": {k: len(v) for k, v in _alert_timestamps.items()},
    }
