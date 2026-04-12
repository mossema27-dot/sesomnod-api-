"""
The Operator — Intelligent Telegram agent for SesomNod
=======================================================
- MAX 2 messages/day (hardcoded, non-negotiable)
- Always explains WHY a pick matters
- Tracks consecutive losses for context
- Receipt URL from pick_receipts table
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger("sesomnod")

MAX_DAILY = 2  # Non-negotiable. Hardcoded.


async def get_today_state(pool) -> dict:
    """Returns today's operator state row as dict. Creates if missing."""
    today = datetime.now(timezone.utc).date()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM operator_state WHERE state_date = $1", today
        )
        if row:
            return dict(row)

        # Create today's row, carrying forward consecutive_losses + total_wins
        prev = await conn.fetchrow("""
            SELECT consecutive_losses, total_wins_all_time
            FROM operator_state
            ORDER BY state_date DESC
            LIMIT 1
        """)
        cons_losses = int(prev["consecutive_losses"]) if prev else 0
        total_wins = int(prev["total_wins_all_time"]) if prev else 0

        row = await conn.fetchrow("""
            INSERT INTO operator_state
                (state_date, messages_sent_today, consecutive_losses, total_wins_all_time)
            VALUES ($1, 0, $2, $3)
            ON CONFLICT (state_date) DO NOTHING
            RETURNING *
        """, today, cons_losses, total_wins)

        if row:
            return dict(row)

        # Race condition: another process inserted first
        row = await conn.fetchrow(
            "SELECT * FROM operator_state WHERE state_date = $1", today
        )
        return dict(row) if row else {
            "state_date": today,
            "messages_sent_today": 0,
            "consecutive_losses": cons_losses,
            "total_wins_all_time": total_wins,
        }


async def can_send(pool) -> bool:
    """Returns True if The Operator can still send today (< MAX_DAILY)."""
    state = await get_today_state(pool)
    return int(state.get("messages_sent_today", 0)) < MAX_DAILY


async def mark_sent(pool, pick_id: int):
    """Increment messages_sent_today. Call ONLY after successful Telegram send."""
    today = datetime.now(timezone.utc).date()
    now = datetime.now(timezone.utc)
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE operator_state SET
                messages_sent_today = messages_sent_today + 1,
                last_pick_id = $1,
                last_message_at = $2,
                updated_at = $2
            WHERE state_date = $3
        """, pick_id, now, today)
    logger.info(f"[Operator] mark_sent: pick_id={pick_id}")


async def mark_result(pool, outcome: str, kickoff_date) -> None:
    """
    Track consecutive losses. Only updates for today/yesterday kickoffs.
    outcome: 'WIN', 'LOSS', 'VOID'
    kickoff_date: date or datetime of the match kickoff
    """
    if outcome not in ("WIN", "LOSS"):
        return  # VOID doesn't affect streaks

    today = datetime.now(timezone.utc).date()
    if hasattr(kickoff_date, 'date'):
        ko_date = kickoff_date.date()
    else:
        ko_date = kickoff_date

    # Only process recent results (today or yesterday)
    if (today - ko_date).days > 1:
        logger.info(f"[Operator] mark_result SKIP: kickoff {ko_date} too old")
        return

    state = await get_today_state(pool)

    async with pool.acquire() as conn:
        if outcome == "WIN":
            await conn.execute("""
                UPDATE operator_state SET
                    consecutive_losses = 0,
                    total_wins_all_time = total_wins_all_time + 1,
                    updated_at = NOW()
                WHERE state_date = $1
            """, today)
            logger.info("[Operator] mark_result: WIN — streak reset")
        elif outcome == "LOSS":
            await conn.execute("""
                UPDATE operator_state SET
                    consecutive_losses = consecutive_losses + 1,
                    updated_at = NOW()
                WHERE state_date = $1
            """, today)
            new_losses = int(state.get("consecutive_losses", 0)) + 1
            logger.info(f"[Operator] mark_result: LOSS — consecutive: {new_losses}")


def build_pick_message(
    pick_data: dict,
    total_scanned: int = 0,
    receipt_slug: Optional[str] = None,
) -> str:
    """
    Builds The Operator's pick message with WHY explanation.
    Uses actual variable names from pick_data dict.
    """
    home = pick_data.get("home_team", "?")
    away = pick_data.get("away_team", "?")
    league = pick_data.get("league", "Football")

    odds = float(pick_data.get("odds") or pick_data.get("our_odds") or 0)
    edge = float(pick_data.get("edge") or pick_data.get("soft_edge") or 0)
    ev = float(pick_data.get("ev") or pick_data.get("ev_percent") or 0)
    omega = int(pick_data.get("score") or pick_data.get("atomic_score") or pick_data.get("omega_score") or 0)
    kelly = float(pick_data.get("kelly_stake") or pick_data.get("kelly_fraction") or 0)
    scan = total_scanned or int(pick_data.get("total_scanned") or 0)
    rejected = max(0, scan - 1)  # All scanned minus this pick

    # xG values
    xg_h = float(pick_data.get("xg_divergence_home") or pick_data.get("signal_xg_home") or pick_data.get("xg_home") or 0)
    xg_a = float(pick_data.get("xg_divergence_away") or pick_data.get("signal_xg_away") or pick_data.get("xg_away") or 0)

    # Build WHY bullets
    why_lines = []

    if xg_h > 0 and xg_a > 0:
        if xg_h > xg_a:
            why_lines.append(f"xG favoriserer {home} ({xg_h:.1f}\u2013{xg_a:.1f})")
        elif xg_a > xg_h:
            why_lines.append(f"xG favoriserer {away} ({xg_a:.1f}\u2013{xg_h:.1f})")
        else:
            why_lines.append(f"xG jevnt ({xg_h:.1f}\u2013{xg_a:.1f})")

    if omega >= 7:
        why_lines.append(f"Omega {omega}/9 \u2014 sterk signal-alignment")
    elif omega >= 5:
        why_lines.append(f"Omega {omega}/9 \u2014 moderat signal-alignment")

    if edge >= 10:
        why_lines.append("Edge bekreftet av flere modeller")
    elif edge >= 6:
        why_lines.append("Edge over minimum-terskel")

    # Fallback if no why lines
    if not why_lines:
        why_lines.append(f"Edge +{edge:.1f}% over fair odds")
        why_lines.append(f"EV +{ev:.1f}% forventet avkastning")

    why_block = "\n".join(f"\u2022 {line}" for line in why_lines)

    # Kelly display
    if kelly > 0:
        kelly_line = f"\nKelly: `{kelly:.1f}%` av bankroll"
    else:
        kelly_line = ""

    # Scan context
    if scan > 0:
        scan_line = f"\n_{scan} kamper skannet. {rejected} avvist. Dette passerte._"
    else:
        scan_line = ""

    # Receipt URL
    if receipt_slug:
        receipt_line = f"\nhttps://sesomnod.netlify.app/proof/{receipt_slug}"
    else:
        receipt_line = ""

    msg = (
        f"\u26a1 *THE OPERATOR*\n\n"
        f"\U0001f3df {league}\n"
        f"*{home} vs {away}*\n"
        f"Odds: `{odds:.2f}` \u00b7 Edge: `+{edge:.1f}%` \u00b7 EV: `+{ev:.1f}%`\n\n"
        f"Hvorfor:\n"
        f"{why_block}"
        f"{kelly_line}"
        f"{scan_line}"
        f"{receipt_line}\n\n"
        f"_Analyse kun. Du bestemmer._"
    )
    return msg


def build_no_pick_message(
    total_scanned: int = 0,
    highest_edge: float = 0.0,
) -> str:
    """
    Builds The Operator's no-pick message.
    Sent when nothing qualifies.
    """
    scan = total_scanned or 200
    edge_str = f"{highest_edge:.1f}" if highest_edge > 0 else "under terskel"

    msg = (
        f"\u26a1 *THE OPERATOR*\n\n"
        f"{scan} kamper skannet. Ingen over 8% edge-terskel.\n\n"
        f"H\u00f8yeste edge i dag: {edge_str}%.\n\n"
        f"_St\u00e5 over. Systemet beskytter bankrollen._"
    )
    return msg
