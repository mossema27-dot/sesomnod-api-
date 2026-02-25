"""
SESOMNOD ENGINE — Play-Money Bankroll Tracker v1.0
Tracks virtual bankroll from 100kr → 30,000kr goal
Compound growth on wins, unchanged on loss
All data persisted in Supabase
"""

import os
from datetime import date, datetime, timezone
from typing import Optional

BANKROLL_GOAL = 30000.0
BANKROLL_START = 100.0

DISCLAIMER = "Dette er underholdning og analyse. Gamble aldri mer enn du har råd til å tape."


async def ensure_bankroll_tables(db_execute) -> None:
    """Create bankroll tables if they don't exist"""
    await db_execute("""
        CREATE TABLE IF NOT EXISTS bankroll (
            id SERIAL PRIMARY KEY,
            dato DATE NOT NULL DEFAULT CURRENT_DATE,
            amount NUMERIC(12,2) NOT NULL,
            change_amount NUMERIC(12,2) DEFAULT 0,
            change_reason TEXT,
            dagens_kamp_id INTEGER REFERENCES dagens_kamp(id),
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    # Insert starting bankroll if empty
    await db_execute(f"""
        INSERT INTO bankroll (dato, amount, change_amount, change_reason)
        SELECT CURRENT_DATE, {BANKROLL_START}, 0, 'Start bankroll (lekepenger)'
        WHERE NOT EXISTS (SELECT 1 FROM bankroll LIMIT 1)
    """)


async def get_current_bankroll(db_query) -> float:
    """Get the latest bankroll amount"""
    rows = await db_query("""
        SELECT amount FROM bankroll ORDER BY created_at DESC LIMIT 1
    """)
    if rows:
        return float(rows[0].get("amount", BANKROLL_START))
    return BANKROLL_START


async def get_bankroll_history(db_query, days: int = 90) -> list:
    """Get bankroll history for equity curve"""
    rows = await db_query(f"""
        SELECT dato, amount, change_amount, change_reason, created_at
        FROM bankroll
        ORDER BY created_at ASC
        LIMIT 200
    """)
    return rows


async def apply_win(db_execute, db_query, odds: float, dagens_kamp_id: Optional[int] = None) -> dict:
    """
    Apply a WIN to the bankroll: bankroll × odds
    Returns: {"before": float, "after": float, "profit": float}
    """
    current = await get_current_bankroll(db_query)
    new_amount = round(current * odds, 2)
    profit = round(new_amount - current, 2)

    dk_ref = f", dagens_kamp_id = {dagens_kamp_id}" if dagens_kamp_id else ""

    await db_execute(f"""
        INSERT INTO bankroll (dato, amount, change_amount, change_reason{', dagens_kamp_id' if dagens_kamp_id else ''})
        VALUES (
            CURRENT_DATE,
            {new_amount},
            {profit},
            'WIN @ {odds:.2f}x — bankroll × odds'
            {', ' + str(dagens_kamp_id) if dagens_kamp_id else ''}
        )
    """)

    return {
        "before": current,
        "after": new_amount,
        "profit": profit,
        "goal": BANKROLL_GOAL,
        "progress_pct": min(100, round(new_amount / BANKROLL_GOAL * 100, 2)),
    }


async def apply_loss(db_execute, db_query, dagens_kamp_id: Optional[int] = None) -> dict:
    """
    Apply a LOSS to the bankroll: unchanged (lekepenger mode)
    Returns: {"before": float, "after": float}
    """
    current = await get_current_bankroll(db_query)

    dk_ref = f", dagens_kamp_id = {dagens_kamp_id}" if dagens_kamp_id else ""

    await db_execute(f"""
        INSERT INTO bankroll (dato, amount, change_amount, change_reason{', dagens_kamp_id' if dagens_kamp_id else ''})
        VALUES (
            CURRENT_DATE,
            {current},
            0,
            'LOSS — bankroll uendret (lekepenger)'
            {', ' + str(dagens_kamp_id) if dagens_kamp_id else ''}
        )
    """)

    return {
        "before": current,
        "after": current,
        "profit": 0,
        "goal": BANKROLL_GOAL,
        "progress_pct": min(100, round(current / BANKROLL_GOAL * 100, 2)),
    }


async def apply_push(db_execute, db_query, dagens_kamp_id: Optional[int] = None) -> dict:
    """
    Apply a PUSH to the bankroll: unchanged
    """
    current = await get_current_bankroll(db_query)

    await db_execute(f"""
        INSERT INTO bankroll (dato, amount, change_amount, change_reason{', dagens_kamp_id' if dagens_kamp_id else ''})
        VALUES (
            CURRENT_DATE,
            {current},
            0,
            'PUSH — innsats returnert'
            {', ' + str(dagens_kamp_id) if dagens_kamp_id else ''}
        )
    """)

    return {
        "before": current,
        "after": current,
        "profit": 0,
        "goal": BANKROLL_GOAL,
        "progress_pct": min(100, round(current / BANKROLL_GOAL * 100, 2)),
    }


def format_daily_summary_telegram(
    bankroll: float,
    bankroll_goal: float,
    dagens_kamp_result: Optional[str],
    dagens_kamp_match: Optional[str],
    total_picks: int,
    wins: int,
    losses: int,
) -> str:
    """Format the 23:00 daily summary Telegram message"""
    progress_pct = min(100, (bankroll / bankroll_goal) * 100)
    progress_bar = _progress_bar(progress_pct)
    settled = wins + losses
    winrate = round(wins / settled * 100, 1) if settled > 0 else 0

    result_line = ""
    if dagens_kamp_result and dagens_kamp_match:
        emoji = {"W": "✅", "L": "❌", "P": "↩️"}.get(dagens_kamp_result, "⏳")
        result_line = f"\nDagens kamp: {dagens_kamp_match} {emoji}"

    return f"""📊 <b>DAGLIG OPPSUMMERING — {date.today().strftime('%d.%m.%Y')}</b>
{result_line}

💰 <b>Bankroll: {bankroll:,.0f}kr</b>
{progress_bar} {progress_pct:.1f}% av {bankroll_goal:,.0f}kr

📈 Totalt: {total_picks} picks | {wins}W/{losses}L | {winrate}% winrate

📅 I morgen: Ny analyse kl. 06:00!

<i>⚠️ {DISCLAIMER}</i>
<i>SesomNod Engine · Automatisk rapport</i>"""


def _progress_bar(pct: float, length: int = 10) -> str:
    filled = int(pct / 100 * length)
    bar = "█" * filled + "░" * (length - filled)
    return f"[{bar}]"
