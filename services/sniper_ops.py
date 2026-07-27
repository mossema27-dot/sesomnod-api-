"""
Sniper Ops — admin-operasjoner for sniper-pipeline.

Samler diagnostikk, manuell trigger, og kill-switch logikk
som tidligere lå spredt i main.py /admin/sniper-* endepunkter.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("sesomnod.sniper_ops")


async def get_sniper_dashboard(pool) -> dict[str, Any]:
    """Hent 30-dagers sniper-statistikk gruppert per tier."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT tier,
                   COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE result = 'WIN') AS wins,
                   COUNT(*) FILTER (WHERE result = 'LOSS') AS losses,
                   COUNT(*) FILTER (WHERE result IS NULL) AS pending,
                   ROUND(AVG(clv_close_pct)::numeric, 4) AS avg_clv,
                   ROUND(SUM(profit_units)::numeric, 4) AS total_profit
            FROM sniper_bets_v1
            WHERE created_at >= NOW() - INTERVAL '30 days'
            GROUP BY tier
            ORDER BY tier
        """)
    tiers = [dict(r) for r in rows]
    total_picks = sum(t["total"] for t in tiers)
    total_wins = sum(t["wins"] for t in tiers)
    return {
        "window_days": 30,
        "tiers": tiers,
        "total_picks": total_picks,
        "win_rate": round(total_wins / total_picks, 4) if total_picks else None,
    }


async def get_sniper_clv_distribution(pool) -> dict[str, Any]:
    """CLV-fordeling for settlete sniper-picks."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT pick_id, tier, clv_close_pct, result, profit_units
            FROM sniper_bets_v1
            WHERE result IS NOT NULL AND clv_close_pct IS NOT NULL
            ORDER BY created_at DESC
        """)
    picks = [dict(r) for r in rows]
    clv_values = [p["clv_close_pct"] for p in picks if p["clv_close_pct"] is not None]
    return {
        "settled_count": len(picks),
        "avg_clv": round(sum(clv_values) / len(clv_values), 4) if clv_values else None,
        "picks": picks,
    }


async def get_protocol_status(pool) -> dict[str, Any]:
    """30-dagers protokoll-status med kill-switch evaluering."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE result IS NOT NULL) AS settled,
                COUNT(*) FILTER (WHERE result = 'WIN') AS wins,
                COUNT(*) FILTER (WHERE result = 'LOSS') AS losses,
                ROUND(AVG(clv_close_pct) FILTER (WHERE result IS NOT NULL)::numeric, 4) AS avg_clv,
                ROUND(SUM(profit_units) FILTER (WHERE result IS NOT NULL)::numeric, 4) AS total_profit,
                MIN(created_at) AS first_pick
            FROM sniper_bets_v1
            WHERE created_at >= NOW() - INTERVAL '30 days'
        """)
    data = dict(row) if row else {}
    settled = data.get("settled", 0) or 0
    wins = data.get("wins", 0) or 0
    losses = data.get("losses", 0) or 0
    total_profit = float(data.get("total_profit", 0) or 0)

    # Kill-switch: ROI < -15% med minst 10 settled picks
    roi_pct = round(total_profit / settled * 100, 2) if settled else 0
    auto_stop = settled >= 10 and roi_pct < -15

    return {
        "window_days": 30,
        "total": data.get("total", 0),
        "settled": settled,
        "wins": wins,
        "losses": losses,
        "win_rate": round(wins / settled, 4) if settled else None,
        "avg_clv": data.get("avg_clv"),
        "roi_pct": roi_pct,
        "auto_stop_required": auto_stop,
        "first_pick_at": str(data.get("first_pick")) if data.get("first_pick") else None,
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
    }
