"""
Shared SQL queries for picks_v2.

Eliminer SQL-fragmentering: alle endpoints som SELECTer picks for V2/Truth-flow
bruker samme funksjon. Garanterer at /admin/test-decision-desk-v2,
/admin/pre-dominant-pool, /admin/batch-recompute-edge-events, og
/admin/edge-truth-dashboard ser samme data.

asyncpg parameter-konvensjon: $1, $2 (ikke %s).
"""
from __future__ import annotations


# Eneste sannhets-SELECT for V2/Truth-pipeline. Kolonner valgt for å være
# self-contained for compute_edge_event + dashboard-aggregat.
_BASE_COLUMNS = """
    id, match_name, home_team, away_team, league, kickoff_time,
    odds, soft_edge, soft_ev,
    home_odds_raw, draw_odds_raw, away_odds_raw,
    atomic_score, tier, signals_triggered,
    dc_home_win_prob, dc_draw_prob, dc_away_win_prob,
    dc_btts_prob, dc_lambda_home, dc_lambda_away,
    dc_over_15, dc_over_25, dc_over_35,
    dc_under_25, dc_under_35,
    outcome, status, result, created_at, updated_at
"""


def get_base_picks_query(
    max_age_days: int = 30,
    require_dc_data: bool = True,
    tiers: list[str] | None = None,
    limit: int | None = 100,
) -> tuple[str, list]:
    """
    Bygg SELECT for V2/Truth-pipeline med konsistente filtre.

    Returns (sql, params). Bruk: `await conn.fetch(sql, *params)`.

    Standard-tiers: ATOMIC + EDGE (matcher eksisterende
    /admin/test-decision-desk-v2 oppførsel).
    """
    if tiers is None:
        tiers = ["ATOMIC", "EDGE"]

    sql = f"SELECT {_BASE_COLUMNS} FROM picks_v2 WHERE 1=1"
    params: list = []
    idx = 1

    if max_age_days is not None and max_age_days > 0:
        # kickoff_time-basert: matcher backfill-job semantikk og er
        # semantisk korrekt for Truth Layer (vi vil se utfall, ikke når
        # rad ble laget).
        sql += f" AND kickoff_time > NOW() - (${idx} || ' days')::interval"
        params.append(str(max_age_days))
        idx += 1

    if require_dc_data:
        sql += " AND dc_home_win_prob IS NOT NULL AND dc_home_win_prob > 0"

    if tiers:
        sql += f" AND tier = ANY(${idx}::text[])"
        params.append(tiers)
        idx += 1

    sql += " ORDER BY id DESC"

    if limit is not None and limit > 0:
        sql += f" LIMIT ${idx}"
        params.append(limit)

    return sql, params


def normalize_picks_row(row) -> dict:
    """
    Konverter asyncpg.Record til dict + parse signals_triggered hvis JSON-string.
    Felles post-processing for alle endpoints.
    """
    import json
    pick = dict(row)
    sigs = pick.get("signals_triggered")
    if isinstance(sigs, str):
        try:
            pick["signals_triggered"] = json.loads(sigs)
        except (ValueError, TypeError):
            pick["signals_triggered"] = []
    return pick
