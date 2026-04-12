"""
No-Bet Verdict Engine
---------------------
1. log_rejected_pick  -- inserts rejected outcomes into no_bet_log (fire-and-forget safe)
2. fill_no_bet_verdicts -- backfills verdict for matches that have finished
"""

import logging
from datetime import datetime, timezone

logger = logging.getLogger("sesomnod")


async def log_rejected_pick(
    pool,
    home_team: str,
    away_team: str,
    league: str,
    kickoff_time: str | None,
    market_type: str,
    edge_pct: float | None,
    omega_score: float | None,
    rejection_reason: str,
) -> None:
    """
    Insert a rejected pick into no_bet_log.
    Acquires its own connection from pool -- safe for asyncio.create_task.
    ON CONFLICT DO NOTHING prevents duplicates.
    NEVER raises -- always try/except with logging.
    """
    try:
        kickoff_dt = None
        if kickoff_time:
            try:
                kickoff_dt = datetime.fromisoformat(
                    kickoff_time.replace("Z", "+00:00")
                )
            except (ValueError, AttributeError):
                pass

        async with pool.acquire() as db:
            await db.execute(
                """
                INSERT INTO no_bet_log
                    (scan_date, home_team, away_team, league, kickoff_time,
                     market_type, edge_pct, omega_score, rejection_reason)
                VALUES (CURRENT_DATE, $1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (scan_date, home_team, away_team, market_type) DO NOTHING
                """,
                home_team,
                away_team,
                league,
                kickoff_dt,
                market_type,
                edge_pct,
                omega_score,
                rejection_reason,
            )
    except Exception as e:
        logger.warning(f"[NoBet] log_rejected_pick feil (non-fatal): {e}")


async def fill_no_bet_verdicts(db) -> dict:
    """
    Find no_bet_log entries where verdict IS NULL and kickoff has passed (>3h ago).
    Cross-reference with picks_v2 to determine if the rejection was correct.

    Verdicts:
      CORRECT_PASS  -- match result confirms the bet would have lost
      WRONG_PASS    -- match result confirms the bet would have won
      CORRECT_BLOCK -- no matching pick in picks_v2, result unknown (our system skipped it)
      INCONCLUSIVE  -- match result not yet available or push/void
    """
    try:
        rows = await db.fetch(
            """
            SELECT id, home_team, away_team, market_type, kickoff_time, edge_pct
            FROM no_bet_log
            WHERE verdict IS NULL
              AND kickoff_time < NOW() - INTERVAL '3 hours'
            ORDER BY kickoff_time ASC
            LIMIT 200
            """
        )

        if not rows:
            return {"processed": 0, "filled": 0}

        filled = 0
        for row in rows:
            try:
                # Try to find matching result in picks_v2 (same teams, same day)
                result_row = await db.fetchrow(
                    """
                    SELECT result, pick
                    FROM picks_v2
                    WHERE home_team = $1 AND away_team = $2
                      AND kickoff::date = $3::date
                      AND result IS NOT NULL
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    row["home_team"],
                    row["away_team"],
                    row["kickoff_time"],
                )

                if not result_row:
                    # Check dagens_kamp for result
                    dk_row = await db.fetchrow(
                        """
                        SELECT result
                        FROM dagens_kamp
                        WHERE home_team = $1 AND away_team = $2
                          AND kickoff::date = $3::date
                          AND result IS NOT NULL
                        ORDER BY timestamp DESC
                        LIMIT 1
                        """,
                        row["home_team"],
                        row["away_team"],
                        row["kickoff_time"],
                    )
                    if dk_row:
                        result_str = dk_row["result"]
                        # We rejected this pick -- if the result was a loss for a hypothetical bet,
                        # our rejection was correct
                        verdict = _determine_verdict_from_result(
                            result_str, row["market_type"], row["edge_pct"]
                        )
                    else:
                        verdict = "INCONCLUSIVE"
                        result_str = None
                else:
                    result_str = result_row["result"]
                    pick_label = result_row["pick"]
                    verdict = _determine_verdict_with_pick(
                        result_str, pick_label, row["market_type"]
                    )

                explanation = _build_explanation(verdict, result_str, row["edge_pct"])

                await db.execute(
                    """
                    UPDATE no_bet_log
                    SET verdict = $1,
                        verdict_explanation = $2,
                        match_result = $3,
                        verdict_filled_at = NOW()
                    WHERE id = $4
                    """,
                    verdict,
                    explanation,
                    result_str,
                    row["id"],
                )
                filled += 1

            except Exception as e:
                logger.warning(f"[NoBet] verdict fill feil for id={row['id']}: {e}")
                continue

        return {"processed": len(rows), "filled": filled}

    except Exception as e:
        logger.error(f"[NoBet] fill_no_bet_verdicts feil: {e}")
        return {"processed": 0, "filled": 0, "error": str(e)}


def _determine_verdict_from_result(
    result_str: str | None, market_type: str | None, edge_pct: float | None
) -> str:
    """Determine verdict when we only have the match result (no pick in picks_v2)."""
    if not result_str:
        return "INCONCLUSIVE"

    result_upper = result_str.upper().strip()

    if result_upper in ("VOID", "CANCELLED", "POSTPONED", "PUSH"):
        return "INCONCLUSIVE"

    # For rejected picks we don't have enough info about which side was rejected,
    # so use edge as a proxy: low edge rejection was likely correct
    if edge_pct is not None and edge_pct < 2.0:
        return "CORRECT_PASS"

    return "CORRECT_BLOCK"


def _determine_verdict_with_pick(
    result_str: str | None, pick_label: str | None, market_type: str | None
) -> str:
    """Determine verdict when a matching pick exists in picks_v2."""
    if not result_str:
        return "INCONCLUSIVE"

    result_upper = result_str.upper().strip()

    if result_upper in ("VOID", "CANCELLED", "POSTPONED", "PUSH"):
        return "INCONCLUSIVE"

    if result_upper == "WIN":
        # The bet would have won -- our rejection was wrong
        return "WRONG_PASS"
    elif result_upper == "LOSS":
        # The bet would have lost -- our rejection was correct
        return "CORRECT_PASS"

    return "INCONCLUSIVE"


def _build_explanation(verdict: str, result_str: str | None, edge_pct: float | None) -> str:
    """Build human-readable explanation for the verdict."""
    edge_str = f"edge {edge_pct:.1f}%" if edge_pct is not None else "edge N/A"

    if verdict == "CORRECT_PASS":
        return f"Riktig avvisning ({edge_str}). Kamp: {result_str or 'ukjent'}."
    elif verdict == "WRONG_PASS":
        return f"Feil avvisning ({edge_str}). Bet ville vunnet. Kamp: {result_str or 'ukjent'}."
    elif verdict == "CORRECT_BLOCK":
        return f"System blokkerte korrekt ({edge_str}). Ingen kvalifisert pick."
    else:
        return f"Resultat ikke tilgjengelig enda ({edge_str})."
