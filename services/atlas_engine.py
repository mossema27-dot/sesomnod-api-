"""
ATLAS Engine v1.0 — Auto-CLV Closer + Decision Quality Score (DQS)
===================================================================
- NEVER invents or estimates closing odds
- Authoritative CLV source: MiroFish /clv/{pick_id} (Pinnacle no-vig closing)
- DQS with null CLV is still valid (partial score)
- Errors never crash the API
"""

import logging
from datetime import datetime, timezone
import httpx

logger = logging.getLogger("atlas")

_MIROFISH_URL = "https://mirofish-service-production.up.railway.app"


def _build_mirofish_pick_id(home: str, away: str, kickoff, market_type: str | None) -> str | None:
    """
    Reconstruct the MiroFish pick_id using the SAME format as
    main.py::_submit_result_to_mirofish and _log_pick_to_mirofish.
    Format: {home_slug}-{away_slug}-{YYYYMMDD}-{market_type}
    Returns None if required fields are missing.
    """
    if not home or not away or kickoff is None:
        return None
    try:
        if hasattr(kickoff, "strftime"):
            if kickoff.tzinfo is None:
                kickoff = kickoff.replace(tzinfo=timezone.utc)
            date_str = kickoff.strftime("%Y%m%d")
        else:
            dt = datetime.fromisoformat(str(kickoff).replace("Z", "+00:00"))
            date_str = dt.strftime("%Y%m%d")
    except Exception:
        return None
    home_slug = str(home).lower().replace(" ", "-")
    away_slug = str(away).lower().replace(" ", "-")
    mt = str(market_type or "h2h").lower().replace(" ", "_")
    return f"{home_slug}-{away_slug}-{date_str}-{mt}"


async def _fetch_mirofish_clv(pick_id: str) -> tuple[float | None, str]:
    """
    Fetch clv_pct from MiroFish /clv/{pick_id}.
    Returns (clv_pct, reason). clv_pct is None on any non-authoritative state.
    reason is one of: ok | not_tracked | no_closing_odds_yet | http_{code} | exception
    Never raises.
    """
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(f"{_MIROFISH_URL}/clv/{pick_id}")
        if r.status_code == 404:
            return (None, "not_tracked")
        if r.status_code != 200:
            return (None, f"http_{r.status_code}")
        body = r.json() if r.content else {}
        pick_data = body.get("pick", {}) if isinstance(body, dict) else {}
        closing_odds = pick_data.get("closing_odds")
        if closing_odds is None:
            return (None, "no_closing_odds_yet")
        # MiroFish response field is "clv" (not "clv_pct") — verified via live GET.
        clv_raw = pick_data.get("clv")
        if clv_raw is None:
            return (None, "no_clv_yet")
        try:
            return (float(clv_raw), "ok")
        except (TypeError, ValueError):
            return (None, "invalid_clv")
    except Exception as e:
        logger.warning(f"[ATLAS] MiroFish fetch error for {pick_id}: {e}")
        return (None, "exception")


def atlas_calculate_dqs(
    clv_pct: float | None,
    edge_pct: float | None,
    kelly_verified: bool | None,
) -> dict:
    """
    Calculate Decision Quality Score (0-100) with grade A/B/C/D.

    Components:
      - CLV:   50 points max (real Pinnacle CLV only)
      - Edge:  30 points max (soft edge at pick time)
      - Kelly: 20 points (verified Kelly sizing)

    Returns dict with: dqs_score, dqs_grade, dqs_verdict,
                       clv_component, edge_component, kelly_component
    """
    # ── CLV component: 50 points max ──
    clv_component = 0.0
    if clv_pct is not None:
        if clv_pct > 3.0:
            clv_component = 50.0
        elif clv_pct > 1.0:
            clv_component = 35.0
        elif clv_pct > 0.0:
            clv_component = 20.0
        elif clv_pct > -2.0:
            clv_component = 10.0
        else:
            clv_component = 0.0

    # ── Edge component: 30 points max ──
    edge_component = 0.0
    if edge_pct is not None:
        if edge_pct >= 10.0:
            edge_component = 30.0
        elif edge_pct >= 8.0:
            edge_component = 22.0
        elif edge_pct >= 5.0:
            edge_component = 12.0
        else:
            edge_component = 0.0

    # ── Kelly component: 20 points ──
    kelly_ok = bool(kelly_verified)
    kelly_component_val = 20.0 if kelly_ok else 0.0

    # ── Total DQS ──
    dqs_score = clv_component + edge_component + kelly_component_val

    # ── Grade ──
    if dqs_score >= 80:
        dqs_grade = "A"
        dqs_verdict = "Elite decision — CLV + edge + sizing aligned"
    elif dqs_score >= 60:
        dqs_grade = "B"
        dqs_verdict = "Strong decision — most components verified"
    elif dqs_score >= 40:
        dqs_grade = "C"
        dqs_verdict = "Partial quality — room for improvement"
    else:
        dqs_grade = "D"
        dqs_verdict = "Weak decision — review process"

    # Tag partial if CLV is missing
    if clv_pct is None:
        dqs_verdict += " [CLV pending]"

    return {
        "dqs_score": round(dqs_score, 1),
        "dqs_grade": dqs_grade,
        "dqs_verdict": dqs_verdict,
        "clv_component": round(clv_component, 2),
        "edge_component": round(edge_component, 2),
        "kelly_component": kelly_ok,
    }


async def atlas_run_clv_closer(db) -> dict:
    """
    Main ATLAS job: finds settled picks without DQS and scores them.

    Process:
      1. Find settled picks (WIN/LOSS/VOID in picks_v2) with receipts but no DQS
      2. Get real CLV: picks_v2.pinnacle_clv first, then pick_receipts.clv_pct
      3. NEVER guesses — skips if no real CLV (but still scores partial DQS)
      4. Syncs CLV from picks_v2 to pick_receipts if available
      5. Calculates and stores DQS

    Args:
        db: asyncpg connection (already acquired from pool)

    Returns:
        dict with counts: processed, scored, clv_synced, errors
    """
    results = {
        "processed": 0,
        "scored": 0,
        "clv_synced": 0,
        "skipped": 0,
        "errors": 0,
        "details": [],
    }

    try:
        # Ensure DQS table exists (idempotent safety net)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS decision_quality_scores (
                id SERIAL PRIMARY KEY,
                receipt_id INTEGER UNIQUE REFERENCES pick_receipts(id) ON DELETE CASCADE,
                pick_id INTEGER REFERENCES picks_v2(id),
                dqs_score NUMERIC(5,1),
                dqs_grade CHAR(1),
                dqs_verdict TEXT,
                clv_component NUMERIC(5,2),
                edge_component NUMERIC(5,2),
                kelly_component BOOLEAN,
                calculated_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_dqs_receipt ON decision_quality_scores(receipt_id);
            CREATE INDEX IF NOT EXISTS idx_dqs_grade ON decision_quality_scores(dqs_grade);
        """)
        # Ensure clv_source column exists
        await db.execute("""
            ALTER TABLE pick_receipts ADD COLUMN IF NOT EXISTS clv_source VARCHAR(32);
        """)

        # Find settled picks with receipts but no DQS yet.
        # Extra fields (home_team, away_team, kickoff_time, market_type) are
        # needed to reconstruct the MiroFish pick_id for authoritative CLV.
        # LATERAL subquery guarantees at most ONE dagens_kamp row per receipt
        # — dagens_kamp has no UNIQUE on (home,away,date) so a plain JOIN
        # could fan-out and process the same receipt multiple times.
        rows = await db.fetch("""
            SELECT
                r.id AS receipt_id,
                r.pick_id,
                r.match_name,
                r.kickoff AS receipt_kickoff,
                r.result_outcome,
                r.clv_pct AS receipt_clv,
                r.clv_verified AS receipt_clv_verified,
                r.edge_pct,
                r.kelly_verified,
                p.pinnacle_clv,
                p.result AS picks_v2_result,
                p.soft_edge,
                p.home_team AS pv_home,
                p.away_team AS pv_away,
                p.kickoff_time AS pv_kickoff,
                dk_one.market_type AS dk_market_type
            FROM pick_receipts r
            LEFT JOIN picks_v2 p ON r.pick_id = p.id
            LEFT JOIN LATERAL (
                SELECT market_type
                FROM dagens_kamp
                WHERE home_team = p.home_team
                  AND away_team = p.away_team
                  AND kickoff::date = p.kickoff_time::date
                ORDER BY id DESC
                LIMIT 1
            ) dk_one ON TRUE
            WHERE r.result_outcome IN ('WIN', 'LOSS', 'VOID')
              AND NOT EXISTS (
                  SELECT 1 FROM decision_quality_scores d
                  WHERE d.receipt_id = r.id
              )
            ORDER BY r.created_at ASC
        """)

        if not rows:
            logger.info("[ATLAS] No settled picks without DQS found")
            return results

        logger.info(f"[ATLAS] Found {len(rows)} settled picks to score")

        for row in rows:
            results["processed"] += 1
            try:
                receipt_id = row["receipt_id"]
                pick_id = row["pick_id"]
                match_name = row["match_name"] or "Unknown"

                # ── Resolve CLV from authoritative source (RC #2) ──
                # Doctrine: only Pinnacle no-vig closing is valid CLV.
                # Authoritative source in this system: MiroFish /clv/{pick_id}.
                # If pick_receipts.clv_pct already has a value, keep it (idempotent).
                # Otherwise fetch from MiroFish; 404/no_closing_odds_yet => skip silently.
                clv_pct = None
                clv_source = None

                if row["receipt_clv"] is not None:
                    clv_pct = float(row["receipt_clv"])
                    clv_source = "pick_receipts.clv_pct"
                else:
                    mf_pick_id = _build_mirofish_pick_id(
                        row["pv_home"],
                        row["pv_away"],
                        row["pv_kickoff"],
                        row["dk_market_type"],
                    )
                    if mf_pick_id:
                        fetched_clv, reason = await _fetch_mirofish_clv(mf_pick_id)
                        if fetched_clv is not None:
                            # Race guard at DB-level: WHERE clv_pct IS NULL.
                            # Never overwrite an existing value.
                            up_res = await db.execute("""
                                UPDATE pick_receipts
                                SET clv_pct = $1, clv_verified = TRUE, clv_source = $2
                                WHERE id = $3 AND clv_pct IS NULL
                            """, fetched_clv, "mirofish", receipt_id)
                            if up_res != "UPDATE 0":
                                results["clv_synced"] += 1
                                logger.info(
                                    f"[ATLAS] CLV synced for {match_name}: "
                                    f"{fetched_clv:.2f}% (source=mirofish, pick_id={mf_pick_id})"
                                )
                            clv_pct = fetched_clv
                            clv_source = "mirofish"
                        else:
                            # Controlled skip — do not write NULL, do not crash.
                            logger.warning(
                                f"[ATLAS] MiroFish CLV unavailable for "
                                f"{match_name} (pick_id={mf_pick_id}, reason={reason})"
                            )
                    else:
                        logger.warning(
                            f"[ATLAS] Cannot build MiroFish pick_id for "
                            f"receipt {receipt_id} ({match_name}) — missing fields"
                        )

                # ── Resolve edge ──
                edge_pct = None
                if row["edge_pct"] is not None:
                    edge_pct = float(row["edge_pct"])
                elif row["soft_edge"] is not None:
                    edge_pct = float(row["soft_edge"])

                # ── Kelly verified ──
                kelly_verified = row["kelly_verified"]

                # ── Calculate DQS ──
                dqs = atlas_calculate_dqs(clv_pct, edge_pct, kelly_verified)

                # ── Store DQS ──
                await db.execute("""
                    INSERT INTO decision_quality_scores
                        (receipt_id, pick_id, dqs_score, dqs_grade, dqs_verdict,
                         clv_component, edge_component, kelly_component, calculated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (receipt_id) DO UPDATE SET
                        dqs_score = EXCLUDED.dqs_score,
                        dqs_grade = EXCLUDED.dqs_grade,
                        dqs_verdict = EXCLUDED.dqs_verdict,
                        clv_component = EXCLUDED.clv_component,
                        edge_component = EXCLUDED.edge_component,
                        kelly_component = EXCLUDED.kelly_component,
                        calculated_at = EXCLUDED.calculated_at
                """, receipt_id, pick_id,
                    dqs["dqs_score"], dqs["dqs_grade"], dqs["dqs_verdict"],
                    dqs["clv_component"], dqs["edge_component"],
                    dqs["kelly_component"],
                    datetime.now(timezone.utc),
                )

                results["scored"] += 1
                results["details"].append({
                    "match": match_name,
                    "outcome": row["result_outcome"],
                    "clv_pct": clv_pct,
                    "clv_source": clv_source,
                    "dqs_score": dqs["dqs_score"],
                    "dqs_grade": dqs["dqs_grade"],
                })
                logger.info(
                    f"[ATLAS] DQS scored: {match_name} | "
                    f"Grade={dqs['dqs_grade']} Score={dqs['dqs_score']} | "
                    f"CLV={clv_pct} ({clv_source or 'N/A'})"
                )

            except Exception as e:
                results["errors"] += 1
                logger.error(f"[ATLAS] Error scoring receipt {row.get('receipt_id')}: {e}")

        logger.info(
            f"[ATLAS] Complete: processed={results['processed']} "
            f"scored={results['scored']} synced={results['clv_synced']} "
            f"errors={results['errors']}"
        )

    except Exception as e:
        logger.error(f"[ATLAS] Fatal error in CLV closer: {e}")
        results["errors"] += 1

    return results
