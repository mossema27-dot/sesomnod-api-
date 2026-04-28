"""
Edge Truth Engine — én sannhetskilde for edge-events per pick.

Bygger payload klar for INSERT i edge_events_v1. Pure functions for
compute_edge_event + format_for_db. DB-skriving via upsert_edge_event +
batch_populate_edge_events (krever asyncpg-pool fra caller).

Brukes av:
- POST /admin/batch-recompute-edge-events
- GET /admin/edge-truth-dashboard
- (fremtidig) real-time pipeline etter pick-generering

Komposisjon (alle eksisterende funksjoner, ingen duplisering):
1. enrich_implied_inline (probability_event_generator)
2. _build_event (probability_event_generator) — VEI A edge-fix
3. _enrich_event_with_filter_meta (smartpick_narratives) — confidence + dom_v2
4. passes_all_filters_v2 (smartpick_narratives) — 3-lag pass + reasons
5. compute_filter_diagnostics (smartpick_narratives) — near_miss + pre_dominant
6. calculate_dominance_score_v2 (smartpick_narratives)
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("sesomnod.edge_truth")


# ── Schema (ALTER-statement holdes her som single source of truth) ──────────
EDGE_EVENTS_V1_SCHEMA = """
CREATE TABLE IF NOT EXISTS edge_events_v1 (
    id BIGSERIAL PRIMARY KEY,
    pick_id BIGINT NOT NULL,
    match_id TEXT,
    home_team TEXT,
    away_team TEXT,
    league TEXT,
    kickoff_time TIMESTAMPTZ,

    event_type TEXT NOT NULL,
    event_category TEXT,

    model_prob_pct FLOAT,
    market_implied_pct FLOAT,
    market_source TEXT,

    edge_pct FLOAT,
    odds FLOAT,

    passes_l1_data_quality BOOLEAN,
    passes_l2_statistical BOOLEAN,
    passes_l3_business BOOLEAN,
    is_dominant BOOLEAN,
    is_pre_dominant BOOLEAN,

    failed_reasons TEXT[],
    distance_to_edge_threshold FLOAT,
    distance_to_prob_threshold FLOAT,
    distance_to_conf_threshold FLOAT,
    near_miss BOOLEAN,

    outcome TEXT,
    settled_at TIMESTAMPTZ,

    confidence_tier TEXT,
    confidence FLOAT,
    dominance_score FLOAT,
    extreme_edge_flag BOOLEAN DEFAULT FALSE,

    computed_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(pick_id, event_type)
);

CREATE INDEX IF NOT EXISTS idx_edge_events_pick ON edge_events_v1(pick_id);
CREATE INDEX IF NOT EXISTS idx_edge_events_kickoff ON edge_events_v1(kickoff_time);
CREATE INDEX IF NOT EXISTS idx_edge_events_dominant ON edge_events_v1(is_dominant);
CREATE INDEX IF NOT EXISTS idx_edge_events_pre_dominant ON edge_events_v1(is_pre_dominant);
CREATE INDEX IF NOT EXISTS idx_edge_events_event_type ON edge_events_v1(event_type);
"""


def _confidence_to_tier(conf: float) -> str:
    if conf >= 0.95:
        return "PLATINUM"
    if conf >= 0.85:
        return "GOLD"
    if conf >= 0.70:
        return "SILVER"
    return "BRONZE"


def compute_edge_event(pick: dict, event_def: dict) -> dict | None:
    """
    Komputer edge_event-payload for én pick + event_def.

    Returnerer dict klar for upsert ELLER None hvis:
    - pick mangler dc-data (LIMITED completeness)
    - event er INFO_EVENT (totals/BTTS uten odds)
    - coherence-feil (range, realisme)

    Pure function. Ingen DB-skriving.
    """
    from services.probability_event_generator import (
        enrich_implied_inline,
        _build_event as _gen_build_event,
        _data_completeness,
    )
    from services.smartpick_narratives import (
        _enrich_event_with_filter_meta,
        passes_all_filters_v2,
        compute_filter_diagnostics,
    )

    completeness = _data_completeness(pick)
    if completeness == "LIMITED":
        return None

    enriched_pick = enrich_implied_inline(pick)

    base_event = _gen_build_event(enriched_pick, event_def, completeness)
    if base_event is None:
        return None

    enriched_event = _enrich_event_with_filter_meta(base_event, pick)

    passed_all, failed_reasons = passes_all_filters_v2(enriched_event, pick)
    diagnostics = compute_filter_diagnostics(enriched_event, pick)

    passes_l1 = not any(r.startswith("L1:") for r in failed_reasons)
    passes_l2 = not any(r.startswith("L2:") for r in failed_reasons)
    passes_l3 = not any(r.startswith("L3:") for r in failed_reasons)

    confidence = float(enriched_event.get("confidence") or 0.0)
    dominance = float(enriched_event.get("dominance_score_v2") or 0.0)

    return {
        "pick_id": pick.get("id") or pick.get("pick_id"),
        "match_id": pick.get("match_id") or pick.get("match_name"),
        "home_team": pick.get("home_team", ""),
        "away_team": pick.get("away_team", ""),
        "league": pick.get("league") or "",
        "kickoff_time": pick.get("kickoff_time"),

        "event_type": base_event["label"],
        "event_category": base_event.get("category", "main"),

        "model_prob_pct": base_event["probability_pct"],
        "market_implied_pct": base_event.get("market_implied_pct"),
        "market_source": base_event.get("market_source"),

        "edge_pct": base_event.get("edge_pct"),
        "odds": base_event.get("odds"),

        "passes_l1_data_quality": passes_l1,
        "passes_l2_statistical": passes_l2,
        "passes_l3_business": passes_l3,
        "is_dominant": passed_all,
        "is_pre_dominant": bool(diagnostics.get("pre_dominant")),

        "failed_reasons": list(failed_reasons),
        "distance_to_edge_threshold": diagnostics["distance_to_threshold"]["edge_pp"],
        "distance_to_prob_threshold": diagnostics["distance_to_threshold"]["prob_pp"],
        "distance_to_conf_threshold": diagnostics["distance_to_threshold"]["conf_pp"],
        "near_miss": bool(diagnostics.get("near_miss")),

        # outcome krever event-vs-score mapping (TODO: separat backfill-job).
        # pick.outcome refererer til pick's predicted_outcome, ikke per-event-utfall.
        "outcome": None,
        "settled_at": pick.get("settled_at"),

        "confidence_tier": _confidence_to_tier(confidence),
        "confidence": confidence,
        "dominance_score": dominance,
        "extreme_edge_flag": bool(base_event.get("extreme_edge_flag")),
    }


def compute_all_edge_events_for_pick(pick: dict) -> list[dict]:
    """
    Bygg alle edge_event-payloads for én pick.
    Returnerer liste (kan være tom hvis ingen events kvalifiserer).
    """
    from services.probability_event_generator import EVENT_DEFS

    payloads = []
    for event_def in EVENT_DEFS:
        payload = compute_edge_event(pick, event_def)
        if payload:
            payloads.append(payload)
    return payloads


# ── DB-skriving (krever asyncpg-pool fra caller) ────────────────────────────

UPSERT_SQL = """
INSERT INTO edge_events_v1 (
    pick_id, match_id, home_team, away_team, league, kickoff_time,
    event_type, event_category,
    model_prob_pct, market_implied_pct, market_source, edge_pct, odds,
    passes_l1_data_quality, passes_l2_statistical, passes_l3_business,
    is_dominant, is_pre_dominant,
    failed_reasons,
    distance_to_edge_threshold, distance_to_prob_threshold, distance_to_conf_threshold,
    near_miss,
    outcome, settled_at,
    confidence_tier, confidence, dominance_score, extreme_edge_flag,
    computed_at
) VALUES (
    $1, $2, $3, $4, $5, $6,
    $7, $8,
    $9, $10, $11, $12, $13,
    $14, $15, $16,
    $17, $18,
    $19,
    $20, $21, $22,
    $23,
    $24, $25,
    $26, $27, $28, $29,
    NOW()
)
ON CONFLICT (pick_id, event_type) DO UPDATE SET
    model_prob_pct = EXCLUDED.model_prob_pct,
    market_implied_pct = EXCLUDED.market_implied_pct,
    market_source = EXCLUDED.market_source,
    edge_pct = EXCLUDED.edge_pct,
    odds = EXCLUDED.odds,
    passes_l1_data_quality = EXCLUDED.passes_l1_data_quality,
    passes_l2_statistical = EXCLUDED.passes_l2_statistical,
    passes_l3_business = EXCLUDED.passes_l3_business,
    is_dominant = EXCLUDED.is_dominant,
    is_pre_dominant = EXCLUDED.is_pre_dominant,
    failed_reasons = EXCLUDED.failed_reasons,
    distance_to_edge_threshold = EXCLUDED.distance_to_edge_threshold,
    distance_to_prob_threshold = EXCLUDED.distance_to_prob_threshold,
    distance_to_conf_threshold = EXCLUDED.distance_to_conf_threshold,
    near_miss = EXCLUDED.near_miss,
    outcome = COALESCE(EXCLUDED.outcome, edge_events_v1.outcome),
    settled_at = COALESCE(EXCLUDED.settled_at, edge_events_v1.settled_at),
    confidence_tier = EXCLUDED.confidence_tier,
    confidence = EXCLUDED.confidence,
    dominance_score = EXCLUDED.dominance_score,
    extreme_edge_flag = EXCLUDED.extreme_edge_flag,
    computed_at = NOW();
"""


def _payload_to_args(p: dict) -> tuple:
    return (
        p["pick_id"], p.get("match_id"), p["home_team"], p["away_team"],
        p["league"], p.get("kickoff_time"),
        p["event_type"], p.get("event_category"),
        p["model_prob_pct"], p.get("market_implied_pct"),
        p.get("market_source"), p.get("edge_pct"), p.get("odds"),
        p["passes_l1_data_quality"], p["passes_l2_statistical"],
        p["passes_l3_business"],
        p["is_dominant"], p["is_pre_dominant"],
        p["failed_reasons"],
        p["distance_to_edge_threshold"], p["distance_to_prob_threshold"],
        p["distance_to_conf_threshold"],
        p["near_miss"],
        p.get("outcome"), p.get("settled_at"),
        p["confidence_tier"], p["confidence"], p["dominance_score"],
        p["extreme_edge_flag"],
    )


async def upsert_edge_event(conn, payload: dict) -> None:
    """Upsert én edge_event til edge_events_v1 via asyncpg-connection."""
    await conn.execute(UPSERT_SQL, *_payload_to_args(payload))


async def batch_populate_edge_events(pool, picks: list[dict]) -> dict:
    """
    Batch-populate edge_events_v1 fra picks-list. Idempotent.

    Returnerer summary stats (ingen DB-aggregat — caller kan SELECT etter).
    """
    total_events = 0
    picks_processed = 0
    picks_with_zero_events = 0
    errors: list[dict] = []

    async with pool.acquire() as conn:
        async with conn.transaction():
            for pick in picks:
                try:
                    payloads = compute_all_edge_events_for_pick(pick)
                    if not payloads:
                        picks_with_zero_events += 1
                    for payload in payloads:
                        await upsert_edge_event(conn, payload)
                        total_events += 1
                    picks_processed += 1
                except Exception as e:
                    logger.error(
                        "[EdgeTruth] pick %s failed: %s",
                        pick.get("id"), e,
                    )
                    errors.append({
                        "pick_id": pick.get("id"),
                        "error": str(e)[:200],
                    })

    return {
        "picks_processed": picks_processed,
        "picks_with_zero_events": picks_with_zero_events,
        "total_events_upserted": total_events,
        "errors": errors,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
