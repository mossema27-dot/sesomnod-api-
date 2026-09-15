"""
Offentlige endepunkter (brev §5.7), whitelist, cache 60 s.

  GET /public/oraklion/brain              → tilstand + siste beslutning + flat + chain_head
  GET /public/oraklion/brain/ledger.json  → hendelser servert FRA canonical (T20)

Envelope = {data, as_of, source, degraded} — samme som øvrige /public/oraklion/*.
Ingen bookmaker-navn, ingen interne hostnavn, ingen meta_private-felt.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Query

from . import METHODOLOGY_VERSION
from .ledger import chain_head, fetch_ledger

router = APIRouter()
_db_state: Any = None
_CACHE: dict = {}
CACHE_TTL_SEC = 60

PUBLIC_DECISION_KEYS = (
    "decision_id", "seq", "status", "record_type", "fixture_id", "league", "home", "away", "kickoff_utc",
    "market_key", "p_model", "odds_locked", "odds_ts_utc", "odds_ts_basis", "lock_age_min", "ev",
    "model_version", "selection_rule_version", "committed_utc", "outcome", "settled_utc",
    "settlement_rules_version", "odds_close", "close_ts_utc", "close_minutes_before",
    "closing_missing_reason", "clv_odds_pct", "clv_fair_pct", "brier_model", "brier_market",
    "published_utc",
)


def configure(db_state: Any) -> None:
    global _db_state
    _db_state = db_state


def _envelope(data: dict, *, degraded: bool = False, source: str = "live") -> dict:
    return {
        "data": data,
        "as_of": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": source,
        "degraded": degraded,
    }


def _cache_get(key: str) -> Optional[dict]:
    hit = _CACHE.get(key)
    if hit and (time.time() - hit[0]) < CACHE_TTL_SEC:
        return hit[1]
    return None


def _cache_set(key: str, value: dict) -> dict:
    _CACHE[key] = (time.time(), value)
    return value


def _jsonable(v: Any) -> Any:
    if isinstance(v, datetime):
        return v.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    return str(v)


def _public_decision(row: dict) -> dict:
    d = {k: _jsonable(row.get(k)) for k in PUBLIC_DECISION_KEYS}
    d["pre_kickoff_publication"] = (
        "EXTERNAL_VERIFIED" if row.get("published_utc") and row.get("kickoff_utc")
        and row["published_utc"] < row["kickoff_utc"] else "INTERNAL_ONLY")
    return d


def derive_engine_state(hb: Optional[dict], now: datetime, source_sla_sec: int) -> tuple[str, dict]:
    if not hb or not hb.get("last_ok_utc"):
        return "SCANNER_DOWN", {"reason": "NO_HEARTBEAT"}
    interval = int(hb.get("expected_interval_sec") or 900)
    if (now - hb["last_ok_utc"]).total_seconds() > 2 * interval:
        return "SCANNER_DOWN", {"reason": "LAST_OK_TOO_OLD", "last_ok_utc": _jsonable(hb["last_ok_utc"])}
    src = hb.get("last_source_fetch_utc")
    if not src or (now - src).total_seconds() > source_sla_sec:
        return "SOURCE_STALE", {"reason": "SOURCE_OLDER_THAN_SLA", "last_source_fetch_utc": _jsonable(src)}
    return "RUNNING", {}


async def _schema_present(conn) -> bool:
    return bool(await conn.fetchval("SELECT to_regclass('oraklion.brain_events') IS NOT NULL;"))


@router.get("/public/oraklion/brain")
async def public_oraklion_brain():
    cached = _cache_get("brain")
    if cached:
        return cached
    if _db_state is None or not getattr(_db_state, "connected", False) or not getattr(_db_state, "pool", None):
        return _cache_set("brain", _envelope({"engine_state": "SCANNER_DOWN", "decision_state": "HOLD",
                                              "methodology_version": METHODOLOGY_VERSION},
                                             degraded=True, source="db_unavailable"))
    now = datetime.now(timezone.utc)
    try:
        async with _db_state.pool.acquire() as conn:
            if not await _schema_present(conn):
                return _cache_set("brain", _envelope(
                    {"engine_state": "SCANNER_DOWN", "decision_state": "HOLD",
                     "methodology_version": METHODOLOGY_VERSION, "flat": None, "latest_decision": None},
                    degraded=True, source="schema_missing"))
            cfg_rows = await conn.fetch("SELECT key, value FROM oraklion.brain_config;")
            cfg = {r["key"]: r["value"] for r in cfg_rows}
            hb_row = await conn.fetchrow("SELECT * FROM oraklion.brain_heartbeat WHERE job_name='oraklion_brain_tick';")
            hb = dict(hb_row) if hb_row else None
            engine_state, engine_detail = derive_engine_state(hb, now, int(cfg.get("SOURCE_SLA_SEC", "108000")))

            open_row = await conn.fetchrow(
                "SELECT * FROM oraklion.brain_decisions WHERE status='OPEN' ORDER BY seq DESC LIMIT 1;")
            latest_row = open_row or await conn.fetchrow(
                "SELECT * FROM oraklion.brain_decisions ORDER BY seq DESC LIMIT 1;")
            n_susp = await conn.fetchval("SELECT count(*) FROM oraklion.brain_decisions WHERE status='SUSPENDED';")
            last_tick = await conn.fetchrow("SELECT * FROM oraklion.brain_ticks ORDER BY tick_utc DESC LIMIT 1;")
            ratio = await conn.fetchrow(
                """
                SELECT count(*) FILTER (WHERE reason = 'NO_CANDIDATES') AS holds,
                       count(*) FILTER (WHERE reason IS DISTINCT FROM 'OPEN_DECISION_EXISTS') AS ticks
                FROM oraklion.brain_ticks WHERE tick_utc >= now() - interval '30 days';
                """)
            snap = await conn.fetchrow(
                "SELECT payload, computed_utc FROM oraklion.brain_stats_snapshot ORDER BY computed_utc DESC LIMIT 1;")
            head = await chain_head(conn)

        if open_row:
            decision_state = "OPEN_DECISION"
        elif n_susp:
            decision_state = "SUSPENDED"
        else:
            decision_state = "HOLD"
        hold_reason = (last_tick["reason"] if last_tick else "NO_HEARTBEAT")
        explanation = {
            "RUNNING": "The engine scans the candidate stream every 15 minutes and holds until a candidate passes the gate.",
            "SOURCE_STALE": "The candidate source has not refreshed within its SLA. No new decisions are recorded.",
            "SCANNER_DOWN": "The scanner has not reported within two intervals. No new decisions are recorded.",
        }[engine_state]
        flat = json.loads(snap["payload"]) if snap and isinstance(snap["payload"], str) else (snap["payload"] if snap else None)
        if isinstance(flat, dict) and len(flat.get("equity_curve") or []) > 500:
            flat["equity_curve"] = flat["equity_curve"][-500:]
        data = {
            "generated_utc": _jsonable(now),
            "engine_state": engine_state,
            "engine_detail": engine_detail,
            "decision_state": decision_state,
            "hold_reason": hold_reason if decision_state == "HOLD" else None,
            "last_scan_ok_utc": _jsonable(hb["last_ok_utc"]) if hb else None,
            "explanation": explanation,
            "scanned_n": int(last_tick["scanned_n"]) if last_tick else 0,
            "passed_n": int(last_tick["passed_n"]) if last_tick else 0,
            "skip_counts": (json.loads(last_tick["skip_counts"]) if last_tick and isinstance(last_tick["skip_counts"], str)
                            else (last_tick["skip_counts"] if last_tick else {})),
            "hold_ratio_30d": (round(int(ratio["holds"]) / int(ratio["ticks"]), 4) if ratio and int(ratio["ticks"]) else None),
            "latest_decision": _public_decision(dict(latest_row)) if latest_row else None,
            "flat": flat,
            "flat_computed_utc": _jsonable(snap["computed_utc"]) if snap else None,
            "campaign": None,  # compound-simulering er M2
            "chain_head": {"seq": int(head["seq"]), "hash": head["hash"], "ts_utc": _jsonable(head["ts_utc"])} if head else None,
            "methodology_version": cfg.get("METHODOLOGY_VERSION", METHODOLOGY_VERSION),
            "selection_rule_version": cfg.get("SELECTION_RULE_VERSION"),
            "settlement_rules_version": cfg.get("SETTLEMENT_RULES_VERSION"),
            "gate": {k: cfg.get(k) for k in ("P_MIN", "ODDS_MIN", "ODDS_MAX", "EV_MIN", "MIN_MINUTES_TO_KICKOFF",
                                             "MAX_HOURS_TO_KICKOFF", "MAX_LOCK_AGE_MIN", "FLAT_STAKE", "MARKETS")},
            "labels": {
                "positive_clv_share": "share with positive CLV against the closing reference",
                "ev": "model estimate",
                "units": "simulated units, no monetary value",
            },
        }
        return _cache_set("brain", _envelope(data, degraded=(engine_state != "RUNNING")))
    except Exception as e:  # noqa: BLE001
        return _envelope({"engine_state": "SCANNER_DOWN", "decision_state": "HOLD", "error": type(e).__name__},
                         degraded=True, source="error")


@router.get("/public/oraklion/brain/ledger.json")
async def public_oraklion_brain_ledger(since_seq: int = Query(0, ge=0), limit: int = Query(500, ge=1, le=500)):
    key = f"ledger:{since_seq}:{limit}"
    cached = _cache_get(key)
    if cached:
        return cached
    if _db_state is None or not getattr(_db_state, "pool", None):
        return _envelope({"events": []}, degraded=True, source="db_unavailable")
    try:
        async with _db_state.pool.acquire() as conn:
            if not await _schema_present(conn):
                return _cache_set(key, _envelope({"events": []}, degraded=True, source="schema_missing"))
            events = await fetch_ledger(conn, since_seq=since_seq, limit=limit)
            head = await chain_head(conn)
        return _cache_set(key, _envelope({
            "events": events,
            "chain_head": {"seq": int(head["seq"]), "hash": head["hash"]} if head else None,
            "verify": "sha256(prev_hash + canonical) == hash; genesis prev_hash = 64 zeros; see tools/verify_ledger.py",
        }))
    except Exception as e:  # noqa: BLE001
        return _envelope({"events": [], "error": type(e).__name__}, degraded=True, source="error")
