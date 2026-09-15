"""
Brain-tick: skann → commit → oppgjør → snapshot → heartbeat (brev §3, §5).

Kilde for alt: sniper_bets_v1 (lesing). Brain gjør INGEN eksterne API-kall.
- Kandidat = sniper-rad med result='PENDING', tier i CANDIDATE_TIERS.
- Closing = sniper.odds_close (Pinnacle, fanget av Sniper-jobben før avspark).
- Resultat = sniper.result etter stabilitetsregel (§5.2).

Kjøres kun når BRAIN_V2_JOBS=on (main.py). Idempotent per tick.
"""
from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from . import METHODOLOGY_VERSION
from .canonical import q
from .ledger import append_event, has_event, take_ledger_lock
from .rules import (
    SNIPER_MARKET_TO_KEY, BrainConfig, Candidate, check_transition,
    lock_age_minutes, select, void_deadline,
)
from .stats import brier_binary, clv_odds_pct, compute_flat

logger = logging.getLogger("oraklion_brain")

JOB_NAME = "oraklion_brain_tick"
SOURCE_TABLE = "sniper_bets_v1"
TERMINAL_VOID_STATUSES = ("CANC", "WO", "AWD")
WAIT_STATUSES = ("ABD", "SUSP", "INT")
MODEL_VERSION = "sniper_live@" + (os.environ.get("RAILWAY_GIT_COMMIT_SHA") or "local")[:12]


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ── config / heartbeat ──────────────────────────────────────────

async def load_config(conn) -> BrainConfig:
    rows = await conn.fetch("SELECT key, value FROM oraklion.brain_config;")
    return BrainConfig.from_rows({r["key"]: r["value"] for r in rows})


async def heartbeat_start(pool, cfg: BrainConfig, now: datetime) -> None:
    async with pool.acquire() as conn:  # egen autocommit-transaksjon (§5.6)
        await conn.execute(
            """
            INSERT INTO oraklion.brain_heartbeat (job_name, expected_interval_sec, last_start_utc, api_day)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (job_name) DO UPDATE SET
                expected_interval_sec = EXCLUDED.expected_interval_sec,
                last_start_utc = EXCLUDED.last_start_utc,
                api_calls_today = CASE WHEN oraklion.brain_heartbeat.api_day = EXCLUDED.api_day
                                       THEN oraklion.brain_heartbeat.api_calls_today ELSE 0 END,
                api_day = EXCLUDED.api_day;
            """,
            JOB_NAME, cfg.tick_interval_sec, now, now.date(),
        )


async def heartbeat_finish(pool, now: datetime, error: Optional[str], source_fetch_utc: Optional[datetime]) -> None:
    async with pool.acquire() as conn:
        if error is None:
            await conn.execute(
                "UPDATE oraklion.brain_heartbeat SET last_ok_utc=$2, last_error=NULL, "
                "last_source_fetch_utc=COALESCE($3, last_source_fetch_utc) WHERE job_name=$1;",
                JOB_NAME, now, source_fetch_utc)
        else:
            await conn.execute(
                "UPDATE oraklion.brain_heartbeat SET last_error=$2 WHERE job_name=$1;",
                JOB_NAME, error[:500])


async def source_last_fetch(conn) -> Optional[datetime]:
    """Siste Sniper-skann (sniper_scan_log) — fallback siste pick_timestamp."""
    if await conn.fetchval("SELECT to_regclass('public.sniper_scan_log') IS NOT NULL;"):
        ts = await conn.fetchval("SELECT max(scanned_at) FROM sniper_scan_log;")
        if ts:
            return ts
    return await conn.fetchval("SELECT max(pick_timestamp) FROM sniper_bets_v1;")


# ── epoke ───────────────────────────────────────────────────────

async def ensure_epoch(pool, cfg: BrainConfig, now: datetime) -> None:
    async with pool.acquire() as conn:
        async with conn.transaction():
            await take_ledger_lock(conn)
            if await has_event(conn, "EPOCH_START"):
                return
            await append_event(conn, event_type="EPOCH_START", ts_utc=now, payload={
                "methodology_version": cfg.methodology_version,
                "selection_rule_version": cfg.selection_rule_version,
                "settlement_rules_version": cfg.settlement_rules_version,
                "devig_method": "multiplicative",
                "candidate_source": SOURCE_TABLE,
                "markets": list(cfg.markets),
                "flat_stake": cfg.flat_stake,
                "p_min": q(cfg.p_min, 6), "odds_min": q(cfg.odds_min, 3), "odds_max": q(cfg.odds_max, 3),
                "ev_min": q(cfg.ev_min, 6), "max_lock_age_min": cfg.max_lock_age_min,
                "min_minutes_to_kickoff": cfg.min_minutes_to_kickoff,
                "max_hours_to_kickoff": cfg.max_hours_to_kickoff,
            })


# ── skann + commit ──────────────────────────────────────────────

async def fetch_candidates(conn, cfg: BrainConfig, now: datetime) -> list[Candidate]:
    rows = await conn.fetch(
        """
        SELECT s.id, s.match_id, s.league, s.home_team, s.away_team, s.kickoff_time,
               s.market, s.model_prob, s.odds_open, s.odds_open_timestamp,
               COALESCE(s.market_tier, 'PRIMARY') AS market_tier
        FROM sniper_bets_v1 s
        WHERE s.result = 'PENDING'
          AND s.kickoff_time > $1
          AND s.kickoff_time <= $1 + make_interval(hours => $2)
          AND NOT EXISTS (
              SELECT 1 FROM oraklion.brain_decisions d
              WHERE d.fixture_id = s.match_id::text
          )
        ORDER BY s.kickoff_time ASC;
        """,
        now, int(cfg.max_hours_to_kickoff),
    )
    out: list[Candidate] = []
    for r in rows:
        out.append(Candidate(
            source_table=SOURCE_TABLE, source_id=int(r["id"]), fixture_id=str(r["match_id"]),
            league=r["league"] or "", home=r["home_team"], away=r["away_team"],
            kickoff_utc=r["kickoff_time"], market_key=SNIPER_MARKET_TO_KEY.get(r["market"], r["market"]),
            p_model=float(r["model_prob"]), odds=float(r["odds_open"]),
            odds_ts_utc=r["odds_open_timestamp"], odds_ts_basis="column" if r["odds_open_timestamp"] else "unknown",
            tier=r["market_tier"], model_version=MODEL_VERSION,
        ))
    return out


async def open_decision_exists(conn) -> bool:
    return bool(await conn.fetchval("SELECT 1 FROM oraklion.brain_decisions WHERE status='OPEN' LIMIT 1;"))


async def record_tick(conn, now: datetime, reason: Optional[str], scanned_n: int, passed_n: int,
                      skip_counts: dict, decision_id: Optional[uuid.UUID]) -> None:
    await conn.execute(
        "INSERT INTO oraklion.brain_ticks (tick_utc, reason, scanned_n, passed_n, skip_counts, decision_id) "
        "VALUES ($1, $2, $3, $4, $5::jsonb, $6);",
        now, reason, scanned_n, passed_n, json.dumps(skip_counts), decision_id)


async def maybe_hold_event(conn, now: datetime, reason: str, scanned_n: int, passed_n: int, skip_counts: dict) -> None:
    """HOLD-hendelse i ledgeren maks én per UTC-dag per grunn (ticks logges alltid i brain_ticks)."""
    last = await conn.fetchrow(
        "SELECT ts_utc, canonical FROM oraklion.brain_events WHERE event_type='HOLD' ORDER BY seq DESC LIMIT 1;")
    if last and last["ts_utc"].date() == now.date():
        try:
            if json.loads(last["canonical"])["payload"].get("reason") == reason:
                return
        except Exception:
            pass
    await append_event(conn, event_type="HOLD", ts_utc=now, payload={
        "reason": reason, "scanned_n": scanned_n, "passed_n": passed_n, "skip_counts": skip_counts})


async def scan_and_commit(pool, cfg: BrainConfig, now: datetime, source_stale: bool) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            await take_ledger_lock(conn)
            if await open_decision_exists(conn):
                await record_tick(conn, now, "OPEN_DECISION_EXISTS", 0, 0, {}, None)
                return {"hold": "OPEN_DECISION_EXISTS"}
            if source_stale:
                await record_tick(conn, now, "SOURCE_STALE", 0, 0, {}, None)
                await maybe_hold_event(conn, now, "SOURCE_STALE", 0, 0, {})
                return {"hold": "SOURCE_STALE"}
            cands = await fetch_candidates(conn, cfg, now)
            if not cands:
                await record_tick(conn, now, "NO_MODEL_ROWS", 0, 0, {}, None)
                await maybe_hold_event(conn, now, "NO_MODEL_ROWS", 0, 0, {})
                return {"hold": "NO_MODEL_ROWS"}
            sel = select(cands, now, cfg)
            if sel.chosen is None:
                reason = "ODDS_TS_UNVERIFIED" if sel.skip_counts.get("ODDS_TS_UNVERIFIED") == sel.scanned_n else "NO_CANDIDATES"
                await record_tick(conn, now, reason, sel.scanned_n, 0, sel.skip_counts, None)
                await maybe_hold_event(conn, now, reason, sel.scanned_n, 0, sel.skip_counts)
                return {"hold": reason, "scanned_n": sel.scanned_n, "skip_counts": sel.skip_counts}

            c = sel.chosen
            decision_id = uuid.uuid4()
            lock_age = lock_age_minutes(now, c.odds_ts_utc)
            idem = __import__("hashlib").sha256(
                f"{c.fixture_id}|{c.market_key}|{cfg.selection_rule_version}|{c.source_table}:{c.source_id}".encode()
            ).hexdigest()
            existing = await conn.fetchval(
                "SELECT decision_id FROM oraklion.brain_decisions WHERE idempotency_key=$1;", idem)
            if existing:
                await record_tick(conn, now, "OPEN_DECISION_EXISTS", sel.scanned_n, sel.passed_n, sel.skip_counts, existing)
                return {"hold": "DEDUP", "decision_id": str(existing)}

            payload = {
                "fixture_id": c.fixture_id, "league": c.league, "home": c.home, "away": c.away,
                "kickoff_utc": c.kickoff_utc, "market_key": c.market_key,
                "model_version": c.model_version, "p_model": q(c.p_model, 6),
                "odds_locked": q(c.odds, 3), "odds_ts_utc": c.odds_ts_utc, "odds_ts_basis": c.odds_ts_basis,
                "lock_age_min": lock_age, "selection_rule_version": cfg.selection_rule_version,
                "ev": q(c.ev, 6), "committed_utc": now, "record_type": "FORWARD",
                "scanned_n": sel.scanned_n, "passed_n": sel.passed_n,
            }
            meta_private = {"source_table": c.source_table, "source_id": c.source_id,
                            "odds_source": "pinnacle_via_api_football", "tier": c.tier}
            ev = await append_event(conn, event_type="COMMIT", ts_utc=now, payload=payload,
                                    decision_id=decision_id, meta_private=meta_private)
            await conn.execute(
                """
                INSERT INTO oraklion.brain_decisions
                    (decision_id, seq, record_type, status, source_table, source_id, fixture_id, league,
                     home, away, kickoff_utc, market_key, p_model, odds_locked, odds_ts_utc, odds_ts_basis,
                     lock_age_min, ev, model_version, selection_rule_version, idempotency_key, committed_utc)
                VALUES ($1,$2,'FORWARD','OPEN',$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20);
                """,
                decision_id, ev["seq"], c.source_table, c.source_id, c.fixture_id, c.league, c.home, c.away,
                c.kickoff_utc, c.market_key, c.p_model, c.odds, c.odds_ts_utc, c.odds_ts_basis, lock_age,
                c.ev, c.model_version, cfg.selection_rule_version, idem, now,
            )
            await record_tick(conn, now, None, sel.scanned_n, sel.passed_n, sel.skip_counts, decision_id)
            logger.info("[Brain] COMMIT seq=%s decision=%s %s v %s %s @ %s",
                        ev["seq"], decision_id, c.home, c.away, c.market_key, c.odds)
            return {"commit": str(decision_id), "seq": ev["seq"], "hash": ev["hash"]}


# ── oppgjør ─────────────────────────────────────────────────────

async def _transition(conn, d: dict, new_status: str, now: datetime, event_type: str, payload: dict,
                      extra_sql: str = "", extra_args: tuple = ()) -> None:
    check_transition(d["status"], new_status)
    await append_event(conn, event_type=event_type, ts_utc=now, payload=payload, decision_id=d["decision_id"])
    await conn.execute(
        f"UPDATE oraklion.brain_decisions SET status=$2, updated_utc=$3 {extra_sql} WHERE decision_id=$1;",
        d["decision_id"], new_status, now, *extra_args)


async def settle_open(pool, cfg: BrainConfig, now: datetime) -> dict:
    out = {"settled": 0, "void": 0, "suspended": 0, "waiting": 0}
    async with pool.acquire() as conn:
        decisions = await conn.fetch(
            "SELECT * FROM oraklion.brain_decisions WHERE status IN ('OPEN','SUSPENDED') ORDER BY seq;")
        for drow in decisions:
            d = dict(drow)
            s = await conn.fetchrow(
                """
                SELECT result, settled_at, fixture_status, kickoff_time, home_goals, away_goals, total_goals,
                       odds_close, odds_close_timestamp, close_capture_minutes_before, clv_source
                FROM sniper_bets_v1 WHERE id = $1;
                """, d["source_id"])
            if s is None:
                continue
            async with conn.transaction():
                await take_ledger_lock(conn)
                status = (s["fixture_status"] or "").upper()
                result = (s["result"] or "PENDING").upper()

                # PST → SUSPENDED (går aldri tilbake til OPEN)
                if d["status"] == "OPEN" and status == "PST":
                    await _transition(conn, d, "SUSPENDED", now, "DECISION_SUSPENDED",
                                      {"fixture_status": status, "original_kickoff_utc": d["kickoff_utc"]})
                    out["suspended"] += 1
                    continue
                if status in TERMINAL_VOID_STATUSES or (
                        result == "PENDING" and now > void_deadline(d["kickoff_utc"])):
                    reason = status if status in TERMINAL_VOID_STATUSES else "NOT_COMPLETED_51H"
                    await _transition(conn, d, "VOID", now, "REVEAL",
                                      {"outcome": "VOID", "reason": reason,
                                       "settlement_rules_version": cfg.settlement_rules_version, "settled_utc": now},
                                      ", outcome='VOID', settled_utc=$3, settlement_rules_version=$4",
                                      (cfg.settlement_rules_version,))
                    out["void"] += 1
                    continue
                if result not in ("WIN", "LOSS", "VOID") or s["settled_at"] is None:
                    out["waiting"] += 1
                    continue

                # Stabilitet: kildens settled_at >= SETTLE_STABLE_MIN gammel OG samme resultat observert
                # i en tidligere tick >= 15 min siden (to lesninger).
                stable_since = now - timedelta(minutes=cfg.settle_stable_min)
                prev_obs_ok = (d.get("observed_result") == result and d.get("observed_utc") is not None
                               and d["observed_utc"] <= now - timedelta(minutes=15))
                if s["settled_at"] > stable_since or not prev_obs_ok:
                    await conn.execute(
                        "UPDATE oraklion.brain_decisions SET observed_result=$2, "
                        "observed_utc=COALESCE(CASE WHEN observed_result=$2 THEN observed_utc END, $3) "
                        "WHERE decision_id=$1;", d["decision_id"], result, now)
                    out["waiting"] += 1
                    continue

                if result == "VOID":
                    await _transition(conn, d, "VOID", now, "REVEAL",
                                      {"outcome": "VOID", "reason": "SOURCE_VOID",
                                       "settlement_rules_version": cfg.settlement_rules_version, "settled_utc": now},
                                      ", outcome='VOID', settled_utc=$3, settlement_rules_version=$4",
                                      (cfg.settlement_rules_version,))
                    out["void"] += 1
                    continue

                # Closing-referanse fra kildens capture (kun gyldig hvis fanget før avspark)
                odds_close = None
                close_ts = None
                close_min = None
                missing = None
                if d["status"] == "SUSPENDED":
                    missing = "RESCHEDULED"
                elif s["odds_close"] is None:
                    missing = "NO_FETCH_IN_WINDOW"
                elif (s["clv_source"] or "") == "POST_KICKOFF_INVALID" or (
                        s["odds_close_timestamp"] and s["odds_close_timestamp"] >= d["kickoff_utc"]):
                    missing = "POST_KICKOFF_CAPTURE"
                else:
                    odds_close = float(s["odds_close"])
                    close_ts = s["odds_close_timestamp"]
                    close_min = s["close_capture_minutes_before"]
                y = 1 if result == "WIN" else 0
                clv_o = clv_odds_pct(float(d["odds_locked"]), odds_close) if odds_close else None
                brier_m = brier_binary(float(d["p_model"]), y)
                payload = {
                    "outcome": result, "settled_utc": now, "settlement_rules_version": cfg.settlement_rules_version,
                    "score": {"home": s["home_goals"], "away": s["away_goals"]},
                    "odds_close": q(odds_close, 3) if odds_close else None,
                    "close_ts_utc": close_ts, "close_minutes_before": close_min,
                    "closing_missing_reason": missing,
                    "clv_odds_pct": q(clv_o, 4) if clv_o is not None else None,
                    "clv_fair_pct": None,   # p_fair krever alle utfall ved close; kilden lagrer ikke Under-odds (M2)
                    "brier_model": q(brier_m, 6), "brier_market": None,
                }
                await _transition(
                    conn, d, "SETTLED", now, "REVEAL", payload,
                    ", outcome=$4, settled_utc=$3, settlement_rules_version=$5, odds_close=$6, close_ts_utc=$7, "
                    "close_minutes_before=$8, closing_missing_reason=$9, clv_odds_pct=$10, brier_model=$11",
                    (result, cfg.settlement_rules_version, odds_close, close_ts, close_min, missing, clv_o, brier_m),
                )
                out["settled"] += 1
    return out


# ── snapshot ────────────────────────────────────────────────────

async def write_snapshot(pool, cfg: BrainConfig, now: datetime) -> dict:
    async with pool.acquire() as conn:
        epoch_seq = await conn.fetchval(
            "SELECT seq FROM oraklion.brain_events WHERE event_type='EPOCH_START' ORDER BY seq LIMIT 1;") or 0
        rows = await conn.fetch(
            "SELECT * FROM oraklion.brain_decisions WHERE record_type='FORWARD' AND seq >= $1;", epoch_seq)
        flat = compute_flat([dict(r) for r in rows], cfg.flat_stake, now)
        payload = json.loads(json.dumps(flat, default=str))
        await conn.execute(
            "INSERT INTO oraklion.brain_stats_snapshot (methodology_version, computed_utc, payload) VALUES ($1,$2,$3::jsonb);",
            cfg.methodology_version, now, json.dumps(payload))
        return payload


# ── tick ────────────────────────────────────────────────────────

async def tick(pool, now: Optional[datetime] = None) -> dict:
    now = now or _now()
    async with pool.acquire() as conn:
        cfg = await load_config(conn)
        src = await source_last_fetch(conn)
    await heartbeat_start(pool, cfg, now)
    result: dict = {"now": now.isoformat()}
    try:
        source_stale = (src is None) or ((now - src).total_seconds() > cfg.source_sla_sec)
        await ensure_epoch(pool, cfg, now)
        result["settle"] = await settle_open(pool, cfg, now)
        result["scan"] = await scan_and_commit(pool, cfg, now, source_stale)
        if result["settle"]["settled"] or result["settle"]["void"] or result["scan"].get("commit"):
            await write_snapshot(pool, cfg, now)
        await heartbeat_finish(pool, now, None, src)
    except Exception as e:  # noqa: BLE001
        logger.error("[Brain] tick failed: %s", e, exc_info=True)
        await heartbeat_finish(pool, now, f"{type(e).__name__}: {e}", src)
        result["error"] = str(e)
    return result


async def brain_tick_job(db_state) -> None:
    """Wrapper for APScheduler (samme mønster som _sniper_*_job i main.py)."""
    if not getattr(db_state, "connected", False) or not getattr(db_state, "pool", None):
        return
    try:
        r = await tick(db_state.pool)
        logger.info("[Brain] tick: %s", r)
    except Exception as e:  # noqa: BLE001
        logger.error("[Brain] tick job failed: %s", e, exc_info=True)
