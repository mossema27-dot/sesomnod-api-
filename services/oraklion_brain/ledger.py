"""
Én skrivevei til oraklion.brain_events (brev §5.1, §5.4).

append_event() MÅ kalles inne i en transaksjon (conn.transaction()). Den tar
advisory-lås, leser siste seq/hash, bygger event_public, hasher og setter inn.
Dermed er seq tett (max+1) og kjeden lineær. Rollback av transaksjonen gir
ikke hull fordi seq beregnes under låsen, ikke fra en sekvens.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Optional

from .canonical import GENESIS_PREV_HASH, build_event_public, canonical_json, compute_hash

LOCK_KEY = "oraklion_brain_ledger"


async def take_ledger_lock(conn) -> None:
    await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1));", LOCK_KEY)


async def append_event(
    conn,
    *,
    event_type: str,
    ts_utc: datetime,
    payload: dict,
    decision_id: Optional[uuid.UUID] = None,
    campaign_id: Optional[uuid.UUID] = None,
    meta_private: Optional[dict] = None,
) -> dict:
    """Returnerer {seq, event_id, hash, prev_hash, canonical}."""
    await take_ledger_lock(conn)
    row = await conn.fetchrow(
        "SELECT seq, hash FROM oraklion.brain_events ORDER BY seq DESC LIMIT 1;"
    )
    seq = (int(row["seq"]) + 1) if row else 1
    prev_hash = row["hash"] if row else GENESIS_PREV_HASH
    event_id = uuid.uuid4()
    public = build_event_public(
        event_type=event_type, event_id=event_id, seq=seq, ts_utc=ts_utc,
        payload=payload, decision_id=decision_id, campaign_id=campaign_id,
        meta_private=meta_private,
    )
    canonical = canonical_json(public)
    h = compute_hash(prev_hash, canonical)
    await conn.execute(
        """
        INSERT INTO oraklion.brain_events
            (seq, event_id, event_type, ts_utc, decision_id, campaign_id,
             schema_version, canonical, prev_hash, hash, meta_private)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb);
        """,
        seq, event_id, event_type, ts_utc, decision_id, campaign_id,
        public["schema_version"], canonical, prev_hash, h,
        json.dumps(meta_private, default=str) if meta_private else None,
    )
    return {"seq": seq, "event_id": event_id, "hash": h, "prev_hash": prev_hash, "canonical": canonical}


async def verify_chain(conn) -> list[dict]:
    rows = await conn.fetch("SELECT seq, problem FROM oraklion.brain_verify_chain();")
    return [dict(r) for r in rows]


async def chain_head(conn) -> Optional[dict]:
    row = await conn.fetchrow(
        "SELECT seq, hash, ts_utc FROM oraklion.brain_events ORDER BY seq DESC LIMIT 1;"
    )
    return dict(row) if row else None


async def has_event(conn, event_type: str) -> bool:
    return bool(await conn.fetchval(
        "SELECT 1 FROM oraklion.brain_events WHERE event_type = $1 LIMIT 1;", event_type
    ))


async def fetch_ledger(conn, since_seq: int = 0, limit: int = 500) -> list[dict]:
    """Offentlige felt serveres FRA canonical (brev §5.4, T20)."""
    if since_seq > 0:
        rows = await conn.fetch(
            "SELECT seq, canonical, prev_hash, hash FROM oraklion.brain_events "
            "WHERE seq > $1 ORDER BY seq ASC LIMIT $2;", since_seq, limit)
    else:
        rows = await conn.fetch(
            "SELECT seq, canonical, prev_hash, hash FROM oraklion.brain_events "
            "ORDER BY seq DESC LIMIT $1;", limit)
        rows = list(reversed(rows))
    out = []
    for r in rows:
        ev = json.loads(r["canonical"])
        ev["prev_hash"] = r["prev_hash"]
        ev["hash"] = r["hash"]
        ev["canonical"] = r["canonical"]
        out.append(ev)
    return out
