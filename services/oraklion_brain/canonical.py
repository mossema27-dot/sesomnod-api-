"""
Kanonisk offentlig hendelsesformat og hash-kjeding (brev §5.4).

Regler:
- Det som hashes er ÉN struktur: event_public (se build_event_public).
- Serialisering: json.dumps(sort_keys=True, separators=(",",":"), ensure_ascii=False).
- Ingen flyttall i JSON. Tall med desimaler leveres som strenger med fast presisjon
  via q(). Heltall er tillatt som int.
- Tidsstempler: ISO-8601 med 'Z', sekundpresisjon.
- hash = sha256(prev_hash_hex + canonical) over UTF-8. Genesis prev_hash = 64 x '0'.
- Interne metadata hashes ikke direkte, men bindes via meta_private_hash.

Modulen har ingen tredjeparts-avhengigheter og ingen I/O.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_EVEN
from typing import Any, Optional

GENESIS_PREV_HASH = "0" * 64
SCHEMA_VERSION = "v2"

# Fast presisjon per feltnavn (brev §5.4). Ukjente desimalfelt → 6.
PRECISION: dict[str, int] = {
    "p_model": 6,
    "p_fair_close": 6,
    "ev": 6,
    "odds_locked": 3,
    "odds_close": 3,
    "clv_odds_pct": 4,
    "clv_fair_pct": 4,
    "brier_model": 6,
    "brier_market": 6,
    "roi": 6,
}


def q(value: Any, places: int) -> str:
    """Kvantiser til streng med fast antall desimaler (bankers rounding)."""
    d = Decimal(str(value))
    quant = Decimal(1).scaleb(-places)
    return str(d.quantize(quant, rounding=ROUND_HALF_EVEN))


def iso_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        raise ValueError("naive datetime not allowed in ledger")
    return dt.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _normalize(obj: Any, key: Optional[str] = None) -> Any:
    """Gjør payload JSON-kanonisk: floats/Decimal → streng, datetime → ISO Z, uuid → str."""
    if isinstance(obj, bool) or obj is None or isinstance(obj, (int, str)):
        return obj
    if isinstance(obj, (float, Decimal)):
        places = PRECISION.get(key or "", 6)
        return q(obj, places)
    if isinstance(obj, datetime):
        return iso_z(obj)
    if isinstance(obj, uuid.UUID):
        return str(obj)
    if isinstance(obj, dict):
        return {str(k): _normalize(v, str(k)) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_normalize(v, key) for v in obj]
    raise TypeError(f"unsupported type in canonical payload: {type(obj).__name__}")


def canonical_json(obj: Any) -> str:
    return json.dumps(_normalize(obj), sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def compute_hash(prev_hash: str, canonical: str) -> str:
    if len(prev_hash) != 64:
        raise ValueError("prev_hash must be 64 hex chars")
    return sha256_hex(prev_hash + canonical)


def meta_private_hash(meta_private: Optional[dict]) -> Optional[str]:
    if not meta_private:
        return None
    return sha256_hex(canonical_json(meta_private))


def build_event_public(
    *,
    event_type: str,
    event_id: uuid.UUID,
    seq: int,
    ts_utc: datetime,
    payload: dict,
    decision_id: Optional[uuid.UUID] = None,
    campaign_id: Optional[uuid.UUID] = None,
    meta_private: Optional[dict] = None,
) -> dict:
    """Den ene strukturen som hashes. Nøkkelsettet er lukket (brev §5.4)."""
    return {
        "event_id": str(event_id),
        "event_type": event_type,
        "seq": int(seq),
        "ts_utc": iso_z(ts_utc),
        "decision_id": str(decision_id) if decision_id else None,
        "campaign_id": str(campaign_id) if campaign_id else None,
        "schema_version": SCHEMA_VERSION,
        "payload": payload,
        "meta_private_hash": meta_private_hash(meta_private),
    }


EVENT_PUBLIC_KEYS = frozenset({
    "event_id", "event_type", "seq", "ts_utc", "decision_id",
    "campaign_id", "schema_version", "payload", "meta_private_hash",
})


def verify_rows(rows: list[dict]) -> list[dict]:
    """
    Ren verifisering av en liste rader {seq, prev_hash, hash, canonical}
    i seq-rekkefølge. Returnerer liste av {seq, problem}. Tom liste = OK.
    Brukes av tools/verify_ledger.py og av tester; speiler SQL-funksjonen.
    """
    problems: list[dict] = []
    prev_hash = None
    prev_seq = None
    for r in sorted(rows, key=lambda x: int(x["seq"])):
        seq = int(r["seq"])
        if prev_seq is None:
            if r["prev_hash"] != GENESIS_PREV_HASH:
                problems.append({"seq": seq, "problem": "genesis_prev_not_zero"})
        else:
            if seq != prev_seq + 1:
                problems.append({"seq": seq, "problem": "seq_gap"})
            if r["prev_hash"] != prev_hash:
                problems.append({"seq": seq, "problem": "prev_hash_mismatch"})
        if compute_hash(r["prev_hash"], r["canonical"]) != r["hash"]:
            problems.append({"seq": seq, "problem": "hash_mismatch"})
        try:
            parsed = json.loads(r["canonical"])
            if set(parsed.keys()) != EVENT_PUBLIC_KEYS or parsed.get("seq") != seq:
                problems.append({"seq": seq, "problem": "event_public_shape"})
        except Exception:
            problems.append({"seq": seq, "problem": "canonical_not_json"})
        prev_hash = r["hash"]
        prev_seq = seq
    return problems
