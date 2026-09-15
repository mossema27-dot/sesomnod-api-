#!/usr/bin/env python3
"""
verify_ledger.py — re-hash Oraklion Brain-ledgeren fra det offentlige endepunktet.

Bruk:
  python3 tools/verify_ledger.py                       # henter fra sesomnod-api (prod)
  python3 tools/verify_ledger.py --base http://localhost:8000
  python3 tools/verify_ledger.py --file ledger.json    # lokal fil med samme format

Kun standardbibliotek. Skriver OK eller BRUDD med seq og problem, exit 0/1.
Regel: hash == sha256(prev_hash + canonical); genesis prev_hash = 64 nuller; seq tett.
"""
import argparse
import hashlib
import json
import sys
import urllib.request

GENESIS = "0" * 64
PUBLIC_KEYS = {"event_id", "event_type", "seq", "ts_utc", "decision_id",
               "campaign_id", "schema_version", "payload", "meta_private_hash"}


def fetch_all(base: str) -> list:
    events, since = [], 0
    while True:
        url = f"{base}/public/oraklion/brain/ledger.json?since_seq={since}&limit=500"
        with urllib.request.urlopen(url, timeout=20) as r:
            env = json.load(r)
        batch = env.get("data", {}).get("events", [])
        if not batch:
            return events
        events.extend(batch)
        since = batch[-1]["seq"]
        if len(batch) < 500:
            return events


def verify(events: list) -> list:
    problems, prev_hash, prev_seq = [], None, None
    for e in sorted(events, key=lambda x: int(x["seq"])):
        seq = int(e["seq"])
        canon = e["canonical"]
        if prev_seq is None:
            if e["prev_hash"] != GENESIS:
                problems.append((seq, "genesis_prev_not_zero"))
        else:
            if seq != prev_seq + 1:
                problems.append((seq, "seq_gap"))
            if e["prev_hash"] != prev_hash:
                problems.append((seq, "prev_hash_mismatch"))
        if hashlib.sha256((e["prev_hash"] + canon).encode("utf-8")).hexdigest() != e["hash"]:
            problems.append((seq, "hash_mismatch"))
        try:
            parsed = json.loads(canon)
            if set(parsed) != PUBLIC_KEYS or parsed["seq"] != seq:
                problems.append((seq, "event_public_shape"))
            served = {k: e.get(k) for k in PUBLIC_KEYS}
            if served != parsed:
                problems.append((seq, "served_fields_differ_from_canonical"))
        except Exception:
            problems.append((seq, "canonical_not_json"))
        prev_hash, prev_seq = e["hash"], seq
    return problems


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="https://sesomnod-api-production.up.railway.app")
    ap.add_argument("--file")
    a = ap.parse_args()
    if a.file:
        with open(a.file, encoding="utf-8") as f:
            data = json.load(f)
        events = data.get("data", data).get("events", data if isinstance(data, list) else [])
    else:
        events = fetch_all(a.base)
    problems = verify(events)
    if not events:
        print("TOM: ingen hendelser i ledgeren")
        return 0
    if problems:
        print(f"BRUDD: {len(problems)} problem(er) over {len(events)} hendelser")
        for seq, p in problems:
            print(f"  seq={seq} {p}")
        return 1
    print(f"OK: {len(events)} hendelser, kjede intakt, head seq={events[-1]['seq']} hash={events[-1]['hash'][:16]}…")
    return 0


if __name__ == "__main__":
    sys.exit(main())
