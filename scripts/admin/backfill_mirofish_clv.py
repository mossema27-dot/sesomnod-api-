#!/usr/bin/env python3
"""
Backfill: POST historiske settled picks til MiroFish /clv.

Strategi:
  1. GET alle picks fra MiroFish /clv der result == PENDING og closing_odds IS NOT NULL
  2. For hvert pick: hent result fra picks_v2 (match via pick_id-rekonstruksjon
     eller match_name + kickoff-dato fallback)
  3. POST {pick_id, pinnacle_closing_odds, result} til MiroFish /clv
  4. Idempotent: 409 = allerede sendt, skip

MiroFish ClvSubmission-spec (v2.3):
  - pick_id:             str
  - pinnacle_closing_odds: float (>1.0)
  - result:             "WIN" | "LOSS" | "PUSH"

Lokalt: krever DATABASE_URL i environment.
Railway shell: DATABASE_URL er pre-satt.
"""
from __future__ import annotations

import asyncio
import asyncpg
import httpx
import os
import sys
from datetime import datetime, timezone
from typing import Optional

MIROFISH_BASE = os.getenv(
    "MIROFISH_BASE_URL",
    "https://mirofish-service-production.up.railway.app"
)
MIROFISH_KEY = os.getenv("MIROFISH_INTERNAL_KEY", "")


def _get_database_url() -> str:
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        print("FEIL: DATABASE_URL er ikke satt i environment.")
        print("Kjor via Railway shell der DATABASE_URL er pre-satt,")
        print("eller: export DATABASE_URL=<din-url> og kjor pa nytt.")
        sys.exit(1)
    # Aldri print URL
    return url


def _normalize_result(raw: str) -> str | None:
    """Konverter picks_v2.result til MiroFish WIN/LOSS/PUSH-format."""
    if raw is None:
        return None
    val = str(raw).upper().strip()
    if val in ("WIN", "W"):
        return "WIN"
    if val in ("LOSS", "LOSE", "L"):
        return "LOSS"
    if val in ("PUSH", "VOID", "P"):
        return "PUSH"
    return None  # DRAW, UNKNOWN, etc. er ikke submittable


async def backfill():
    database_url = _get_database_url()

    # Step 1: Hent alle PENDING picks med closing_odds fra MiroFish
    print(f"Henter picks fra MiroFish /clv ...")
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get(f"{MIROFISH_BASE}/clv")
        if r.status_code != 200:
            print(f"FEIL: MiroFish /clv returnerte HTTP {r.status_code}")
            sys.exit(1)
        all_picks = r.json()

    pending_with_closing = [
        p for p in all_picks
        if p.get("result") == "PENDING" and p.get("closing_odds") is not None
    ]
    print(f"Totalt i MiroFish: {len(all_picks)} | PENDING med closing_odds: {len(pending_with_closing)}")

    if not pending_with_closing:
        print("Ingen picks a backfille.")
        return

    # Step 2: Koble til DB og hent result fra picks_v2
    conn = await asyncpg.connect(database_url)
    try:
        # Hent alle settled picks_v2-rader (result IS NOT NULL, har hjemme/borte-lag)
        db_picks = await conn.fetch("""
            SELECT
                id,
                match_name,
                home_team,
                away_team,
                kickoff_time,
                result,
                odds
            FROM picks_v2
            WHERE result IS NOT NULL
              AND result NOT IN ('', 'PENDING', 'NO_BET')
        """)
        print(f"picks_v2 settled rader: {len(db_picks)}")
    finally:
        await conn.close()

    # Bygg oppslag: pick_id-slug -> result
    def _make_slug(row: asyncpg.Record) -> str | None:
        home = str(row.get("home_team") or "").lower().replace(" ", "-").strip()
        away = str(row.get("away_team") or "").lower().replace(" ", "-").strip()
        if not home or not away:
            return None
        kt = row.get("kickoff_time")
        if kt is None:
            return None
        if hasattr(kt, "strftime"):
            if kt.tzinfo is None:
                kt = kt.replace(tzinfo=timezone.utc)
            date_str = kt.strftime("%Y%m%d")
        else:
            try:
                dt = datetime.fromisoformat(str(kt).replace("Z", "+00:00"))
                date_str = dt.strftime("%Y%m%d")
            except Exception:
                return None
        return f"{home}-{away}-{date_str}"

    # pick_id prefix -> (result, odds)
    db_lookup: dict[str, tuple[str, float | None]] = {}
    for row in db_picks:
        prefix = _make_slug(row)
        if prefix:
            db_lookup[prefix] = (str(row["result"]), row.get("odds"))

    # Step 3: POST resultater
    success = failed = skipped = no_result = 0
    headers = {"X-Internal-Key": MIROFISH_KEY} if MIROFISH_KEY else {}

    async with httpx.AsyncClient(timeout=10.0) as client:
        for pick in pending_with_closing:
            pick_id: str = pick["pick_id"]
            closing_odds: float = float(pick["closing_odds"])

            # Finn result fra DB: pick_id er {home}-{away}-{YYYYMMDD}-{market}
            # prefix = alt f.o.m. start til siste bindestrek-segment
            parts = pick_id.rsplit("-", 1)
            prefix = parts[0] if len(parts) == 2 else pick_id

            db_entry = db_lookup.get(prefix)
            if db_entry is None:
                print(f"  ? {pick_id} | ikke funnet i picks_v2 (prefix={prefix})")
                no_result += 1
                continue

            raw_result, _ = db_entry
            mf_result = _normalize_result(raw_result)
            if mf_result is None:
                print(f"  ~ {pick_id} | ikke-submittable result: {raw_result}")
                no_result += 1
                continue

            payload = {
                "pick_id": pick_id,
                "pinnacle_closing_odds": closing_odds,
                "result": mf_result,
            }

            try:
                r = await client.post(
                    f"{MIROFISH_BASE}/clv",
                    json=payload,
                    headers=headers,
                )
                if r.status_code in (200, 201):
                    clv_back = None
                    try:
                        clv_back = r.json().get("clv_pct")
                    except Exception:
                        pass
                    print(f"  OK {pick_id} | {mf_result} | closing={closing_odds} | clv={clv_back}")
                    success += 1
                elif r.status_code == 409:
                    print(f"  -> {pick_id} | allerede registrert (409), skip")
                    skipped += 1
                else:
                    print(f"  FEIL {pick_id} | HTTP {r.status_code}: {r.text[:100]}")
                    failed += 1
            except Exception as e:
                print(f"  FEIL {pick_id} | {e}")
                failed += 1

            await asyncio.sleep(0.15)

    print(f"\nResultat: OK={success} Skip={skipped} Feil={failed} Ikke-funnet={no_result} Totalt={len(pending_with_closing)}")

    # Step 4: Verifiser Phase 1 gate
    print("\nHenter Phase 1 gate status ...")
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            gr = await client.get(f"{MIROFISH_BASE}/phase1-gate")
            if gr.status_code == 200:
                d = gr.json()
                print(f"  settled_picks: {d.get('settled_picks')}")
                gate = d.get("phase1_gate", {})
                print(f"  all_gates_passed: {gate.get('all_gates_passed')}")
                print(f"  avg_clv: {d.get('avg_clv')}")
                print(f"  hit_rate: {d.get('hit_rate_pct')}")
            else:
                print(f"  phase1-gate HTTP {gr.status_code}")
        except Exception as e:
            print(f"  phase1-gate feil: {e}")


if __name__ == "__main__":
    asyncio.run(backfill())
