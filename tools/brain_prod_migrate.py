"""
Oraklion Brain v2 — PROD migrering/preflight/status via asyncpg (ingen psql nødvendig).

DSN hentes fra miljø, i denne rekkefølgen: BRAIN_PROD_DSN, DATABASE_PUBLIC_URL, DATABASE_URL.
DSN skrives ALDRI ut. Kjør typisk via Railway CLI slik at variablene injiseres:

  railway run -s Postgres -- .brain_demo_venv/bin/python tools/brain_prod_migrate.py --preflight
  railway run -s Postgres -- .brain_demo_venv/bin/python tools/brain_prod_migrate.py --up
  railway run -s Postgres -- .brain_demo_venv/bin/python tools/brain_prod_migrate.py --status

--preflight : kun lesing. Kolonnesjekk, kandidatvindu, oppgjørsfelter, duplikater, eksisterende oraklion-objekter.
--up        : kjører migrations/2026-09-15_brain_v2_up.sql (idempotent, kun additiv) og verifiserer 6 tabeller + tom kjede.
--status    : kun lesing. Heartbeat, ticks, beslutninger, hendelser, duplikat-/kjedesjekk, Sniper-helse.
--down      : NEKTES uten --i-understand-ledger-loss (fjerner brain_* og alle ledger-hendelser).
"""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
UP = ROOT / "migrations" / "2026-09-15_brain_v2_up.sql"
DOWN = ROOT / "migrations" / "2026-09-15_brain_v2_down.sql"
PREFLIGHT_SQL = ROOT / "scripts" / "brain_prod_preflight.sql"
EXPECTED_TABLES = {"brain_events", "brain_decisions", "brain_config", "brain_heartbeat", "brain_ticks", "brain_stats_snapshot"}


def dsn() -> str:
    for k in ("BRAIN_PROD_DSN", "DATABASE_PUBLIC_URL", "DATABASE_URL"):
        v = os.environ.get(k)
        if v:
            if "railway.internal" in v:
                continue  # privat nett — ikke nåbar fra Mac; prøv neste
            print(f"[prod] DSN fra {k} (host skjult)")
            return v
    raise SystemExit("NO_SOURCE_ACCESS: ingen DSN i miljøet (BRAIN_PROD_DSN / DATABASE_PUBLIC_URL / DATABASE_URL). "
                     "Kjør via `railway run -s Postgres -- ...` eller sett BRAIN_PROD_DSN.")


def show(title: str, rows) -> None:
    print(f"\n== {title} ==")
    if not rows:
        print("(0 rader)")
        return
    for r in rows:
        print("  " + " | ".join(f"{k}={v}" for k, v in dict(r).items()))


async def safe_show(conn, title: str, sql: str):
    """Lese-sjekk som aldri stopper kjøringen: feil vises som tekst (f.eks. manglende tabell)."""
    try:
        rows = await conn.fetch(sql)
    except Exception as e:  # noqa: BLE001
        print(f"\n== {title} ==\n  FEIL: {type(e).__name__}: {e}")
        return None
    show(title, rows)
    return rows


async def preflight(conn) -> int:
    problems = 0
    missing = await conn.fetch(PREFLIGHT_SQL.read_text())
    show("A. Manglende kolonner i sniper_bets_v1 (FORVENTET 0)", missing)
    problems += len(missing)
    await safe_show(conn, "B. Eksisterende oraklion-objekter",
        "SELECT table_name FROM information_schema.tables WHERE table_schema='oraklion' ORDER BY 1;")
    await safe_show(conn, "C. Kandidatvindu nå (PENDING, avspark 0–48 t frem)", """
        SELECT count(*) AS pending_48h,
               count(*) FILTER (WHERE now() - odds_open_timestamp <= interval '90 min') AS lock_age_ok,
               count(*) FILTER (WHERE model_prob >= 0.55 AND odds_open BETWEEN 1.5 AND 2.6
                                  AND model_prob*odds_open - 1 >= 0.05) AS passes_gate_math,
               count(*) FILTER (WHERE COALESCE(market_tier,'PRIMARY')='PRIMARY') AS primary_tier,
               count(*) FILTER (WHERE market='OVER_2_5') AS market_over25,
               min(kickoff_time) AS first_kickoff, max(pick_timestamp) AS last_pick
        FROM sniper_bets_v1 WHERE result='PENDING' AND kickoff_time > now() AND kickoff_time <= now() + interval '48 hours';""")
    await safe_show(conn, "D. Oppgjørsfelter siste 30 d", """
        SELECT count(*) AS settled, count(*) FILTER (WHERE settled_at IS NOT NULL) AS with_settled_at,
               count(*) FILTER (WHERE odds_close IS NOT NULL) AS with_close,
               count(*) FILTER (WHERE odds_close_timestamp IS NOT NULL AND odds_close_timestamp < kickoff_time) AS close_pre_kickoff,
               count(*) FILTER (WHERE total_goals IS NOT NULL) AS with_goals,
               string_agg(DISTINCT COALESCE(clv_source,'NULL'), ',') AS clv_sources,
               string_agg(DISTINCT COALESCE(fixture_status,'NULL'), ',') AS fixture_statuses
        FROM sniper_bets_v1 WHERE result IN ('WIN','LOSS','VOID') AND kickoff_time >= now() - interval '30 days';""")
    await safe_show(conn, "E. Duplikater (match_id, market) (FORVENTET 0)",
        "SELECT match_id, market, count(*) AS n FROM sniper_bets_v1 GROUP BY 1,2 HAVING count(*)>1 LIMIT 5;")
    await safe_show(conn, "F. Siste 5 PENDING (identitet/odds-tid)", """
        SELECT id, match_id, league, home_team, away_team, kickoff_time, model_prob, odds_open,
               odds_open_timestamp, COALESCE(market_tier,'PRIMARY') AS tier
        FROM sniper_bets_v1 WHERE result='PENDING' ORDER BY kickoff_time ASC LIMIT 5;""")
    await safe_show(conn, "G. Sniper-helse",
        "SELECT max(pick_timestamp) AS last_pick, count(*) FILTER (WHERE pick_timestamp >= now() - interval '24 hours') AS picks_24h,"
        " (SELECT value FROM system_state WHERE key='sniper_pick_gen_paused') AS paused FROM sniper_bets_v1;")
    print(f"\nPREFLIGHT {'OK' if problems == 0 else 'FEIL: ' + str(problems) + ' manglende kolonne(r) — STOPP'}")
    return problems


async def up(conn) -> int:
    before = {r["table_name"] for r in await conn.fetch(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='oraklion' AND table_name LIKE 'brain_%';")}
    print(f"[prod] brain_-tabeller før: {sorted(before) or 'ingen'}")
    async with conn.transaction():
        await conn.execute(UP.read_text())
    after = {r["table_name"] for r in await conn.fetch(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='oraklion' AND table_name LIKE 'brain_%';")}
    problems = await conn.fetch("SELECT * FROM oraklion.brain_verify_chain();")
    cfg = await conn.fetch("SELECT key, value FROM oraklion.brain_config ORDER BY key;")
    print(f"[prod] brain_-tabeller etter: {sorted(after)}")
    print(f"[prod] brain_verify_chain(): {len(problems)} rader (FORVENTET 0)")
    print(f"[prod] brain_config: {len(cfg)} nøkler")
    ok = after >= EXPECTED_TABLES and len(problems) == 0
    print(f"MIGRATION {'OK' if ok else 'FEIL'}")
    return 0 if ok else 1


async def status(conn) -> int:
    await safe_show(conn, "Heartbeat", "SELECT * FROM oraklion.brain_heartbeat;")
    await safe_show(conn, "Siste 5 ticks", "SELECT tick_utc, reason, scanned_n, passed_n, skip_counts FROM oraklion.brain_ticks ORDER BY tick_utc DESC LIMIT 5;")
    await safe_show(conn, "Beslutninger per status", "SELECT status, count(*) FROM oraklion.brain_decisions GROUP BY 1 ORDER BY 1;")
    await safe_show(conn, "Hendelser per type", "SELECT event_type, count(*) FROM oraklion.brain_events GROUP BY 1 ORDER BY 1;")
    dup = await conn.fetch("SELECT fixture_id, count(*) FROM oraklion.brain_decisions GROUP BY 1 HAVING count(*)>1;")
    show("Duplikate beslutninger per fixture (FORVENTET 0)", dup)
    open_n = await conn.fetchval("SELECT count(*) FROM oraklion.brain_decisions WHERE status='OPEN';")
    problems = await conn.fetch("SELECT * FROM oraklion.brain_verify_chain();")
    await safe_show(conn, "Sniper-helse",
        "SELECT max(pick_timestamp) AS last_pick, count(*) FILTER (WHERE pick_timestamp >= now() - interval '24 hours') AS picks_24h FROM sniper_bets_v1;")
    print(f"\nOPEN={open_n} (maks 1) · kjedeproblemer={len(problems)} (FORVENTET 0) · duplikater={len(dup)} (FORVENTET 0)")
    ok = open_n <= 1 and not problems and not dup
    print(f"STATUS {'OK' if ok else 'FEIL'}")
    return 0 if ok else 1


async def main() -> int:
    import asyncpg
    args = sys.argv[1:]
    if not args or args[0] not in ("--preflight", "--up", "--status", "--down"):
        print(__doc__); return 2
    if args[0] == "--down" and "--i-understand-ledger-loss" not in args:
        print("NEKTET: --down fjerner alle ledger-hendelser. Legg til --i-understand-ledger-loss (kun etter Don's ord)."); return 2
    conn = await asyncpg.connect(dsn(), timeout=20)
    try:
        if args[0] == "--preflight":
            return await preflight(conn)
        if args[0] == "--up":
            return await up(conn)
        if args[0] == "--status":
            return await status(conn)
        async with conn.transaction():
            await conn.execute(DOWN.read_text())
        print("DOWN utført."); return 0
    finally:
        await conn.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
