"""
Sniper Live — isolert CLV-tracking-pipeline for Over 2.5 mål (Big5).

Bygger på Force Edge Discovery v3-bevis (ROI +14.62% på edge≥10%, hit 62.4%).

Pipeline:
1. generate_picks (06:30 UTC, + catch-up 14:00 UTC)
   - Hent Big5 fixtures neste 2 dager via API-Football
   - Kjør Dixon-Coles predict (services/dixon_coles_engine.py)
   - Fetch Pinnacle Over 2.5-odds
   - Filter: edge >= 9%, odds 1.40-2.50, edge <= 30% (kill-switch quarantine)
   - Lagre til sniper_bets_v1 (UNIQUE pick_id+market = idempotent)
   - First-3-picks: send intern Telegram-alert til Don

2. update_odds_t60 (hver 5. min, 12:00-22:00 UTC)
   - For picks med kickoff i 55-65 min: snapshot odds → odds_t60 + clv_t60_pct

3. update_odds_close (hver 1. min, 12:00-22:00 UTC)
   - For picks med kickoff i 4-6 min: snapshot odds → odds_close + clv_close_pct
   - Setter is_positive_clv_close = (odds_close < odds_open)

4. settle_picks (hver 30. min, 14:00-23:30 UTC)
   - For picks 2t+ post-kickoff: hent score → result + profit_units

ALDRI:
- Push uten Don's eksplisitt godkjenning
- Mock data
- Soft-bookmakers (kun Pinnacle bookmaker_id=4)
- Lagre picks med edge > 30% (quarantine)
- Send til kunde-Telegram-kanal (kun Don's intern chat)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

logger = logging.getLogger("sesomnod.sniper")

# ── KONSTANTER ──────────────────────────────────────────────────────────────
EDGE_THRESHOLD = 0.09           # 9% — Force Edge Discovery v3 robust signal
EDGE_QUARANTINE = 0.30          # 30%+ = bug-signal, drop pick
ODDS_MIN = 1.40
ODDS_MAX = 2.50
PINNACLE_BOOKMAKER_ID = 4
MAX_PICKS_PER_DAY = 5
ALERT_FIRST_N_PICKS = 3

BIG5_LEAGUE_IDS = {
    39:  "Premier League",
    140: "La Liga",
    78:  "Bundesliga",
    135: "Serie A",
    61:  "Ligue 1",
}

API_FOOTBALL_BASE = "https://v3.football.api-sports.io"


# ── SCHEMA ──────────────────────────────────────────────────────────────────
SNIPER_SCHEMA = """
-- Schema er additive: ALTER med IF NOT EXISTS for backward compat.
-- Trygg å re-kjøre.

CREATE TABLE IF NOT EXISTS sniper_bets_v1 (
    id BIGSERIAL PRIMARY KEY,
    match_id TEXT NOT NULL,
    league TEXT NOT NULL,
    home_team TEXT NOT NULL,
    away_team TEXT NOT NULL,
    kickoff_time TIMESTAMPTZ NOT NULL,

    pick_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    market TEXT NOT NULL DEFAULT 'OVER_2_5',

    model_prob FLOAT NOT NULL,
    market_implied_prob FLOAT NOT NULL,
    edge_pct FLOAT NOT NULL,
    lambda_total FLOAT,

    odds_open FLOAT NOT NULL,
    odds_open_timestamp TIMESTAMPTZ NOT NULL,
    odds_open_source TEXT NOT NULL DEFAULT 'pinnacle',

    odds_t60 FLOAT,
    odds_t60_timestamp TIMESTAMPTZ,
    odds_close FLOAT,
    odds_close_timestamp TIMESTAMPTZ,

    clv_t60_pct FLOAT,
    clv_close_pct FLOAT,
    is_positive_clv_close BOOLEAN,

    home_goals INT,
    away_goals INT,
    total_goals INT,
    result TEXT,
    profit_units FLOAT,
    settled_at TIMESTAMPTZ,

    why_data JSONB,
    risk_factors JSONB,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(match_id, market)
);

CREATE INDEX IF NOT EXISTS idx_sniper_kickoff ON sniper_bets_v1(kickoff_time);
CREATE INDEX IF NOT EXISTS idx_sniper_result ON sniper_bets_v1(result);
CREATE INDEX IF NOT EXISTS idx_sniper_clv
    ON sniper_bets_v1(clv_close_pct)
    WHERE clv_close_pct IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_sniper_pick_ts
    ON sniper_bets_v1(pick_timestamp);

-- BUG 3 + observability: track snapshot retries, last settle, fixture status
ALTER TABLE sniper_bets_v1
    ADD COLUMN IF NOT EXISTS snapshot_failed_count INT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_settle_attempt_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS fixture_status TEXT;

CREATE TABLE IF NOT EXISTS sniper_market_intelligence (
    id BIGSERIAL PRIMARY KEY,
    match_id TEXT NOT NULL,
    league TEXT NOT NULL,
    market_name TEXT NOT NULL,
    pinnacle_available BOOLEAN NOT NULL,
    sample_values JSONB,
    observed_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(match_id, market_name)
);
CREATE INDEX IF NOT EXISTS idx_market_intel_match
    ON sniper_market_intelligence(match_id);
"""


# ── TEAM NORMALIZER (kopiert fra scripts/force_edge_discovery.py) ───────────
TEAM_NORMALIZER = {
    "Man United": "Manchester United", "Manchester Utd": "Manchester United",
    "Man City": "Manchester City",
    "Spurs": "Tottenham", "Tottenham Hotspur": "Tottenham",
    "Wolves": "Wolverhampton",
    "Nott'm Forest": "Nottingham Forest",
    "Brighton": "Brighton & Hove Albion", "Brighton & Hove": "Brighton & Hove Albion",
    "West Ham": "West Ham United",
    "Newcastle": "Newcastle United",
    "Leicester": "Leicester City",
    "Sheffield Utd": "Sheffield United",
    "Leeds": "Leeds United",
    "Atletico Madrid": "Atlético Madrid", "Atletico": "Atlético Madrid",
    "Sociedad": "Real Sociedad",
    "Athletic Bilbao": "Athletic Club",
    "Vallecano": "Rayo Vallecano",
    "Cadiz": "Cádiz",
    "Almeria": "Almería",
    "Bayern Munich": "Bayern München", "Bayern": "Bayern München",
    "Dortmund": "Borussia Dortmund", "BVB": "Borussia Dortmund",
    "Leverkusen": "Bayer Leverkusen",
    "M'gladbach": "Borussia Mönchengladbach",
    "Mönchengladbach": "Borussia Mönchengladbach",
    "Frankfurt": "Eintracht Frankfurt",
    "Hoffenheim": "TSG Hoffenheim",
    "Köln": "1. FC Köln",
    "Wolfsburg": "VfL Wolfsburg",
    "Milan": "AC Milan",
    "Inter": "Inter Milan", "Internazionale": "Inter Milan",
    "Roma": "AS Roma",
    "PSG": "Paris Saint-Germain", "Paris SG": "Paris Saint-Germain",
    "Marseille": "Olympique Marseille",
    "Lyon": "Olympique Lyonnais",
    "Monaco": "AS Monaco",
    "Lille": "Lille OSC",
    "Rennes": "Stade Rennais",
}


def _norm_team(name: str) -> str:
    return TEAM_NORMALIZER.get(name, name) if name else name


def _compute_season(date_obj=None) -> int:
    """Big5 crossing-sesonger bruker start-år (verifisert 2026-04-28)."""
    d = date_obj or datetime.now(timezone.utc).date()
    return d.year if d.month >= 8 else d.year - 1


# ── API-FOOTBALL HELPERS ────────────────────────────────────────────────────
def _api_headers() -> dict:
    api_key = os.environ.get("FOOTBALL_API_KEY", "")
    if not api_key:
        raise RuntimeError("FOOTBALL_API_KEY missing in env")
    return {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "v3.football.api-sports.io",
    }


async def fetch_big5_fixtures(date_str: str, season: int,
                               client: httpx.AsyncClient) -> list[dict]:
    """Hent fixtures for én dato på tvers av alle Big5-ligaer."""
    fixtures: list[dict] = []
    for league_id, league_name in BIG5_LEAGUE_IDS.items():
        try:
            r = await client.get(
                f"{API_FOOTBALL_BASE}/fixtures",
                headers=_api_headers(),
                params={"date": date_str, "league": league_id, "season": season},
            )
            data = r.json().get("response") or []
            for f in data:
                f["_league_name"] = league_name
                fixtures.append(f)
        except Exception as e:
            logger.warning("[Sniper] fixtures fetch %s %s: %s",
                           league_name, date_str, e)
    return fixtures


def _parse_pinnacle_over_25(odds_response: list[dict]) -> tuple[float | None, dict | None]:
    """
    Returner (odds, full_market_payload) eller (None, None).

    odds_response = response[].bookmakers[]. Her ser vi etter bookmaker.id=4
    og bet.name="Goals Over/Under" med values[].value="Over 2.5".
    """
    if not odds_response:
        return None, None
    bookmakers = (odds_response[0].get("bookmakers") or [])
    pinnacle = next((b for b in bookmakers
                     if b.get("id") == PINNACLE_BOOKMAKER_ID), None)
    if not pinnacle:
        return None, None
    for bet in (pinnacle.get("bets") or []):
        name = (bet.get("name") or "").lower()
        if "over/under" not in name and "goals over" not in name:
            continue
        for v in (bet.get("values") or []):
            v_label = (v.get("value") or "").lower().strip()
            if v_label == "over 2.5":
                try:
                    return float(v.get("odd")), bet
                except (TypeError, ValueError):
                    return None, None
    return None, None


def _parse_pinnacle_market_intel(odds_response: list[dict]) -> dict:
    """For sniper_market_intelligence — logg AH-availability uten bygging."""
    if not odds_response:
        return {}
    bookmakers = (odds_response[0].get("bookmakers") or [])
    pinnacle = next((b for b in bookmakers
                     if b.get("id") == PINNACLE_BOOKMAKER_ID), None)
    if not pinnacle:
        return {}
    out = {}
    for bet in (pinnacle.get("bets") or []):
        name = bet.get("name") or ""
        out[name] = {
            "values_count": len(bet.get("values") or []),
            "sample_values": (bet.get("values") or [])[:3],
        }
    return out


async def fetch_fixture_odds(fixture_id: int,
                              client: httpx.AsyncClient,
                              max_attempts: int = 3,
                              retry_delay_sec: float = 30.0) -> list[dict]:
    """
    Hent odds for én fixture (kun Pinnacle). BUG 2: retry-logikk.

    Inntil max_attempts forsøk. Mellom-forsøk: sleep retry_delay_sec.
    Returnerer tom liste hvis alle attempts feiler.
    """
    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            r = await client.get(
                f"{API_FOOTBALL_BASE}/odds",
                headers=_api_headers(),
                params={"fixture": fixture_id,
                        "bookmaker": PINNACLE_BOOKMAKER_ID},
            )
            data = r.json().get("response") or []
            if data:
                return data
            last_err = "empty response"
        except Exception as e:
            last_err = str(e)[:100]
        if attempt < max_attempts:
            logger.info("[Sniper] odds %s attempt %d/%d failed (%s), retry in %ds",
                        fixture_id, attempt, max_attempts, last_err, retry_delay_sec)
            await asyncio.sleep(retry_delay_sec)
    logger.warning("[Sniper] odds %s exhausted %d attempts: %s",
                   fixture_id, max_attempts, last_err)
    return []


async def fetch_fixture_score(fixture_id: int,
                               client: httpx.AsyncClient) -> tuple[int | None, int | None, str | None]:
    """Returner (home_goals, away_goals, status) eller (None, None, None)."""
    try:
        r = await client.get(
            f"{API_FOOTBALL_BASE}/fixtures",
            headers=_api_headers(),
            params={"id": fixture_id},
        )
        response = r.json().get("response") or []
        if not response:
            return None, None, None
        f = response[0]
        goals = f.get("goals") or {}
        status = ((f.get("fixture") or {}).get("status") or {}).get("short")
        return goals.get("home"), goals.get("away"), status
    except Exception as e:
        logger.warning("[Sniper] score fetch %s: %s", fixture_id, e)
        return None, None, None


# ── DON-ALERT (intern Telegram) ─────────────────────────────────────────────
async def _maybe_alert_first_picks(pool, payload: dict) -> bool:
    """Send intern Telegram-alert for picks #1-3. Graceful skip ved manglende env."""
    token = os.environ.get("TELEGRAM_TOKEN", "")
    intern_chat = (os.environ.get("DON_INTERNAL_TELEGRAM_CHAT_ID")
                   or os.environ.get("TELEGRAM_CHAT_ID", ""))

    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT COUNT(*) AS n FROM sniper_bets_v1")
    n_total = int(row["n"]) if row else 0

    if n_total > ALERT_FIRST_N_PICKS:
        return False
    if not token or not intern_chat:
        logger.info("[Sniper] Don-alert skipped: TELEGRAM_TOKEN or chat_id mangler")
        return False

    try:
        msg = (
            f"🔍 SNIPER PICK #{n_total}\n"
            f"{payload['home_team']} vs {payload['away_team']}\n"
            f"Liga: {payload['league']}\n"
            f"Modell Over 2.5: {payload['model_prob']*100:.1f}%\n"
            f"Marked implied: {payload['market_implied_prob']*100:.1f}%\n"
            f"Edge: +{payload['edge_pct']:.1f}%\n"
            f"Odds: {payload['odds_open']}\n"
            f"Kickoff: {payload['kickoff_time']}\n"
            f"[INTERN — verifiser før auto-mode]"
        )
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": intern_chat, "text": msg},
            )
        return True
    except Exception as e:
        logger.warning("[Sniper] Don-alert send failed: %s", e)
        return False


# ── PICK GENERATION ─────────────────────────────────────────────────────────
async def generate_picks(pool, days_ahead: int = 2) -> dict:
    """
    Hovedjobb. Itererer Big5-fixtures, predicter, henter Pinnacle-odds,
    filtrerer på edge ≥9% + odds-range, kvarantenerer edge >30%,
    lagrer til sniper_bets_v1.
    """
    from services.dixon_coles_engine import get_dixon_coles_probs

    today = datetime.now(timezone.utc).date()
    season = _compute_season(today)

    stats = {
        "fixtures_scanned": 0,
        "no_pinnacle_odds": 0,
        "outside_odds_range": 0,
        "model_predict_failed": 0,
        "low_edge": 0,
        "quarantined_high_edge": 0,
        "picks_created": 0,
        "ah_intel_logged": 0,
        "alerts_sent": 0,
    }

    async with httpx.AsyncClient(timeout=15.0) as client:
        for offset in range(days_ahead + 1):
            date_str = (today + timedelta(days=offset)).isoformat()
            fixtures = await fetch_big5_fixtures(date_str, season, client)

            for fix in fixtures:
                stats["fixtures_scanned"] += 1
                fix_id = (fix.get("fixture") or {}).get("id")
                if not fix_id:
                    continue

                kickoff_iso = (fix.get("fixture") or {}).get("date")
                teams = fix.get("teams") or {}
                home_raw = (teams.get("home") or {}).get("name") or ""
                away_raw = (teams.get("away") or {}).get("name") or ""
                home_norm = _norm_team(home_raw)
                away_norm = _norm_team(away_raw)
                league_name = fix.get("_league_name") or ""

                odds_response = await fetch_fixture_odds(fix_id, client)
                odds_open, market_payload = _parse_pinnacle_over_25(odds_response)

                # Logg AH-intel uavhengig av Over 2.5-availability
                intel = _parse_pinnacle_market_intel(odds_response)
                if intel:
                    async with pool.acquire() as conn:
                        for market_name, payload in intel.items():
                            try:
                                await conn.execute(
                                    """
                                    INSERT INTO sniper_market_intelligence
                                        (match_id, league, market_name,
                                         pinnacle_available, sample_values)
                                    VALUES ($1, $2, $3, $4, $5)
                                    ON CONFLICT (match_id, market_name)
                                    DO UPDATE SET
                                        pinnacle_available = EXCLUDED.pinnacle_available,
                                        sample_values = EXCLUDED.sample_values,
                                        observed_at = NOW();
                                    """,
                                    str(fix_id), league_name, market_name,
                                    True, json.dumps(payload),
                                )
                                stats["ah_intel_logged"] += 1
                            except Exception as e:
                                logger.warning("[Sniper] intel log fail: %s", e)

                if not odds_open:
                    stats["no_pinnacle_odds"] += 1
                    continue
                if not (ODDS_MIN <= odds_open <= ODDS_MAX):
                    stats["outside_odds_range"] += 1
                    continue

                try:
                    market_implied_home = 0.5  # placeholder for fallback-arg
                    dc_result = await get_dixon_coles_probs(
                        home_norm, away_norm, market_implied_home,
                    )
                    if dc_result.fallback_used:
                        stats["model_predict_failed"] += 1
                        continue
                    prob_over_25 = float(dc_result.over_25)
                    if prob_over_25 <= 0:
                        stats["model_predict_failed"] += 1
                        continue
                    lambda_total = float(dc_result.lambda_home) + float(dc_result.lambda_away)
                except Exception as e:
                    logger.warning("[Sniper] DC predict %s vs %s: %s",
                                   home_norm, away_norm, e)
                    stats["model_predict_failed"] += 1
                    continue

                implied = 1.0 / odds_open
                edge = prob_over_25 - implied

                if edge < EDGE_THRESHOLD:
                    stats["low_edge"] += 1
                    continue
                if edge > EDGE_QUARANTINE:
                    logger.warning(
                        "[Sniper] QUARANTINE edge=%.1f%% on %s vs %s — verifiser model + odds",
                        edge * 100, home_norm, away_norm,
                    )
                    stats["quarantined_high_edge"] += 1
                    continue

                payload = {
                    "match_id": str(fix_id),
                    "league": league_name,
                    "home_team": home_norm,
                    "away_team": away_norm,
                    "kickoff_time": kickoff_iso,
                    "model_prob": prob_over_25,
                    "market_implied_prob": implied,
                    "edge_pct": edge * 100,
                    "lambda_total": lambda_total,
                    "odds_open": odds_open,
                    "odds_open_timestamp": datetime.now(timezone.utc).isoformat(),
                }

                async with pool.acquire() as conn:
                    # BUG 4 fix: ON CONFLICT DO NOTHING (ikke DO UPDATE) garanterer
                    # at catchup-jobs IKKE overskriver odds_open når picken allerede
                    # er logget av primary 06:30-jobben. Original-pris bevares.
                    inserted = await conn.fetchrow(
                        """
                        INSERT INTO sniper_bets_v1
                            (match_id, league, home_team, away_team, kickoff_time,
                             market, model_prob, market_implied_prob, edge_pct,
                             lambda_total,
                             odds_open, odds_open_timestamp, odds_open_source,
                             result)
                        VALUES ($1, $2, $3, $4, $5,
                                'OVER_2_5', $6, $7, $8,
                                $9,
                                $10, $11, 'pinnacle',
                                'PENDING')
                        ON CONFLICT (match_id, market) DO NOTHING
                        RETURNING id;
                        """,
                        payload["match_id"], payload["league"],
                        payload["home_team"], payload["away_team"],
                        payload["kickoff_time"],
                        payload["model_prob"], payload["market_implied_prob"],
                        payload["edge_pct"], payload["lambda_total"],
                        payload["odds_open"], payload["odds_open_timestamp"],
                    )
                if inserted:
                    stats["picks_created"] += 1
                    if await _maybe_alert_first_picks(pool, payload):
                        stats["alerts_sent"] += 1
                    if stats["picks_created"] >= MAX_PICKS_PER_DAY:
                        logger.info("[Sniper] MAX_PICKS_PER_DAY reached")
                        return stats

    return stats


# ── ODDS-SNAPSHOTS ──────────────────────────────────────────────────────────
async def update_odds_t60(pool) -> dict:
    """
    BUG 1: utvidet vindu 50-70 min (var 55-65). Tåler scheduler-jitter.
    BUG 2: retry via fetch_fixture_odds.
    Tracker snapshot_failed_count for observability.
    """
    now = datetime.now(timezone.utc)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, match_id, odds_open, kickoff_time, snapshot_failed_count
            FROM sniper_bets_v1
            WHERE odds_t60 IS NULL
              AND kickoff_time > NOW() + INTERVAL '50 minutes'
              AND kickoff_time < NOW() + INTERVAL '70 minutes';
            """
        )
    updates = 0
    failures = 0
    async with httpx.AsyncClient(timeout=15.0) as client:
        for row in rows:
            odds_response = await fetch_fixture_odds(int(row["match_id"]), client)
            new_odds, _ = _parse_pinnacle_over_25(odds_response)
            if not new_odds:
                failures += 1
                async with pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE sniper_bets_v1 SET snapshot_failed_count = "
                        "COALESCE(snapshot_failed_count, 0) + 1, updated_at = NOW() "
                        "WHERE id = $1;",
                        row["id"],
                    )
                continue
            clv = ((float(row["odds_open"]) - new_odds) / float(row["odds_open"])) * 100
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE sniper_bets_v1 SET
                        odds_t60 = $1, odds_t60_timestamp = $2,
                        clv_t60_pct = $3, updated_at = NOW()
                    WHERE id = $4;
                    """,
                    new_odds, now.isoformat(), clv, row["id"],
                )
            updates += 1
    return {"checked": len(rows), "updated": updates, "failures": failures}


async def update_odds_close(pool) -> dict:
    """
    BUG 1: utvidet vindu 3-7 min (var 4-6). Bedre fangst av T-5-tidspunkt.
    BUG 2: retry via fetch_fixture_odds.
    """
    now = datetime.now(timezone.utc)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, match_id, odds_open, kickoff_time, snapshot_failed_count
            FROM sniper_bets_v1
            WHERE odds_close IS NULL
              AND kickoff_time > NOW() + INTERVAL '3 minutes'
              AND kickoff_time < NOW() + INTERVAL '7 minutes';
            """
        )
    updates = 0
    failures = 0
    async with httpx.AsyncClient(timeout=15.0) as client:
        for row in rows:
            odds_response = await fetch_fixture_odds(int(row["match_id"]), client)
            new_odds, _ = _parse_pinnacle_over_25(odds_response)
            if not new_odds:
                failures += 1
                async with pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE sniper_bets_v1 SET snapshot_failed_count = "
                        "COALESCE(snapshot_failed_count, 0) + 1, updated_at = NOW() "
                        "WHERE id = $1;",
                        row["id"],
                    )
                continue
            clv = ((float(row["odds_open"]) - new_odds) / float(row["odds_open"])) * 100
            is_positive = new_odds < float(row["odds_open"])
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE sniper_bets_v1 SET
                        odds_close = $1, odds_close_timestamp = $2,
                        clv_close_pct = $3, is_positive_clv_close = $4,
                        updated_at = NOW()
                    WHERE id = $5;
                    """,
                    new_odds, now.isoformat(), clv, is_positive, row["id"],
                )
            updates += 1
    return {"checked": len(rows), "updated": updates, "failures": failures}


# ── SETTLEMENT ──────────────────────────────────────────────────────────────
async def settle_picks(pool) -> dict:
    """
    BUG 3: settlement med fixture_status-håndtering.

    API-Football short status:
      FT/AET/PEN  → ferdig spilt → WIN (≥3 mål) eller LOSS (<3)
      CANC/ABD/AWD → avlyst/abandonert → VOID, profit = 0
      PST          → utsatt → forbli PENDING, oppdater last_settle_attempt_at
      NS/1H/HT/2H/LIVE/etc → ikke ferdig → forbli PENDING

    Lagrer fixture_status uansett for observability.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, match_id, odds_open, kickoff_time
            FROM sniper_bets_v1
            WHERE result = 'PENDING'
              AND kickoff_time < NOW() - INTERVAL '2 hours';
            """
        )
    settled = 0
    voided = 0
    pending_kept = 0
    async with httpx.AsyncClient(timeout=15.0) as client:
        for row in rows:
            home_g, away_g, status = await fetch_fixture_score(
                int(row["match_id"]), client,
            )
            now_iso = datetime.now(timezone.utc).isoformat()

            # Always update last_settle_attempt_at + fixture_status
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE sniper_bets_v1 SET
                        fixture_status = $1, last_settle_attempt_at = $2,
                        updated_at = NOW()
                    WHERE id = $3;
                    """,
                    status, now_iso, row["id"],
                )

            # Avlyst / abandonert / awarded → VOID med profit=0
            if status in ("CANC", "ABD", "AWD"):
                async with pool.acquire() as conn:
                    await conn.execute(
                        """
                        UPDATE sniper_bets_v1 SET
                            result = 'VOID', profit_units = 0.0,
                            settled_at = NOW(), updated_at = NOW()
                        WHERE id = $1;
                        """,
                        row["id"],
                    )
                voided += 1
                continue

            # Ferdig spilt → WIN/LOSS
            if status in ("FT", "AET", "PEN") and home_g is not None and away_g is not None:
                total = home_g + away_g
                if total >= 3:
                    result = "WIN"
                    profit = float(row["odds_open"]) - 1.0
                else:
                    result = "LOSS"
                    profit = -1.0
                async with pool.acquire() as conn:
                    await conn.execute(
                        """
                        UPDATE sniper_bets_v1 SET
                            home_goals = $1, away_goals = $2, total_goals = $3,
                            result = $4, profit_units = $5, settled_at = NOW(),
                            updated_at = NOW()
                        WHERE id = $6;
                        """,
                        home_g, away_g, total, result, profit, row["id"],
                    )
                settled += 1
                continue

            # Ikke ferdig (NS, 1H, HT, 2H, LIVE, PST, etc.) → forbli PENDING
            pending_kept += 1
    return {
        "checked": len(rows),
        "settled": settled,
        "voided": voided,
        "kept_pending": pending_kept,
    }


# ── KILL-SWITCHES (3-trinns CLV-validation) ─────────────────────────────────
async def check_kill_switches(pool) -> dict:
    """Etter 5+ settled close-picks: vurder kill-switch-status."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                COUNT(*) FILTER (WHERE odds_close IS NOT NULL) AS n_with_close,
                COUNT(*) FILTER (WHERE is_positive_clv_close) AS n_positive_clv,
                COUNT(*) FILTER (WHERE result IN ('WIN', 'LOSS')) AS n_settled,
                SUM(profit_units) FILTER (WHERE result IN ('WIN', 'LOSS')) AS total_profit,
                AVG(clv_close_pct) FILTER (WHERE clv_close_pct IS NOT NULL) AS avg_clv
            FROM sniper_bets_v1;
            """
        )
    n_close = int(row["n_with_close"] or 0)
    n_positive = int(row["n_positive_clv"] or 0)
    n_settled = int(row["n_settled"] or 0)
    total_profit = float(row["total_profit"] or 0)
    avg_clv = float(row["avg_clv"]) if row["avg_clv"] is not None else None
    roi = (total_profit / n_settled * 100) if n_settled > 0 else 0.0
    pos_clv_pct = (n_positive / n_close * 100) if n_close > 0 else 0.0

    flags = []
    if n_close >= 5 and n_positive == 0:
        flags.append("KILL_SWITCH_NO_POSITIVE_CLV (0/5+ close-snapshots)")
    if n_settled >= 30 and pos_clv_pct < 50.0:
        flags.append("EARLY_WARNING_CLV_BELOW_50PCT")
    if n_settled >= 50 and roi < -3.0:
        flags.append("CRITICAL_ROI_BELOW_NEG_3PCT")
    if n_settled >= 100 and (roi < 0 or pos_clv_pct < 50.0):
        flags.append("HARD_STOP_REQUIRED")

    if n_settled >= 100 and pos_clv_pct >= 55.0 and roi >= 3.0:
        flags.append("EDGE_LIVE_VERIFIED")

    return {
        "n_with_close": n_close,
        "n_positive_clv_close": n_positive,
        "positive_clv_pct": round(pos_clv_pct, 1),
        "n_settled": n_settled,
        "total_profit_units": round(total_profit, 2),
        "roi_pct": round(roi, 2),
        "avg_clv_close_pct": round(avg_clv, 2) if avg_clv is not None else None,
        "flags": flags,
        "auto_stop_required": any(f in ("KILL_SWITCH_NO_POSITIVE_CLV (0/5+ close-snapshots)",
                                         "CRITICAL_ROI_BELOW_NEG_3PCT",
                                         "HARD_STOP_REQUIRED") for f in flags),
    }


# ── DASHBOARD ───────────────────────────────────────────────────────────────
async def build_sniper_dashboard(pool, window_days: int = 30) -> dict:
    """Aggregat over sniper_bets_v1 for dashboard."""
    async with pool.acquire() as conn:
        summary = await conn.fetchrow(
            """
            SELECT
                COUNT(*) AS picks_total,
                COUNT(*) FILTER (WHERE result IN ('WIN','LOSS')) AS picks_settled,
                COUNT(*) FILTER (WHERE odds_close IS NOT NULL) AS picks_with_close,
                COUNT(*) FILTER (WHERE result = 'WIN') AS wins,
                COUNT(*) FILTER (WHERE result = 'LOSS') AS losses,
                COUNT(*) FILTER (WHERE is_positive_clv_close) AS positive_clv,
                AVG(clv_close_pct) FILTER (WHERE clv_close_pct IS NOT NULL) AS avg_clv,
                SUM(profit_units) FILTER (WHERE result IN ('WIN','LOSS')) AS total_profit,
                AVG(edge_pct) AS avg_edge_pct
            FROM sniper_bets_v1
            WHERE pick_timestamp > NOW() - ($1 || ' days')::interval;
            """,
            str(window_days),
        )
        by_league = await conn.fetch(
            """
            SELECT league, COUNT(*) AS n,
                   COUNT(*) FILTER (WHERE result = 'WIN') AS wins,
                   AVG(edge_pct) AS avg_edge,
                   AVG(clv_close_pct) AS avg_clv
            FROM sniper_bets_v1
            WHERE pick_timestamp > NOW() - ($1 || ' days')::interval
            GROUP BY league
            ORDER BY n DESC;
            """,
            str(window_days),
        )
        recent = await conn.fetch(
            """
            SELECT id, match_id, league, home_team, away_team, kickoff_time,
                   model_prob, market_implied_prob, edge_pct,
                   odds_open, odds_t60, odds_close,
                   clv_close_pct, is_positive_clv_close,
                   result, profit_units
            FROM sniper_bets_v1
            ORDER BY pick_timestamp DESC
            LIMIT 10;
            """
        )

    s = dict(summary) if summary else {}
    settled = int(s.get("picks_settled") or 0)
    wins = int(s.get("wins") or 0)
    pclose = int(s.get("picks_with_close") or 0)
    pos = int(s.get("positive_clv") or 0)
    profit = float(s.get("total_profit") or 0)

    summary_out = {
        "picks_total": int(s.get("picks_total") or 0),
        "picks_settled": settled,
        "picks_with_close": pclose,
        "wins": wins,
        "losses": int(s.get("losses") or 0),
        "win_rate_pct": round(wins / settled * 100, 1) if settled > 0 else 0,
        "roi_pct": round(profit / settled * 100, 2) if settled > 0 else 0,
        "total_profit_units": round(profit, 2),
        "positive_clv_close": pos,
        "positive_clv_pct": round(pos / pclose * 100, 1) if pclose > 0 else 0,
        "avg_clv_close_pct": round(float(s["avg_clv"]), 2) if s.get("avg_clv") is not None else None,
        "avg_edge_pct": round(float(s["avg_edge_pct"]), 2) if s.get("avg_edge_pct") is not None else None,
    }

    kill = await check_kill_switches(pool)

    return {
        "computed_at": datetime.now(timezone.utc).isoformat(),
        "window_days": window_days,
        "summary": summary_out,
        "by_league": [dict(r) for r in by_league],
        "recent_picks": [dict(r) for r in recent],
        "kill_switch_status": kill,
    }
