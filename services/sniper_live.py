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

# ── SHADOW MODE ─────────────────────────────────────────────────────────────
# PRIMARY = BIG5_LEAGUE_IDS, edge ≥ EDGE_THRESHOLD (9%) — locked + calibrated.
# SHADOW_BIG5 = Big5 men 5%-9% edge — calibrated, lower threshold, observer.
# SHADOW_GLOBAL = Top-15 ligaer utenfor Big5, edge ≥9% — UNCALIBRATED, research.
PRIMARY_LEAGUES = BIG5_LEAGUE_IDS
EDGE_THRESHOLD_SHADOW_BIG5 = 0.05  # 5%

# Liga-IDer hentet fra API-Football. Overlapp med PRIMARY filtreres bort.
SHADOW_GLOBAL_LEAGUES = {
    2:   "Champions League",
    3:   "Europa League",
    88:  "Eredivisie",
    94:  "Primeira Liga",
    79:  "Bundesliga 2",
    136: "Serie B",
    253: "MLS",
    71:  "Brasiliansk Serie A",
    128: "Argentinsk Primera",
    262: "Liga MX",
    40:  "Championship",
    113: "Allsvenskan",
    103: "Eliteserien",
}

SHADOW_DAILY_CAP = 50
SHADOW_TIERS = ("SHADOW_BIG5", "SHADOW_GLOBAL")

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

-- CLV-armor: late-capture flags (KODE 2) + market availability (KODE 3)
ALTER TABLE sniper_bets_v1
    ADD COLUMN IF NOT EXISTS t60_late_capture BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS close_late_capture BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS market_open_at_t60 BOOLEAN,
    ADD COLUMN IF NOT EXISTS market_open_at_close BOOLEAN,
    ADD COLUMN IF NOT EXISTS pinnacle_markets_at_close JSONB;

-- DEL 1: capture-timing drift tracking
ALTER TABLE sniper_bets_v1
    ADD COLUMN IF NOT EXISTS t60_capture_minutes_before INT,
    ADD COLUMN IF NOT EXISTS close_capture_minutes_before INT;

-- SHADOW MODE: tier-isolering (PRIMARY urørt, SHADOW for research)
ALTER TABLE sniper_bets_v1
    ADD COLUMN IF NOT EXISTS market_tier   TEXT DEFAULT 'PRIMARY',
    ADD COLUMN IF NOT EXISTS is_calibrated BOOLEAN DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS shadow_reason TEXT;
CREATE INDEX IF NOT EXISTS idx_sniper_market_tier
    ON sniper_bets_v1(market_tier);

CREATE TABLE IF NOT EXISTS shadow_team_mismatches (
    id BIGSERIAL PRIMARY KEY,
    detected_at TIMESTAMPTZ DEFAULT NOW(),
    league TEXT,
    home_team_raw TEXT,
    away_team_raw TEXT,
    home_team_normalized TEXT,
    away_team_normalized TEXT,
    teams_in_model BOOLEAN,
    fixture_id TEXT,
    UNIQUE(fixture_id)
);
CREATE INDEX IF NOT EXISTS idx_shadow_mismatch_detected
    ON shadow_team_mismatches(detected_at);

-- PROFIT-MASKIN: KODE 1 — CLV-decisions (PROPOSALS, ikke auto-execute)
CREATE TABLE IF NOT EXISTS clv_decisions (
    id BIGSERIAL PRIMARY KEY,
    decision_timestamp TIMESTAMPTZ DEFAULT NOW(),
    primary_decision TEXT,
    shadow_big5_decision TEXT,
    shadow_global_decisions JSONB,
    metadata JSONB
);
CREATE INDEX IF NOT EXISTS idx_clv_decisions_ts
    ON clv_decisions(decision_timestamp DESC);

-- PROFIT-MASKIN: KODE 3 — expected ROI (CLV-proxy for ROI)
ALTER TABLE sniper_bets_v1
    ADD COLUMN IF NOT EXISTS expected_roi_pct FLOAT;

-- PROFIT-MASKIN: KODE 5 (deferred) — Grok-context flagg
ALTER TABLE sniper_bets_v1
    ADD COLUMN IF NOT EXISTS grok_context_pending BOOLEAN DEFAULT TRUE;

-- PROFIT-MASKIN: KODE 6 — morgen-rapport (Don-lesbar)
CREATE TABLE IF NOT EXISTS don_morning_reports (
    id BIGSERIAL PRIMARY KEY,
    report_date DATE UNIQUE,
    generated_at TIMESTAMPTZ DEFAULT NOW(),
    report_markdown TEXT,
    summary_json JSONB
);

-- PROFIT-MASKIN: KODE 2 — CLV breakdown by tier × edge_bucket × odds_bucket
CREATE OR REPLACE VIEW sniper_clv_breakdown AS
SELECT
    market_tier, league,
    CASE
        WHEN edge_pct < 7  THEN '5-7%'
        WHEN edge_pct < 9  THEN '7-9%'
        WHEN edge_pct < 12 THEN '9-12%'
        ELSE                    '12%+'
    END AS edge_bucket,
    CASE
        WHEN odds_open < 1.6 THEN '1.40-1.60'
        WHEN odds_open < 1.9 THEN '1.60-1.90'
        WHEN odds_open < 2.2 THEN '1.90-2.20'
        ELSE                      '2.20-2.50'
    END AS odds_bucket,
    COUNT(*) FILTER (WHERE odds_close IS NOT NULL) AS n_with_close,
    ROUND(
        AVG(clv_close_pct) FILTER (WHERE odds_close IS NOT NULL)::numeric,
        2
    ) AS avg_clv,
    ROUND(
        COUNT(*) FILTER (WHERE is_positive_clv_close = TRUE) * 100.0
        / NULLIF(COUNT(*) FILTER (WHERE odds_close IS NOT NULL), 0)::numeric,
        1
    ) AS pct_positive_clv,
    ROUND(
        AVG(expected_roi_pct) FILTER (WHERE expected_roi_pct IS NOT NULL)::numeric,
        2
    ) AS avg_expected_roi_pct,
    COUNT(*) FILTER (WHERE result = 'WIN')                  AS wins,
    COUNT(*) FILTER (WHERE result IN ('WIN', 'LOSS'))       AS settled,
    ROUND(SUM(profit_units)::numeric, 2)                     AS total_profit
FROM sniper_bets_v1
WHERE market_tier IN ('PRIMARY', 'SHADOW_BIG5', 'SHADOW_GLOBAL')
GROUP BY market_tier, league, edge_bucket, odds_bucket
HAVING COUNT(*) FILTER (WHERE odds_close IS NOT NULL) > 0
ORDER BY market_tier, avg_clv DESC NULLS LAST;

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

-- KODE 5: system-wide flags (kill-switch pause, etc.)
CREATE TABLE IF NOT EXISTS system_state (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- KODE 4: per-pick CLV view (Don-lesbar)
CREATE OR REPLACE VIEW sniper_clv_per_pick AS
SELECT
    id, match_id, league,
    home_team || ' vs ' || away_team AS match,
    kickoff_time,
    ROUND(odds_open::numeric, 2)      AS odds_open,
    ROUND(odds_t60::numeric, 2)       AS odds_t60,
    ROUND(odds_close::numeric, 2)     AS odds_close,
    ROUND(clv_t60_pct::numeric, 2)    AS clv_t60_pct,
    ROUND(clv_close_pct::numeric, 2)  AS clv_close_pct,
    CASE
        WHEN odds_close IS NULL              THEN 'Pending'
        WHEN odds_close < odds_open          THEN 'Positive (sharp agrees)'
        WHEN odds_close > odds_open          THEN 'Negative (sharp disagrees)'
        ELSE                                       'Neutral'
    END AS clv_verdict,
    is_positive_clv_close,
    market_open_at_t60, market_open_at_close,
    t60_late_capture, close_late_capture,
    t60_capture_minutes_before,
    close_capture_minutes_before,
    snapshot_failed_count,
    market_tier, is_calibrated, shadow_reason,
    result,
    ROUND(profit_units::numeric, 2)   AS profit_units,
    fixture_status
FROM sniper_bets_v1;
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


def _pinnacle_market_status(odds_response: list[dict]) -> dict:
    """
    KODE 3: rik Pinnacle-status for market-availability tracking.

    Returnerer:
      pinnacle_present:  bool       — Pinnacle bookmaker funnet i respons
      over_25_open:      bool|None  — True om Over 2.5 åpen, False om Pinnacle
                                       finnes men markedet er stengt, None om
                                       Pinnacle ikke i responsen i det hele tatt.
      over_25_odds:      float|None — odds for Over 2.5 hvis tilgjengelig
      bets_summary:      list[dict] — alle Pinnacle-markeder med values_count
                                       (lagres som JSONB ved closing-snapshot).
    """
    out = {
        "pinnacle_present": False,
        "over_25_open": None,
        "over_25_odds": None,
        "bets_summary": [],
    }
    if not odds_response:
        return out
    bookmakers = (odds_response[0].get("bookmakers") or [])
    pinnacle = next(
        (b for b in bookmakers if b.get("id") == PINNACLE_BOOKMAKER_ID),
        None,
    )
    if not pinnacle:
        return out
    out["pinnacle_present"] = True
    out["over_25_open"] = False
    for bet in (pinnacle.get("bets") or []):
        name = bet.get("name") or ""
        values = bet.get("values") or []
        out["bets_summary"].append({"name": name, "values_count": len(values)})
        name_lower = name.lower()
        if "over/under" in name_lower or "goals over" in name_lower:
            for v in values:
                if (v.get("value") or "").lower().strip() == "over 2.5":
                    out["over_25_open"] = True
                    try:
                        out["over_25_odds"] = float(v.get("odd"))
                    except (TypeError, ValueError):
                        pass
                    break
    return out


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
    """
    Send intern Telegram-alert for picks #1-3.

    HARD ISOLATION: kun DON_INTERNAL_TELEGRAM_CHAT_ID (ingen fallback til
    kunde-kanalen TELEGRAM_CHAT_ID). Hvis env mangler → graceful skip,
    picks lagres fortsatt. Don setter env via Railway dashboard når klar.
    """
    token = os.environ.get("TELEGRAM_TOKEN", "")
    intern_chat = os.environ.get("DON_INTERNAL_TELEGRAM_CHAT_ID", "")

    # SHADOW MODE: Don-alert KUN for PRIMARY-tier (calibrated edge ≥9% Big5).
    # SHADOW_BIG5 + SHADOW_GLOBAL skal aldri sendes til Telegram.
    if (payload.get("market_tier") or "PRIMARY") != "PRIMARY":
        return False
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT COUNT(*) AS n FROM sniper_bets_v1 "
            "WHERE market_tier = 'PRIMARY';"
        )
    n_total = int(row["n"]) if row else 0

    if n_total > ALERT_FIRST_N_PICKS:
        return False
    if not token:
        logger.info("[Sniper] Don-alert skipped: TELEGRAM_TOKEN ikke satt")
        return False
    if not intern_chat:
        logger.info(
            "[Sniper] Don-alert skipped: DON_INTERNAL_TELEGRAM_CHAT_ID "
            "ikke satt (sett i Railway dashboard for aktivering)"
        )
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
            f"Kickoff: {payload.get('kickoff_iso') or payload.get('kickoff_time')}\n"
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


# ── SHADOW MODE HELPERS ─────────────────────────────────────────────────────
def _classify_pick(league_id: int, edge: float) -> tuple[str, bool, str | None] | None:
    """
    Tier-klassifisering basert på liga + edge.

    Returns (market_tier, is_calibrated, shadow_reason) eller None om pick
    ikke kvalifiserer noen tier.
    """
    if league_id in PRIMARY_LEAGUES:
        if edge >= EDGE_THRESHOLD:        # ≥9% — PRIMARY (urørt logikk)
            return ("PRIMARY", True, None)
        if edge >= EDGE_THRESHOLD_SHADOW_BIG5:  # 5% ≤ edge < 9% — SHADOW_BIG5
            return ("SHADOW_BIG5", True, "BIG5_LOWER_EDGE")
        return None
    if league_id in SHADOW_GLOBAL_LEAGUES and league_id not in PRIMARY_LEAGUES:
        if edge >= EDGE_THRESHOLD:  # ≥9% — SHADOW_GLOBAL (uncalibrated)
            return ("SHADOW_GLOBAL", False, "GLOBAL_UNCALIBRATED")
        return None
    return None


async def _shadow_picks_today_count(pool) -> int:
    """Telle SHADOW-picks (begge tier) generert i dag UTC. Brukes for cap-check."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT COUNT(*) AS n
            FROM sniper_bets_v1
            WHERE market_tier = ANY($1::TEXT[])
              AND DATE(pick_timestamp AT TIME ZONE 'UTC')
                  = DATE(NOW() AT TIME ZONE 'UTC');
            """,
            list(SHADOW_TIERS),
        )
    return int(row["n"]) if row else 0


async def _log_team_mismatch(pool, *, fixture_id: str, league: str,
                              home_raw: str, away_raw: str,
                              home_norm: str, away_norm: str,
                              teams_in_model: bool) -> None:
    """Log SHADOW_GLOBAL team-mismatch. Idempotent via UNIQUE(fixture_id)."""
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO shadow_team_mismatches
                    (fixture_id, league, home_team_raw, away_team_raw,
                     home_team_normalized, away_team_normalized, teams_in_model)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (fixture_id) DO NOTHING;
                """,
                fixture_id, league, home_raw, away_raw,
                home_norm, away_norm, teams_in_model,
            )
    except Exception as e:
        logger.warning("[Sniper] mismatch log fail %s: %s", fixture_id, e)


async def fetch_fixtures_for_leagues(date_str: str, season: int,
                                      leagues_map: dict[int, str],
                                      client: httpx.AsyncClient) -> list[dict]:
    """Hent fixtures for arbitrary liga-map. Inkluderer _league_name + _league_id."""
    fixtures: list[dict] = []
    for league_id, league_name in leagues_map.items():
        try:
            r = await client.get(
                f"{API_FOOTBALL_BASE}/fixtures",
                headers=_api_headers(),
                params={"date": date_str, "league": league_id, "season": season},
            )
            data = r.json().get("response") or []
            for f in data:
                f["_league_name"] = league_name
                f["_league_id"] = league_id
                fixtures.append(f)
        except Exception as e:
            logger.warning("[Sniper] fixtures fetch %s %s: %s",
                           league_name, date_str, e)
    return fixtures


# ── PICK GENERATION ─────────────────────────────────────────────────────────
async def generate_picks(pool, days_ahead: int = 2) -> dict:
    """
    Hovedjobb. Tre-tier pick generation:
      PRIMARY      — Big5 + edge ≥9% (urørt original logikk, calibrated)
      SHADOW_BIG5  — Big5 + 5% ≤ edge < 9% (calibrated, observer)
      SHADOW_GLOBAL — Top-15 ligaer utenfor Big5 + edge ≥9% (UNCALIBRATED)

    SHADOW har dagligs cap 50 picks. PRIMARY er aldri cappet.
    Telegram-alerts kun for PRIMARY. Kill-switch leser kun PRIMARY.
    """
    from services.dixon_coles_engine import get_dixon_coles_probs

    # KODE 5: respekter Don's kill-switch FØR vi forbruker API-Football quota
    try:
        if await is_sniper_paused(pool):
            logger.warning(
                "[Sniper] generate_picks SKIPPED — system_state.%s = true",
                SNIPER_PAUSE_KEY,
            )
            return {"status": "PAUSED", "reason": "kill_switch_active",
                    "picks_created": 0}
    except Exception as e:
        logger.warning("[Sniper] pause-check failed (proceeding): %s", e)

    today = datetime.now(timezone.utc).date()
    season = _compute_season(today)

    # SHADOW daily cap-check (PRIMARY har egen MAX_PICKS_PER_DAY = 5)
    try:
        shadow_today_initial = await _shadow_picks_today_count(pool)
    except Exception as e:
        logger.warning("[Sniper] shadow-cap query failed: %s", e)
        shadow_today_initial = 0
    shadow_today = shadow_today_initial

    stats = {
        "fixtures_scanned": 0,
        "no_pinnacle_odds": 0,
        "outside_odds_range": 0,
        "model_predict_failed": 0,
        "low_edge": 0,
        "quarantined_high_edge": 0,
        "picks_created": 0,
        "primary_created": 0,
        "shadow_big5_created": 0,
        "shadow_global_created": 0,
        "shadow_team_mismatches_logged": 0,
        "shadow_cap_skipped": 0,
        "shadow_today_initial": shadow_today_initial,
        "ah_intel_logged": 0,
        "alerts_sent": 0,
    }

    # Kombinert liga-map: PRIMARY + SHADOW_GLOBAL (PRIMARY-IDer overskriver
    # SHADOW-IDer om de skulle overlappe, så PRIMARY-klassifisering vinner).
    combined_leagues = {**SHADOW_GLOBAL_LEAGUES, **PRIMARY_LEAGUES}

    async with httpx.AsyncClient(timeout=15.0) as client:
        for offset in range(days_ahead + 1):
            date_str = (today + timedelta(days=offset)).isoformat()
            fixtures = await fetch_fixtures_for_leagues(
                date_str, season, combined_leagues, client,
            )

            for fix in fixtures:
                stats["fixtures_scanned"] += 1
                fix_id = (fix.get("fixture") or {}).get("id")
                if not fix_id:
                    continue
                league_id = fix.get("_league_id")
                if league_id is None:
                    continue

                kickoff_iso = (fix.get("fixture") or {}).get("date")
                teams = fix.get("teams") or {}
                home_raw = (teams.get("home") or {}).get("name") or ""
                away_raw = (teams.get("away") or {}).get("name") or ""
                home_norm = _norm_team(home_raw)
                away_norm = _norm_team(away_raw)
                league_name = fix.get("_league_name") or ""
                is_primary_league = league_id in PRIMARY_LEAGUES

                # SHADOW cap-check FØR vi forbruker odds-quota (kun for shadow leagues)
                if not is_primary_league and shadow_today >= SHADOW_DAILY_CAP:
                    stats["shadow_cap_skipped"] += 1
                    continue

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

                # ── DC PREDICT ──
                fallback_used = False
                try:
                    market_implied_home = 0.5  # placeholder for fallback-arg
                    dc_result = await get_dixon_coles_probs(
                        home_norm, away_norm, market_implied_home,
                    )
                    fallback_used = bool(dc_result.fallback_used)
                    if fallback_used:
                        # SHADOW_GLOBAL: log mismatch (uncalibrated team).
                        # PRIMARY/SHADOW_BIG5 (Big5): bare counter.
                        if not is_primary_league:
                            await _log_team_mismatch(
                                pool,
                                fixture_id=str(fix_id), league=league_name,
                                home_raw=home_raw, away_raw=away_raw,
                                home_norm=home_norm, away_norm=away_norm,
                                teams_in_model=False,
                            )
                            stats["shadow_team_mismatches_logged"] += 1
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
                    if not is_primary_league:
                        await _log_team_mismatch(
                            pool,
                            fixture_id=str(fix_id), league=league_name,
                            home_raw=home_raw, away_raw=away_raw,
                            home_norm=home_norm, away_norm=away_norm,
                            teams_in_model=False,
                        )
                        stats["shadow_team_mismatches_logged"] += 1
                    stats["model_predict_failed"] += 1
                    continue

                implied = 1.0 / odds_open
                edge = prob_over_25 - implied

                # ── TIER-KLASSIFISERING ──
                classification = _classify_pick(league_id, edge)
                if classification is None:
                    stats["low_edge"] += 1
                    continue
                market_tier, is_calibrated, shadow_reason = classification

                # Quarantine gjelder ALLE tiers (>30% edge = bug-signal)
                if edge > EDGE_QUARANTINE:
                    logger.warning(
                        "[Sniper] QUARANTINE edge=%.1f%% on %s vs %s tier=%s — verifiser",
                        edge * 100, home_norm, away_norm, market_tier,
                    )
                    stats["quarantined_high_edge"] += 1
                    continue

                # SHADOW cap re-check (kan ha blitt nådd mid-loop fra parallelle inserts)
                if market_tier in SHADOW_TIERS and shadow_today >= SHADOW_DAILY_CAP:
                    stats["shadow_cap_skipped"] += 1
                    continue

                # asyncpg krever datetime-objekt for TIMESTAMPTZ, ikke ISO-string
                try:
                    kickoff_dt = datetime.fromisoformat(
                        kickoff_iso.replace("Z", "+00:00")
                    ) if kickoff_iso else None
                except (TypeError, ValueError):
                    kickoff_dt = None
                if kickoff_dt is None:
                    continue
                now_dt = datetime.now(timezone.utc)
                payload = {
                    "match_id": str(fix_id),
                    "league": league_name,
                    "home_team": home_norm,
                    "away_team": away_norm,
                    "kickoff_time": kickoff_dt,
                    "kickoff_iso": kickoff_iso,
                    "model_prob": prob_over_25,
                    "market_implied_prob": implied,
                    "edge_pct": edge * 100,
                    "lambda_total": lambda_total,
                    "odds_open": odds_open,
                    "odds_open_timestamp": now_dt,
                    "market_tier": market_tier,
                    "is_calibrated": is_calibrated,
                    "shadow_reason": shadow_reason,
                }

                async with pool.acquire() as conn:
                    # ON CONFLICT DO NOTHING bevarer odds_open ved catchup-runs.
                    inserted = await conn.fetchrow(
                        """
                        INSERT INTO sniper_bets_v1
                            (match_id, league, home_team, away_team, kickoff_time,
                             market, model_prob, market_implied_prob, edge_pct,
                             lambda_total,
                             odds_open, odds_open_timestamp, odds_open_source,
                             result, market_tier, is_calibrated, shadow_reason)
                        VALUES ($1, $2, $3, $4, $5,
                                'OVER_2_5', $6, $7, $8,
                                $9,
                                $10, $11, 'pinnacle',
                                'PENDING', $12, $13, $14)
                        ON CONFLICT (match_id, market) DO NOTHING
                        RETURNING id;
                        """,
                        payload["match_id"], payload["league"],
                        payload["home_team"], payload["away_team"],
                        payload["kickoff_time"],
                        payload["model_prob"], payload["market_implied_prob"],
                        payload["edge_pct"], payload["lambda_total"],
                        payload["odds_open"], payload["odds_open_timestamp"],
                        payload["market_tier"], payload["is_calibrated"],
                        payload["shadow_reason"],
                    )
                if inserted:
                    stats["picks_created"] += 1
                    if market_tier == "PRIMARY":
                        stats["primary_created"] += 1
                        if await _maybe_alert_first_picks(pool, payload):
                            stats["alerts_sent"] += 1
                        if stats["primary_created"] >= MAX_PICKS_PER_DAY:
                            logger.info(
                                "[Sniper] PRIMARY MAX_PICKS_PER_DAY (%d) reached",
                                MAX_PICKS_PER_DAY,
                            )
                            return stats
                    elif market_tier == "SHADOW_BIG5":
                        stats["shadow_big5_created"] += 1
                        shadow_today += 1
                    elif market_tier == "SHADOW_GLOBAL":
                        stats["shadow_global_created"] += 1
                        shadow_today += 1

    return stats


# ── ODDS-SNAPSHOTS ──────────────────────────────────────────────────────────
def _minutes_before_kickoff(now: datetime, kickoff: datetime | None) -> int | None:
    """DEL 1: timing-drift. Negativ verdi = etter kickoff."""
    if kickoff is None:
        return None
    if kickoff.tzinfo is None:
        kickoff = kickoff.replace(tzinfo=timezone.utc)
    return int((kickoff - now).total_seconds() // 60)


async def _capture_t60(pool, row: dict, late: bool, client: httpx.AsyncClient) -> str:
    """
    Per-pick T-60 capture. Skriver odds_t60, clv_t60, market_open_at_t60,
    t60_capture_minutes_before (drift-tracking).
    Returnerer 'updated' | 'failed_no_odds' | 'failed_no_pinnacle'.
    """
    now = datetime.now(timezone.utc)
    minutes_before = _minutes_before_kickoff(now, row.get("kickoff_time"))
    odds_response = await fetch_fixture_odds(int(row["match_id"]), client)
    status = _pinnacle_market_status(odds_response)
    new_odds = status["over_25_odds"]
    market_open = status["over_25_open"]
    if new_odds is None:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE sniper_bets_v1 SET
                    snapshot_failed_count = COALESCE(snapshot_failed_count, 0) + 1,
                    market_open_at_t60 = $1,
                    updated_at = NOW()
                WHERE id = $2;
                """,
                market_open, row["id"],
            )
        return "failed_no_pinnacle" if not status["pinnacle_present"] else "failed_no_odds"
    clv = ((float(row["odds_open"]) - new_odds) / float(row["odds_open"])) * 100
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE sniper_bets_v1 SET
                odds_t60 = $1, odds_t60_timestamp = $2,
                clv_t60_pct = $3,
                market_open_at_t60 = TRUE,
                t60_late_capture = $4,
                t60_capture_minutes_before = $5,
                updated_at = NOW()
            WHERE id = $6;
            """,
            new_odds, now, clv, late, minutes_before, row["id"],
        )
    return "updated"


async def _capture_close(pool, row: dict, late: bool, client: httpx.AsyncClient) -> str:
    """
    Per-pick T-5 close capture. Skriver odds_close, clv_close, is_positive_clv_close,
    market_open_at_close, pinnacle_markets_at_close (JSONB),
    close_capture_minutes_before (drift-tracking).
    """
    now = datetime.now(timezone.utc)
    minutes_before = _minutes_before_kickoff(now, row.get("kickoff_time"))
    odds_response = await fetch_fixture_odds(int(row["match_id"]), client)
    status = _pinnacle_market_status(odds_response)
    new_odds = status["over_25_odds"]
    market_open = status["over_25_open"]
    bets_blob = json.dumps(status["bets_summary"]) if status["pinnacle_present"] else None
    if new_odds is None:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE sniper_bets_v1 SET
                    snapshot_failed_count = COALESCE(snapshot_failed_count, 0) + 1,
                    market_open_at_close = $1,
                    pinnacle_markets_at_close = $2,
                    updated_at = NOW()
                WHERE id = $3;
                """,
                market_open, bets_blob, row["id"],
            )
        return "failed_no_pinnacle" if not status["pinnacle_present"] else "failed_no_odds"
    clv = ((float(row["odds_open"]) - new_odds) / float(row["odds_open"])) * 100
    is_positive = new_odds < float(row["odds_open"])
    # KODE 3: expected_roi_pct = (odds_open / odds_close - 1) * 100
    # Tolkning: "hva ville vi tjent hvis vi solgte tilbake til close-odds?"
    # Positiv = vi var tidlig på riktig side av sharp money.
    expected_roi = (float(row["odds_open"]) / new_odds - 1.0) * 100.0
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE sniper_bets_v1 SET
                odds_close = $1, odds_close_timestamp = $2,
                clv_close_pct = $3, is_positive_clv_close = $4,
                market_open_at_close = TRUE,
                pinnacle_markets_at_close = $5,
                close_late_capture = $6,
                close_capture_minutes_before = $7,
                expected_roi_pct = $8,
                updated_at = NOW()
            WHERE id = $9;
            """,
            new_odds, now, clv, is_positive, bets_blob, late, minutes_before,
            expected_roi, row["id"],
        )
    return "updated"


async def update_odds_t60(pool) -> dict:
    """
    BUG 1: utvidet vindu 50-70 min (var 55-65). Tåler scheduler-jitter.
    BUG 2: retry via fetch_fixture_odds.
    KODE 3: lagrer market_open_at_t60 også når Over 2.5 ikke er åpen.
    """
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
            outcome = await _capture_t60(pool, row, late=False, client=client)
            if outcome == "updated":
                updates += 1
            else:
                failures += 1
    return {"checked": len(rows), "updated": updates, "failures": failures}


async def update_odds_close(pool) -> dict:
    """
    BUG 1: utvidet vindu 3-7 min (var 4-6). Bedre fangst av T-5-tidspunkt.
    BUG 2: retry via fetch_fixture_odds.
    KODE 3: lagrer market_open_at_close + pinnacle_markets_at_close (JSONB).
    KODE 5: kaller check_don_kill_switch hvis nye picks ble settled.
    """
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
            outcome = await _capture_close(pool, row, late=False, client=client)
            if outcome == "updated":
                updates += 1
            else:
                failures += 1
    kill_status = None
    if updates > 0:
        try:
            kill_status = await check_don_kill_switch(pool)
        except Exception as e:
            logger.warning("[Sniper] kill-switch check failed: %s", e)
    return {"checked": len(rows), "updated": updates,
            "failures": failures, "kill_switch": kill_status}


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
            now_dt = datetime.now(timezone.utc)

            # Always update last_settle_attempt_at + fixture_status
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE sniper_bets_v1 SET
                        fixture_status = $1, last_settle_attempt_at = $2,
                        updated_at = NOW()
                    WHERE id = $3;
                    """,
                    status, now_dt, row["id"],
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
    kill_status = None
    if settled > 0 or voided > 0:
        try:
            kill_status = await check_don_kill_switch(pool)
        except Exception as e:
            logger.warning("[Sniper] kill-switch check failed: %s", e)
    return {
        "checked": len(rows),
        "settled": settled,
        "voided": voided,
        "kept_pending": pending_kept,
        "kill_switch": kill_status,
    }


# ── MISSED-SNAPSHOT DETECTOR (KODE 2) ───────────────────────────────────────
async def catch_missed_snapshots(pool) -> dict:
    """
    Backup-cron: hvis primary T-60 eller T-5-cron crasher, late-capture odds
    så lenge marked fortsatt er åpent (= før kickoff).

    T-60 sen-vindu: kickoff in [NOW()+10min, NOW()+50min] AND odds_t60 IS NULL
    T-5  sen-vindu: kickoff in [NOW()+0.5min, NOW()+3min] AND odds_close IS NULL

    Markerer t60_late_capture / close_late_capture = TRUE ved suksess.
    """
    async with pool.acquire() as conn:
        t60_rows = await conn.fetch(
            """
            SELECT id, match_id, odds_open, kickoff_time, snapshot_failed_count
            FROM sniper_bets_v1
            WHERE odds_t60 IS NULL
              AND kickoff_time > NOW() + INTERVAL '10 minutes'
              AND kickoff_time < NOW() + INTERVAL '50 minutes';
            """
        )
        close_rows = await conn.fetch(
            """
            SELECT id, match_id, odds_open, kickoff_time, snapshot_failed_count
            FROM sniper_bets_v1
            WHERE odds_close IS NULL
              AND kickoff_time > NOW() + INTERVAL '30 seconds'
              AND kickoff_time < NOW() + INTERVAL '3 minutes';
            """
        )
    t60_recovered, t60_failed = 0, 0
    close_recovered, close_failed = 0, 0
    async with httpx.AsyncClient(timeout=15.0) as client:
        for row in t60_rows:
            outcome = await _capture_t60(pool, row, late=True, client=client)
            if outcome == "updated":
                t60_recovered += 1
                logger.warning(
                    "[Sniper] late T-60 capture pick_id=%s match_id=%s",
                    row["id"], row["match_id"],
                )
            else:
                t60_failed += 1
        for row in close_rows:
            outcome = await _capture_close(pool, row, late=True, client=client)
            if outcome == "updated":
                close_recovered += 1
                logger.warning(
                    "[Sniper] late close capture pick_id=%s match_id=%s",
                    row["id"], row["match_id"],
                )
            else:
                close_failed += 1
    return {
        "t60_checked": len(t60_rows),
        "t60_recovered": t60_recovered,
        "t60_failed": t60_failed,
        "close_checked": len(close_rows),
        "close_recovered": close_recovered,
        "close_failed": close_failed,
    }


# ── DON'S 5-PICKS KILL-SWITCH (KODE 5) ──────────────────────────────────────
SNIPER_PAUSE_KEY = "sniper_pick_gen_paused"


async def is_sniper_paused(pool) -> bool:
    """Sjekk om generate_picks er pauset av kill-switch."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT value FROM system_state WHERE key = $1;",
            SNIPER_PAUSE_KEY,
        )
    return bool(row) and (row["value"] or "").lower() == "true"


async def check_don_kill_switch(pool) -> dict:
    """
    Don's regel: etter 5 settled picks med odds_close, hvis ≤2 har
    is_positive_clv_close = TRUE, AUTO_STOP pick generation.

    Skriver system_state.sniper_pick_gen_paused = 'true' og logger CRITICAL.
    """
    # SHADOW MODE: kill-switch leser KUN PRIMARY-tier. SHADOW skal aldri
    # påvirke pause-flagget — det er research, ikke styringssignal.
    async with pool.acquire() as conn:
        last_5 = await conn.fetch(
            """
            SELECT id, match_id, is_positive_clv_close
            FROM sniper_bets_v1
            WHERE market_tier = 'PRIMARY'
              AND odds_close IS NOT NULL
              AND is_positive_clv_close IS NOT NULL
            ORDER BY kickoff_time DESC
            LIMIT 5;
            """
        )
    if len(last_5) < 5:
        return {"status": "INSUFFICIENT_DATA", "n": len(last_5)}
    positive_count = sum(1 for p in last_5 if p["is_positive_clv_close"])
    pick_ids = [p["id"] for p in last_5]
    if positive_count <= 2:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO system_state (key, value, updated_at)
                VALUES ($1, 'true', NOW())
                ON CONFLICT (key) DO UPDATE SET value = 'true', updated_at = NOW();
                """,
                SNIPER_PAUSE_KEY,
            )
        logger.critical(
            "[Sniper] DON_KILL_SWITCH_TRIGGERED: %d/5 positive CLV (picks=%s). "
            "Pick generation PAUSED via system_state. Manuell restart kreves.",
            positive_count, pick_ids,
        )
        return {
            "status": "AUTO_STOP",
            "positive_count": positive_count,
            "checked_picks": pick_ids,
            "paused": True,
        }
    return {
        "status": "PASS",
        "positive_count": positive_count,
        "checked_picks": pick_ids,
        "paused": False,
    }


# ── PRE-FLIGHT TEST (KODE 1 + 6 VERIFIKASJONER) ─────────────────────────────
PRE_FLIGHT_MOCK_ODDS_OPEN  = 1.85
PRE_FLIGHT_MOCK_ODDS_T60   = 1.80
PRE_FLIGHT_MOCK_ODDS_CLOSE = 1.75
PRE_FLIGHT_TEST_PREFIX     = "PRE_FLIGHT_TEST_"


def _expected_clv_pct(odds_open: float, new_odds: float) -> float:
    """Speil av CLV-formel i _capture_*. Brukt for V2-verifikasjon."""
    return ((odds_open - new_odds) / odds_open) * 100.0


def _extract_function_body(src: str, def_signature_prefix: str) -> str:
    """Hent funksjonskropp basert på signaturprefix (matcher første def som starter med prefix)."""
    lines = src.splitlines()
    body_lines: list[str] = []
    in_fn = False
    fn_indent = 0
    for ln in lines:
        if not in_fn:
            stripped = ln.lstrip()
            if stripped.startswith(def_signature_prefix):
                in_fn = True
                fn_indent = len(ln) - len(stripped)
                body_lines.append(ln)
            continue
        if ln.strip() == "":
            body_lines.append(ln)
            continue
        cur_indent = len(ln) - len(ln.lstrip())
        if cur_indent <= fn_indent and ln.strip():
            break
        body_lines.append(ln)
    return "\n".join(body_lines)


async def _verify_no_cache_in_sniper_fetch() -> dict:
    """
    V5: bekrefter at sniper-pipelinen aldri leser cached odds.

    Static code inspection scopet til konkrete funksjonskropper for å unngå
    self-reference (V5 leste tidligere sin egen kildekode-streng som false
    positive). Sjekker:
      a) fetch_fixture_odds: kaller client.get() direkte (httpx), uten
         api_football_call eller cache_get.
      b) _capture_t60 / _capture_close: bruker KUN fetch_fixture_odds.
      c) Modulen importerer ingen cache-moduler.
    """
    import os
    src_path = os.path.join(os.path.dirname(__file__), "sniper_live.py")
    try:
        with open(src_path, encoding="utf-8") as fh:
            src = fh.read()
    except Exception as e:
        return {"verdict": "FAIL", "reason": f"cannot read source: {e}"}

    fetch_body  = _extract_function_body(src, "async def fetch_fixture_odds(")
    cap_t60     = _extract_function_body(src, "async def _capture_t60(")
    cap_close   = _extract_function_body(src, "async def _capture_close(")
    import_block = "\n".join(
        ln for ln in src.splitlines()
        if ln.startswith("import ") or ln.startswith("from ")
    )

    findings = {
        "fetch_uses_direct_httpx": (
            "client.get(" in fetch_body
            and "api_football_call(" not in fetch_body
            and "cache_get(" not in fetch_body
        ),
        "capture_t60_uses_only_fetch_fixture_odds": (
            "fetch_fixture_odds(" in cap_t60
            and "api_football_call(" not in cap_t60
        ),
        "capture_close_uses_only_fetch_fixture_odds": (
            "fetch_fixture_odds(" in cap_close
            and "api_football_call(" not in cap_close
        ),
        "no_cache_imports": (
            "api_football_cache" not in import_block
            and "from redis" not in import_block
            and "import redis" not in import_block
        ),
    }
    verdict = "PASS" if all(findings.values()) else "FAIL"
    return {"verdict": verdict, "findings": findings}


async def _verify_timezone(pool) -> dict:
    """V3: PostgreSQL session timezone må være UTC."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                (NOW() AT TIME ZONE 'UTC')::TEXT AS server_now_utc,
                current_setting('TIMEZONE')      AS server_tz,
                NOW()::TEXT                      AS now_raw;
            """
        )
    server_tz_raw = row["server_tz"] or ""
    server_tz = server_tz_raw.upper()
    # Etc/UTC og UTC er IANA-aliaser med identisk offset (+00:00, ingen DST).
    # Postgres returnerer ofte 'Etc/UTC' som canonical via tzdata.
    is_utc = server_tz in ("UTC", "ETC/UTC")
    verdict = "PASS" if is_utc else "FAIL"
    return {
        "verdict": verdict,
        "server_tz": server_tz_raw,
        "server_now_utc": row["server_now_utc"],
        "now_raw": row["now_raw"],
        "accepted_aliases": ["UTC", "Etc/UTC"],
    }


async def _capture_t60_mock(pool, pick_id: int, kickoff_time: datetime,
                             odds_open: float, mock_new_odds: float) -> dict:
    """
    Pre-flight T-60 capture med kjente odds (ingen live API-call).
    Skriver eksakt samme felt som _capture_t60 + drift-tracking.
    """
    now = datetime.now(timezone.utc)
    minutes_before = _minutes_before_kickoff(now, kickoff_time)
    clv = _expected_clv_pct(odds_open, mock_new_odds)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE sniper_bets_v1 SET
                odds_t60 = $1, odds_t60_timestamp = $2,
                clv_t60_pct = $3,
                market_open_at_t60 = TRUE,
                t60_late_capture = FALSE,
                t60_capture_minutes_before = $4,
                updated_at = NOW()
            WHERE id = $5;
            """,
            mock_new_odds, now, clv, minutes_before, pick_id,
        )
    return {
        "now_utc": now.isoformat(),
        "minutes_before": minutes_before,
        "expected_clv_pct": round(clv, 4),
    }


async def _capture_close_mock(pool, pick_id: int, kickoff_time: datetime,
                               odds_open: float, mock_new_odds: float) -> dict:
    """Pre-flight T-5 close capture med kjente odds."""
    now = datetime.now(timezone.utc)
    minutes_before = _minutes_before_kickoff(now, kickoff_time)
    clv = _expected_clv_pct(odds_open, mock_new_odds)
    is_positive = mock_new_odds < odds_open
    expected_roi = (odds_open / mock_new_odds - 1.0) * 100.0
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE sniper_bets_v1 SET
                odds_close = $1, odds_close_timestamp = $2,
                clv_close_pct = $3, is_positive_clv_close = $4,
                market_open_at_close = TRUE,
                pinnacle_markets_at_close = $5,
                close_late_capture = FALSE,
                close_capture_minutes_before = $6,
                expected_roi_pct = $7,
                updated_at = NOW()
            WHERE id = $8;
            """,
            mock_new_odds, now, clv, is_positive,
            json.dumps([{"name": "Goals Over/Under", "values_count": 1}]),
            minutes_before, expected_roi, pick_id,
        )
    return {
        "now_utc": now.isoformat(),
        "minutes_before": minutes_before,
        "expected_clv_pct": round(clv, 4),
        "expected_is_positive": is_positive,
        "expected_roi_pct": round(expected_roi, 4),
    }


async def run_pre_flight_test(pool, pick_id_override: int | None = None) -> dict:
    """
    KODE 1 + 6 VERIFIKASJONER bulletproof pre-flight test.

    INGEN bruk av live-pick (V1: dedikert TEST-rad).
    Injekterer kjente odds (V2: CLV-formel verifisert).
    Sjekker DB session timezone (V3: UTC).
    Måler timing-drift (V4: T-60 ∈ [58,62], T-5 ∈ [3,7]).
    Static code-sjekk for cache-bypass (V5).
    Returnerer scheduler health-hint for V6 (post-deploy curl).

    pick_id_override: ignoreres — pre-flight bruker ALLTID dedikert TEST-rad.
    """
    started_at = datetime.now(timezone.utc)
    test_match_id = f"{PRE_FLIGHT_TEST_PREFIX}{int(started_at.timestamp())}"

    diagnostic: dict = {
        "started_at": started_at.isoformat(),
        "test_match_id": test_match_id,
        "verifications": {},
        "overall": "PENDING",
    }
    if pick_id_override is not None:
        diagnostic["note_pick_id_override_ignored"] = (
            "Pre-flight bruker dedikert TEST-rad. pick_id-parameter ignorert."
        )

    test_pick_id: int | None = None

    try:
        # ── V3: TIMEZONE ──
        tz = await _verify_timezone(pool)
        diagnostic["verifications"]["V3_timezone"] = tz

        # ── V5: NO-CACHE (static code inspection) ──
        diagnostic["verifications"]["V5_no_cache"] = (
            await _verify_no_cache_in_sniper_fetch()
        )

        # ── V1: DEDIKERT TEST-RAD ──
        # Kickoff settes til NOW()+60 min slik at samme rad kan brukes av T-60-leg.
        # T-5-leg vil oppdatere kickoff til NOW()+5 min senere.
        t60_kickoff = datetime.now(timezone.utc) + timedelta(minutes=60)
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO sniper_bets_v1
                    (match_id, league, home_team, away_team, kickoff_time, market,
                     model_prob, market_implied_prob, edge_pct, lambda_total,
                     odds_open, odds_open_timestamp, odds_open_source, result,
                     market_tier, is_calibrated, shadow_reason)
                VALUES
                    ($1, 'TEST', 'TEST_HOME', 'TEST_AWAY', $2, 'OVER_2_5',
                     0.6, 0.5405, 5.95, 2.5,
                     $3, $4, 'pre_flight_mock', 'PENDING',
                     'TEST', FALSE, 'PRE_FLIGHT_TEST_ROW')
                RETURNING id;
                """,
                test_match_id, t60_kickoff,
                PRE_FLIGHT_MOCK_ODDS_OPEN, started_at,
            )
        test_pick_id = int(row["id"])
        diagnostic["verifications"]["V1_test_row_isolation"] = {
            "verdict": "PASS",
            "test_pick_id": test_pick_id,
            "test_match_id": test_match_id,
            "isolation": "Dedikert TEST-rad opprettet — ingen mutasjon av live picks.",
        }

        # ── T-60 LEG: V2 (CLV-formel) + V4 (timing-drift) ──
        t60_capture = await _capture_t60_mock(
            pool, test_pick_id, t60_kickoff,
            PRE_FLIGHT_MOCK_ODDS_OPEN, PRE_FLIGHT_MOCK_ODDS_T60,
        )
        async with pool.acquire() as conn:
            t60_row = await conn.fetchrow(
                """
                SELECT odds_t60, clv_t60_pct, market_open_at_t60,
                       t60_capture_minutes_before
                FROM sniper_bets_v1 WHERE id = $1;
                """,
                test_pick_id,
            )
        t60_clv_db = (
            float(t60_row["clv_t60_pct"])
            if t60_row["clv_t60_pct"] is not None else None
        )
        expected_t60_clv = _expected_clv_pct(
            PRE_FLIGHT_MOCK_ODDS_OPEN, PRE_FLIGHT_MOCK_ODDS_T60,
        )
        t60_clv_diff = (
            abs(t60_clv_db - expected_t60_clv) if t60_clv_db is not None else None
        )
        t60_min_before = t60_row["t60_capture_minutes_before"]

        # ── T-5 LEG ──
        close_kickoff = datetime.now(timezone.utc) + timedelta(minutes=5)
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE sniper_bets_v1 SET kickoff_time = $1 WHERE id = $2;",
                close_kickoff, test_pick_id,
            )
        close_capture = await _capture_close_mock(
            pool, test_pick_id, close_kickoff,
            PRE_FLIGHT_MOCK_ODDS_OPEN, PRE_FLIGHT_MOCK_ODDS_CLOSE,
        )
        async with pool.acquire() as conn:
            close_row = await conn.fetchrow(
                """
                SELECT odds_close, clv_close_pct, is_positive_clv_close,
                       market_open_at_close, close_capture_minutes_before
                FROM sniper_bets_v1 WHERE id = $1;
                """,
                test_pick_id,
            )
        close_clv_db = (
            float(close_row["clv_close_pct"])
            if close_row["clv_close_pct"] is not None else None
        )
        expected_close_clv = _expected_clv_pct(
            PRE_FLIGHT_MOCK_ODDS_OPEN, PRE_FLIGHT_MOCK_ODDS_CLOSE,
        )
        close_clv_diff = (
            abs(close_clv_db - expected_close_clv)
            if close_clv_db is not None else None
        )
        close_min_before = close_row["close_capture_minutes_before"]

        # ── V2: CLV-FORMEL ──
        v2 = {
            "expected": {
                "clv_t60_pct":      round(expected_t60_clv, 4),
                "clv_close_pct":    round(expected_close_clv, 4),
                "is_positive_clv":  PRE_FLIGHT_MOCK_ODDS_CLOSE < PRE_FLIGHT_MOCK_ODDS_OPEN,
            },
            "observed": {
                "clv_t60_pct":      t60_clv_db,
                "clv_close_pct":    close_clv_db,
                "is_positive_clv":  close_row["is_positive_clv_close"],
            },
            "diff_t60":   t60_clv_diff,
            "diff_close": close_clv_diff,
            "tolerance":  0.01,
            "verdict": "PASS" if (
                t60_clv_diff is not None and t60_clv_diff <= 0.01
                and close_clv_diff is not None and close_clv_diff <= 0.01
                and close_row["is_positive_clv_close"] is True
            ) else "FAIL",
        }
        diagnostic["verifications"]["V2_clv_formula"] = v2

        # ── V4: TIMING-DRIFT ──
        v4 = {
            "t60": {
                "minutes_before": t60_min_before,
                "expected_range": [58, 62],
                "verdict": "PASS" if (
                    t60_min_before is not None and 58 <= t60_min_before <= 62
                ) else "FAIL",
            },
            "close": {
                "minutes_before": close_min_before,
                "expected_range": [3, 7],
                "verdict": "PASS" if (
                    close_min_before is not None and 3 <= close_min_before <= 7
                ) else "FAIL",
            },
        }
        v4["verdict"] = (
            "PASS" if v4["t60"]["verdict"] == "PASS"
            and v4["close"]["verdict"] == "PASS" else "FAIL"
        )
        diagnostic["verifications"]["V4_timing_drift"] = v4

        # ── V6: SCHEDULER (post-deploy hint) ──
        diagnostic["verifications"]["V6_scheduler_health"] = {
            "verdict": "POST_DEPLOY_CURL_REQUIRED",
            "instructions": (
                "Etter deploy: curl /admin/scheduler-health og verify "
                "at sniper_pick_generation, sniper_odds_t60, sniper_odds_close, "
                "sniper_settlement, sniper_missed_snapshots alle har "
                "last_success=true (ikke null/false)."
            ),
            "expected_jobs": [
                "sniper_pick_generation",
                "sniper_odds_t60",
                "sniper_odds_close",
                "sniper_settlement",
                "sniper_missed_snapshots",
            ],
        }

        diagnostic["t60_capture"] = t60_capture
        diagnostic["close_capture"] = close_capture

    finally:
        # ── DELETE TEST-RAD (idempotent prefix-delete) ──
        async with pool.acquire() as conn:
            del_count = await conn.fetchval(
                """
                WITH deleted AS (
                    DELETE FROM sniper_bets_v1
                    WHERE match_id LIKE $1
                    RETURNING 1
                )
                SELECT COUNT(*) FROM deleted;
                """,
                f"{PRE_FLIGHT_TEST_PREFIX}%",
            )
        diagnostic["test_rows_deleted"] = int(del_count or 0)

    # ── OVERALL ──
    verdicts = [
        diagnostic["verifications"].get(k, {}).get("verdict")
        for k in ("V1_test_row_isolation", "V2_clv_formula",
                  "V3_timezone", "V4_timing_drift", "V5_no_cache")
    ]
    diagnostic["overall"] = "PASS" if all(v == "PASS" for v in verdicts) else "FAIL"
    diagnostic["finished_at"] = datetime.now(timezone.utc).isoformat()
    return diagnostic


# ── PER-PICK CLV (KODE 4) ───────────────────────────────────────────────────
async def build_clv_per_pick(pool) -> dict:
    """Returner sniper_clv_per_pick view + markdown-tabell for Don."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM sniper_clv_per_pick ORDER BY kickoff_time DESC;"
        )
    picks = []
    for r in rows:
        d = dict(r)
        if d.get("kickoff_time"):
            d["kickoff_time"] = d["kickoff_time"].isoformat()
        for k in ("odds_open", "odds_t60", "odds_close",
                  "clv_t60_pct", "clv_close_pct", "profit_units"):
            if d.get(k) is not None:
                d[k] = float(d[k])
        picks.append(d)

    header = ("| ID | Match | Kickoff | Open | T60 | Close | CLV T60% | "
              "CLV Close% | Verdict | Result | Profit | Status |")
    sep = "|" + "|".join(["---"] * 12) + "|"
    body_lines = []
    for p in picks:
        body_lines.append(
            "| {id} | {match} | {kickoff} | {open} | {t60} | {close} | "
            "{clv_t60} | {clv_close} | {verdict} | {result} | {profit} | "
            "{status} |".format(
                id=p["id"],
                match=(p.get("match") or "")[:30],
                kickoff=(p.get("kickoff_time") or "")[:16],
                open=p.get("odds_open") if p.get("odds_open") is not None else "-",
                t60=p.get("odds_t60") if p.get("odds_t60") is not None else "-",
                close=p.get("odds_close") if p.get("odds_close") is not None else "-",
                clv_t60=(f"{p['clv_t60_pct']:+.2f}"
                         if p.get("clv_t60_pct") is not None else "-"),
                clv_close=(f"{p['clv_close_pct']:+.2f}"
                           if p.get("clv_close_pct") is not None else "-"),
                verdict=p.get("clv_verdict") or "-",
                result=p.get("result") or "PENDING",
                profit=(f"{p['profit_units']:+.2f}"
                        if p.get("profit_units") is not None else "-"),
                status=p.get("fixture_status") or "-",
            )
        )
    return {
        "computed_at": datetime.now(timezone.utc).isoformat(),
        "n_picks": len(picks),
        "picks": picks,
        "markdown_table": "\n".join([header, sep] + body_lines),
    }


# ── KILL-SWITCHES (3-trinns CLV-validation) ─────────────────────────────────
async def check_kill_switches(pool) -> dict:
    """
    Etter 5+ settled close-picks: vurder kill-switch-status.
    SHADOW MODE: leser KUN PRIMARY-tier — SHADOW er research, ikke trigger.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                COUNT(*) FILTER (WHERE odds_close IS NOT NULL) AS n_with_close,
                COUNT(*) FILTER (WHERE is_positive_clv_close) AS n_positive_clv,
                COUNT(*) FILTER (WHERE result IN ('WIN', 'LOSS')) AS n_settled,
                SUM(profit_units) FILTER (WHERE result IN ('WIN', 'LOSS')) AS total_profit,
                AVG(clv_close_pct) FILTER (WHERE clv_close_pct IS NOT NULL) AS avg_clv
            FROM sniper_bets_v1
            WHERE market_tier = 'PRIMARY';
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
async def _summarize_tier(pool, tier: str, window_days: int) -> dict:
    """Aggregat per tier. Brukes 3 ganger fra build_sniper_dashboard."""
    async with pool.acquire() as conn:
        s = await conn.fetchrow(
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
            WHERE market_tier = $1
              AND pick_timestamp > NOW() - ($2 || ' days')::interval;
            """,
            tier, str(window_days),
        )
    s = dict(s) if s else {}
    settled = int(s.get("picks_settled") or 0)
    wins = int(s.get("wins") or 0)
    pclose = int(s.get("picks_with_close") or 0)
    pos = int(s.get("positive_clv") or 0)
    profit = float(s.get("total_profit") or 0)
    return {
        "picks_total":         int(s.get("picks_total") or 0),
        "picks_settled":       settled,
        "picks_with_close":    pclose,
        "wins":                wins,
        "losses":              int(s.get("losses") or 0),
        "win_rate_pct":        round(wins / settled * 100, 1) if settled > 0 else 0,
        "roi_pct":             round(profit / settled * 100, 2) if settled > 0 else 0,
        "total_profit_units":  round(profit, 2),
        "positive_clv_close":  pos,
        "positive_clv_pct":    round(pos / pclose * 100, 1) if pclose > 0 else 0,
        "avg_clv_close_pct":   round(float(s["avg_clv"]), 2)
                                if s.get("avg_clv") is not None else None,
        "avg_edge_pct":        round(float(s["avg_edge_pct"]), 2)
                                if s.get("avg_edge_pct") is not None else None,
    }


# ── PROFIT-MASKIN: KODE 4 — Wilson lower bound (statistisk signifikans) ─────
import math


def compute_wilson_lower_bound(
    positive_count: int, total: int, confidence: float = 0.95,
) -> float:
    """
    Wilson score interval — robust lower bound for proportions med liten n.
    Forhindrer false positive ved multiple comparisons (13 ligaer × random).

    Returnerer 0.0 hvis total <= 0.
    Confidence 0.95 → z=1.96. 0.99 → z=2.576.
    """
    if total <= 0:
        return 0.0
    if confidence >= 0.99:
        z = 2.576
    elif confidence >= 0.975:
        z = 2.241
    elif confidence >= 0.95:
        z = 1.96
    else:
        z = 1.645  # 0.90
    p = positive_count / total
    denom = 1.0 + (z * z) / total
    centre = p + (z * z) / (2.0 * total)
    margin = z * math.sqrt(
        (p * (1.0 - p) + (z * z) / (4.0 * total)) / total
    )
    return max(0.0, (centre - margin) / denom)


# ── PROFIT-MASKIN: KODE 1 — CLV beslutningsmotor (PROPOSALS) ────────────────
DECISION_PRIMARY_MIN_N         = 20
DECISION_SHADOW_BIG5_MIN_N     = 30
DECISION_SHADOW_GLOBAL_MIN_N   = 50
DECISION_NEGATIVE_LEAGUE_MIN_N = 100
DECISION_AUTO_STOP_MIN_N       = 20
DECISION_SUSPEND_BIG5_MIN_N    = 50


async def _tier_clv_aggregate(pool, tier: str) -> dict:
    """Aggregat for decision-engine. Returnerer n_with_close, avg_clv, pct_positive."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                COUNT(*) FILTER (WHERE odds_close IS NOT NULL) AS n_with_close,
                COUNT(*) FILTER (WHERE is_positive_clv_close = TRUE) AS n_positive,
                AVG(clv_close_pct) FILTER (WHERE clv_close_pct IS NOT NULL) AS avg_clv,
                AVG(expected_roi_pct) FILTER (WHERE expected_roi_pct IS NOT NULL)
                    AS avg_expected_roi
            FROM sniper_bets_v1
            WHERE market_tier = $1;
            """,
            tier,
        )
    n = int(row["n_with_close"] or 0)
    pos = int(row["n_positive"] or 0)
    return {
        "n_with_close": n,
        "n_positive": pos,
        "avg_clv": float(row["avg_clv"]) if row["avg_clv"] is not None else None,
        "avg_expected_roi": float(row["avg_expected_roi"])
                            if row["avg_expected_roi"] is not None else None,
        "pct_positive_clv": round(pos / n * 100, 1) if n > 0 else None,
        "wilson_lb_95": round(
            compute_wilson_lower_bound(pos, n, 0.95) * 100, 1
        ) if n > 0 else None,
    }


async def _shadow_global_per_league(pool) -> list[dict]:
    """Per-liga aggregat for SHADOW_GLOBAL — input til Wilson + decision."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                league,
                COUNT(*) FILTER (WHERE odds_close IS NOT NULL) AS n_with_close,
                COUNT(*) FILTER (WHERE is_positive_clv_close = TRUE) AS n_positive,
                AVG(clv_close_pct) FILTER (WHERE clv_close_pct IS NOT NULL) AS avg_clv,
                AVG(expected_roi_pct) FILTER (WHERE expected_roi_pct IS NOT NULL)
                    AS avg_expected_roi
            FROM sniper_bets_v1
            WHERE market_tier = 'SHADOW_GLOBAL'
            GROUP BY league
            ORDER BY n_with_close DESC;
            """
        )
    out = []
    for r in rows:
        n = int(r["n_with_close"] or 0)
        pos = int(r["n_positive"] or 0)
        out.append({
            "league": r["league"],
            "n_with_close": n,
            "n_positive": pos,
            "avg_clv": float(r["avg_clv"]) if r["avg_clv"] is not None else None,
            "avg_expected_roi": float(r["avg_expected_roi"])
                                if r["avg_expected_roi"] is not None else None,
            "pct_positive_clv": round(pos / n * 100, 1) if n > 0 else None,
            "wilson_lb_95": round(
                compute_wilson_lower_bound(pos, n, 0.95) * 100, 1
            ) if n > 0 else None,
        })
    return out


def _classify_primary_decision(agg: dict) -> str:
    """PRIMARY decision-rules. Returnerer decision-streng."""
    n = agg.get("n_with_close") or 0
    if n < DECISION_PRIMARY_MIN_N:
        return "INSUFFICIENT_DATA"
    avg_clv = agg.get("avg_clv")
    pos_pct = agg.get("pct_positive_clv")
    if avg_clv is not None and avg_clv > 0 and pos_pct is not None and pos_pct >= 55.0:
        return "SCALE_VOLUME_PROPOSAL"
    if avg_clv is not None and avg_clv < 0 and n >= DECISION_AUTO_STOP_MIN_N:
        return "CONSIDER_AUTO_STOP"
    return "CONTINUE_OBSERVE"


def _classify_shadow_big5_decision(agg: dict) -> str:
    """SHADOW_BIG5 decision-rules."""
    n = agg.get("n_with_close") or 0
    if n < DECISION_SHADOW_BIG5_MIN_N:
        return "INSUFFICIENT_DATA"
    avg_clv = agg.get("avg_clv")
    pos_pct = agg.get("pct_positive_clv")
    if avg_clv is not None and avg_clv > 0 and pos_pct is not None and pos_pct >= 55.0:
        return "PROMOTE_TO_PRIMARY_LOWER_THRESHOLD"
    if avg_clv is not None and avg_clv < -1.0 and n >= DECISION_SUSPEND_BIG5_MIN_N:
        return "SUSPEND_BIG5_LOW_EDGE"
    return "CONTINUE_OBSERVE"


def _classify_shadow_global_decisions(per_league: list[dict]) -> list[dict]:
    """Per-liga decisions for SHADOW_GLOBAL. Wilson lower bound > 50% kreves
    for promotion (forhindrer multiple-comparisons false positive)."""
    out = []
    for league_agg in per_league:
        n = league_agg.get("n_with_close") or 0
        decision = "INSUFFICIENT_DATA"
        if n >= DECISION_SHADOW_GLOBAL_MIN_N:
            wilson = league_agg.get("wilson_lb_95")
            avg_clv = league_agg.get("avg_clv")
            if wilson is not None and wilson > 50.0:
                decision = "TRIGGER_FORCE_EDGE_DISCOVERY_2"
            elif (n >= DECISION_NEGATIVE_LEAGUE_MIN_N
                  and avg_clv is not None and avg_clv < -1.0):
                decision = "NEGATIVE_LEAGUE_FLAGGED"
            else:
                decision = "CONTINUE_OBSERVE"
        out.append({**league_agg, "decision": decision})
    return out


async def evaluate_clv_decision_layer(pool) -> dict:
    """
    KODE 1: Beslutningsmotor — genererer PROPOSALS (ikke auto-execute).

    Don evaluerer via /admin/clv-decisions før noen action tas.
    Lagrer hver kjøring i clv_decisions for historikk.
    """
    primary_agg = await _tier_clv_aggregate(pool, "PRIMARY")
    sh_big5_agg = await _tier_clv_aggregate(pool, "SHADOW_BIG5")
    sh_global_per_league = await _shadow_global_per_league(pool)

    primary_decision     = _classify_primary_decision(primary_agg)
    shadow_big5_decision = _classify_shadow_big5_decision(sh_big5_agg)
    shadow_global_list   = _classify_shadow_global_decisions(sh_global_per_league)

    actionable_global = [
        d for d in shadow_global_list
        if d["decision"] not in ("INSUFFICIENT_DATA", "CONTINUE_OBSERVE")
    ]

    metadata = {
        "primary_aggregate":        primary_agg,
        "shadow_big5_aggregate":    sh_big5_agg,
        "shadow_global_n_leagues":  len(sh_global_per_league),
        "actionable_global_count":  len(actionable_global),
        "thresholds": {
            "primary_min_n":         DECISION_PRIMARY_MIN_N,
            "shadow_big5_min_n":     DECISION_SHADOW_BIG5_MIN_N,
            "shadow_global_min_n":   DECISION_SHADOW_GLOBAL_MIN_N,
            "wilson_confidence":     0.95,
            "wilson_promote_pct":    50.0,
        },
    }

    async with pool.acquire() as conn:
        decision_id = await conn.fetchval(
            """
            INSERT INTO clv_decisions
                (primary_decision, shadow_big5_decision,
                 shadow_global_decisions, metadata)
            VALUES ($1, $2, $3, $4)
            RETURNING id;
            """,
            primary_decision, shadow_big5_decision,
            json.dumps(shadow_global_list),
            json.dumps(metadata),
        )

    actionable_primary  = primary_decision not in ("INSUFFICIENT_DATA",
                                                    "CONTINUE_OBSERVE")
    actionable_big5     = shadow_big5_decision not in ("INSUFFICIENT_DATA",
                                                        "CONTINUE_OBSERVE")
    if actionable_primary or actionable_big5 or actionable_global:
        logger.critical(
            "[ClvDecisions] PROPOSAL: primary=%s, shadow_big5=%s, "
            "actionable_global=%d (decision_id=%s)",
            primary_decision, shadow_big5_decision,
            len(actionable_global), decision_id,
        )

    return {
        "decision_id":              decision_id,
        "primary_decision":         primary_decision,
        "shadow_big5_decision":     shadow_big5_decision,
        "shadow_global_decisions":  shadow_global_list,
        "metadata":                 metadata,
        "actionable_global":        actionable_global,
    }


async def fetch_clv_decisions(pool, limit: int = 10) -> list[dict]:
    """Read-only: hent siste N decisions for /admin/clv-decisions."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, decision_timestamp, primary_decision,
                   shadow_big5_decision, shadow_global_decisions, metadata
            FROM clv_decisions
            ORDER BY decision_timestamp DESC
            LIMIT $1;
            """,
            limit,
        )
    out = []
    for r in rows:
        d = dict(r)
        if d.get("decision_timestamp"):
            d["decision_timestamp"] = d["decision_timestamp"].isoformat()
        # asyncpg deserialiserer JSONB → str (dict via codec). Pass through.
        for k in ("shadow_global_decisions", "metadata"):
            v = d.get(k)
            if isinstance(v, str):
                try:
                    d[k] = json.loads(v)
                except Exception:
                    pass
        out.append(d)
    return out


# ── PROFIT-MASKIN: KODE 2 — CLV breakdown (markdown + JSON) ─────────────────
async def build_clv_breakdown(pool) -> dict:
    """Returnerer sniper_clv_breakdown + markdown-tabell."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM sniper_clv_breakdown;"
        )
    items = []
    for r in rows:
        d = dict(r)
        for k in ("avg_clv", "pct_positive_clv", "avg_expected_roi_pct",
                  "total_profit"):
            if d.get(k) is not None:
                d[k] = float(d[k])
        items.append(d)

    header = ("| Tier | Liga | Edge | Odds | n_close | avg_clv% | "
              "pos_clv% | exp_roi% | wins | settled | profit |")
    sep = "|" + "|".join(["---"] * 11) + "|"
    body = []
    for it in items:
        body.append(
            "| {tier} | {lg} | {eb} | {ob} | {n} | {clv} | "
            "{pos} | {roi} | {w} | {s} | {p} |".format(
                tier=it.get("market_tier") or "-",
                lg=(it.get("league") or "-")[:24],
                eb=it.get("edge_bucket") or "-",
                ob=it.get("odds_bucket") or "-",
                n=it.get("n_with_close") or 0,
                clv=(f"{it['avg_clv']:+.2f}"
                     if it.get("avg_clv") is not None else "-"),
                pos=(f"{it['pct_positive_clv']:.1f}"
                     if it.get("pct_positive_clv") is not None else "-"),
                roi=(f"{it['avg_expected_roi_pct']:+.2f}"
                     if it.get("avg_expected_roi_pct") is not None else "-"),
                w=it.get("wins") or 0,
                s=it.get("settled") or 0,
                p=(f"{it['total_profit']:+.2f}"
                   if it.get("total_profit") is not None else "-"),
            )
        )
    return {
        "computed_at":     datetime.now(timezone.utc).isoformat(),
        "n_buckets":       len(items),
        "buckets":         items,
        "markdown_table":  "\n".join([header, sep] + body),
    }


# ── PROFIT-MASKIN: KODE 6 — morgen-rapport (Don-lesbar) ─────────────────────
async def generate_morning_report(pool, target_date: datetime | None = None) -> dict:
    """
    Daglig markdown-rapport. Idempotent via UNIQUE(report_date) — ON CONFLICT
    UPDATE bevarer sist genererte versjon for dagen.
    """
    now = datetime.now(timezone.utc)
    report_date = (target_date or now).date()

    async with pool.acquire() as conn:
        last_24h = await conn.fetch(
            """
            SELECT market_tier,
                   COUNT(*) AS picks_total,
                   COUNT(*) FILTER (WHERE odds_close IS NOT NULL) AS n_close,
                   COUNT(*) FILTER (WHERE is_positive_clv_close) AS n_positive,
                   AVG(clv_close_pct) FILTER (WHERE clv_close_pct IS NOT NULL) AS avg_clv,
                   AVG(expected_roi_pct) FILTER (WHERE expected_roi_pct IS NOT NULL)
                       AS avg_exp_roi,
                   COUNT(*) FILTER (WHERE result = 'WIN') AS wins,
                   COUNT(*) FILTER (WHERE result IN ('WIN','LOSS')) AS settled
            FROM sniper_bets_v1
            WHERE pick_timestamp > NOW() - INTERVAL '24 hours'
              AND market_tier IN ('PRIMARY', 'SHADOW_BIG5', 'SHADOW_GLOBAL')
            GROUP BY market_tier;
            """
        )
        top_global = await conn.fetch(
            """
            SELECT league,
                   COUNT(*) FILTER (WHERE odds_close IS NOT NULL) AS n_close,
                   COUNT(*) FILTER (WHERE is_positive_clv_close) AS n_pos,
                   AVG(clv_close_pct) FILTER (WHERE clv_close_pct IS NOT NULL) AS avg_clv
            FROM sniper_bets_v1
            WHERE market_tier = 'SHADOW_GLOBAL'
            GROUP BY league
            HAVING COUNT(*) FILTER (WHERE odds_close IS NOT NULL) >= 5
            ORDER BY avg_clv DESC NULLS LAST
            LIMIT 3;
            """
        )
        latest_decision = await conn.fetchrow(
            """
            SELECT id, decision_timestamp, primary_decision,
                   shadow_big5_decision
            FROM clv_decisions
            ORDER BY decision_timestamp DESC
            LIMIT 1;
            """
        )
        mismatch_24h = await conn.fetchval(
            """
            SELECT COUNT(*) FROM shadow_team_mismatches
            WHERE detected_at > NOW() - INTERVAL '24 hours';
            """
        )
        cap_today = await _shadow_picks_today_count(pool)
        kill = await check_kill_switches(pool)

    by_tier = {r["market_tier"]: dict(r) for r in last_24h}

    def _fmt_tier(tier_key: str, label: str) -> str:
        d = by_tier.get(tier_key, {})
        n_close = int(d.get("n_close") or 0)
        n_pos = int(d.get("n_positive") or 0)
        avg_clv = d.get("avg_clv")
        avg_exp = d.get("avg_exp_roi")
        return (
            f"- **{label}**: picks={d.get('picks_total') or 0}, "
            f"close={n_close}, pos_clv={n_pos}, "
            f"avg_clv={f'{float(avg_clv):+.2f}%' if avg_clv is not None else '-'}, "
            f"avg_exp_roi={f'{float(avg_exp):+.2f}%' if avg_exp is not None else '-'}, "
            f"wins={d.get('wins') or 0}/{d.get('settled') or 0}"
        )

    top_lines = []
    for r in top_global:
        avg_clv = r.get("avg_clv")
        top_lines.append(
            f"  - {r['league']}: n_close={r['n_close']}, "
            f"pos={r['n_pos']}, avg_clv="
            f"{f'{float(avg_clv):+.2f}%' if avg_clv is not None else '-'}"
        )
    top_block = "\n".join(top_lines) if top_lines else "  - (ingen liga med >=5 close-snapshots ennå)"

    decision_block = "  - (ingen decisions kjørt ennå)"
    if latest_decision:
        decision_block = (
            f"  - id={latest_decision['id']}, "
            f"ts={latest_decision['decision_timestamp'].isoformat()}\n"
            f"  - PRIMARY: **{latest_decision['primary_decision']}**\n"
            f"  - SHADOW_BIG5: **{latest_decision['shadow_big5_decision']}**"
        )

    md = (
        f"# Don's Morgen-Rapport — {report_date.isoformat()}\n"
        f"_Generert {now.isoformat()}_\n\n"
        f"## 24t pick-stats per tier\n"
        f"{_fmt_tier('PRIMARY', 'PRIMARY (kunde-grunnlag)')}\n"
        f"{_fmt_tier('SHADOW_BIG5', 'SHADOW_BIG5 (calibrated observer)')}\n"
        f"{_fmt_tier('SHADOW_GLOBAL', 'SHADOW_GLOBAL (UNCALIBRATED research)')}\n\n"
        f"## Kill-switch (PRIMARY)\n"
        f"  - n_with_close={kill.get('n_with_close')}, "
        f"positive_clv_pct={kill.get('positive_clv_pct')}%, "
        f"flags={kill.get('flags')}\n"
        f"  - auto_stop_required={kill.get('auto_stop_required')}\n\n"
        f"## CLV-decisions (siste)\n{decision_block}\n\n"
        f"## Top 3 SHADOW_GLOBAL ligaer (avg_clv)\n{top_block}\n\n"
        f"## Pipeline-status\n"
        f"  - SHADOW cap: {cap_today}/{SHADOW_DAILY_CAP} brukt i dag\n"
        f"  - Team-mismatches siste 24t: {int(mismatch_24h or 0)}\n"
    )

    summary = {
        "report_date":    report_date.isoformat(),
        "by_tier":        {k: dict(v) for k, v in by_tier.items()},
        "kill_switch":    kill,
        "shadow_cap":     {"used_today": cap_today, "limit": SHADOW_DAILY_CAP},
        "team_mismatches_24h": int(mismatch_24h or 0),
        "latest_decision_id": (latest_decision["id"] if latest_decision else None),
    }

    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO don_morning_reports
                (report_date, generated_at, report_markdown, summary_json)
            VALUES ($1, NOW(), $2, $3)
            ON CONFLICT (report_date) DO UPDATE SET
                generated_at = EXCLUDED.generated_at,
                report_markdown = EXCLUDED.report_markdown,
                summary_json = EXCLUDED.summary_json;
            """,
            report_date, md, json.dumps(summary, default=str),
        )

    return {
        "report_date":     report_date.isoformat(),
        "generated_at":    now.isoformat(),
        "report_markdown": md,
        "summary":         summary,
    }


async def fetch_morning_report(pool, target_date: str | None = None) -> dict:
    """Read-only: hent en spesifikk dagsrapport (default i dag)."""
    if target_date:
        date_obj = datetime.fromisoformat(target_date).date()
    else:
        date_obj = datetime.now(timezone.utc).date()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, report_date, generated_at, report_markdown, summary_json
            FROM don_morning_reports
            WHERE report_date = $1;
            """,
            date_obj,
        )
    if not row:
        return {"status": "NOT_FOUND", "report_date": date_obj.isoformat()}
    d = dict(row)
    if d.get("report_date"):
        d["report_date"] = d["report_date"].isoformat()
    if d.get("generated_at"):
        d["generated_at"] = d["generated_at"].isoformat()
    if isinstance(d.get("summary_json"), str):
        try:
            d["summary_json"] = json.loads(d["summary_json"])
        except Exception:
            pass
    return {"status": "ok", **d}


async def build_sniper_dashboard(pool, window_days: int = 30) -> dict:
    """
    SHADOW MODE: 3-veis tier-split + team-mismatch-counter.

    PRIMARY      = locked, calibrated, kunde-leveranse-grunnlag.
    SHADOW_BIG5  = calibrated observer (Big5 5%-9% edge).
    SHADOW_GLOBAL = UNCALIBRATED research (Top-15 utenfor Big5).

    Eksisterende "summary"/"by_league"/"recent_picks" beholdes som
    PRIMARY-only for backward kompatibilitet med curl-clients.
    """
    primary = await _summarize_tier(pool, "PRIMARY",       window_days)
    sh_big5 = await _summarize_tier(pool, "SHADOW_BIG5",   window_days)
    sh_glob = await _summarize_tier(pool, "SHADOW_GLOBAL", window_days)

    async with pool.acquire() as conn:
        by_league = await conn.fetch(
            """
            SELECT market_tier, league, COUNT(*) AS n,
                   COUNT(*) FILTER (WHERE result = 'WIN') AS wins,
                   AVG(edge_pct) AS avg_edge,
                   AVG(clv_close_pct) AS avg_clv
            FROM sniper_bets_v1
            WHERE pick_timestamp > NOW() - ($1 || ' days')::interval
              AND market_tier IN ('PRIMARY', 'SHADOW_BIG5', 'SHADOW_GLOBAL')
            GROUP BY market_tier, league
            ORDER BY market_tier, n DESC;
            """,
            str(window_days),
        )
        recent = await conn.fetch(
            """
            SELECT id, match_id, league, home_team, away_team, kickoff_time,
                   model_prob, market_implied_prob, edge_pct,
                   odds_open, odds_t60, odds_close,
                   clv_close_pct, is_positive_clv_close,
                   result, profit_units, market_tier
            FROM sniper_bets_v1
            WHERE market_tier = 'PRIMARY'
            ORDER BY pick_timestamp DESC
            LIMIT 10;
            """
        )
        mismatch_today = await conn.fetchval(
            """
            SELECT COUNT(*) FROM shadow_team_mismatches
            WHERE DATE(detected_at AT TIME ZONE 'UTC')
                  = DATE(NOW() AT TIME ZONE 'UTC');
            """
        )
        mismatch_window = await conn.fetchval(
            """
            SELECT COUNT(*) FROM shadow_team_mismatches
            WHERE detected_at > NOW() - ($1 || ' days')::interval;
            """,
            str(window_days),
        )
        shadow_today = await _shadow_picks_today_count(pool)

    kill = await check_kill_switches(pool)

    return {
        "computed_at": datetime.now(timezone.utc).isoformat(),
        "window_days": window_days,
        # ── 3-veis split ────────────────────────────────────────
        "primary": {
            **primary,
            "calibrated": True,
            "is_strategic": True,
            "kill_switch_status": kill,
        },
        "shadow_big5": {
            **sh_big5,
            "calibrated": True,
            "is_strategic": False,
            "note": "Observer-tier — Big5 med 5%-9% edge",
        },
        "shadow_global": {
            **sh_glob,
            "calibrated": False,
            "is_strategic": False,
            "warning": "UNCALIBRATED — bruker ikke for strategisk beslutning",
        },
        "shadow_cap": {
            "limit_per_day":  SHADOW_DAILY_CAP,
            "shadow_today":   shadow_today,
            "remaining":      max(0, SHADOW_DAILY_CAP - shadow_today),
        },
        "team_mismatches": {
            "today":         int(mismatch_today or 0),
            "window_days":   int(mismatch_window or 0),
        },
        # ── Backward-kompat (PRIMARY-only) ───────────────────────
        "summary": primary,
        "by_league": [dict(r) for r in by_league],
        "recent_picks": [dict(r) for r in recent],
        "kill_switch_status": kill,
    }
