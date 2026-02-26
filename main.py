"""
SESOMNOD ENGINE — FastAPI Backend v3.0
Full Automation: Auto result-check, bankroll tracker, Telegram, scheduler
"""

import os
import math
import json
import asyncio
import random
from datetime import date, datetime, timedelta, timezone
from typing import Optional, List
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from dagens_kamp import analyze_dagens_kamp, format_dagens_kamp_telegram, DISCLAIMER
from auto_result import (
    check_result_football_data,
    check_result_odds_api,
    determine_result,
    format_win_telegram,
    format_loss_telegram,
    format_push_telegram,
)
from bankroll import (
    ensure_bankroll_tables,
    get_current_bankroll,
    get_bankroll_history,
    apply_win,
    apply_loss,
    apply_push,
    format_daily_summary_telegram,
    BANKROLL_GOAL,
    BANKROLL_START,
)

# ── CONFIG ────────────────────────────────────────────────────
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")
FOOTBALL_DATA_KEY = os.getenv("FOOTBALL_DATA_KEY", "")

SUPABASE_PAT = os.getenv("SUPABASE_PAT", "")
SUPABASE_PROJECT = os.getenv("SUPABASE_PROJECT", "")
SUPABASE_QUERY_URL = f"https://api.supabase.com/v1/projects/{SUPABASE_PROJECT}/database/query"

# ── DATABASE HELPERS ──────────────────────────────────────────
async def db_query(sql: str) -> list:
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            SUPABASE_QUERY_URL,
            headers={
                "Authorization": f"Bearer {SUPABASE_PAT}",
                "Content-Type": "application/json"
            },
            json={"query": sql},
            timeout=30
        )
        if resp.status_code not in (200, 201):
            raise HTTPException(status_code=500, detail=f"DB error: {resp.text}")
        return resp.json()

async def db_execute(sql: str) -> list:
    return await db_query(sql)

# ── TELEGRAM HELPERS ──────────────────────────────────────────
async def send_telegram(message: str) -> bool:
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{TELEGRAM_API}/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": "HTML"
            },
            timeout=15
        )
        return resp.status_code == 200

def format_pick_message(pick: dict, safe_mode: bool = True) -> str:
    tier_emoji = {1: "🔥", 2: "⚡", 3: "📊"}.get(pick.get("tier", 3), "📊")
    mode_tag = "🔒 SAFE MODE" if safe_mode else "🚀 LIVE PICK"
    ev = pick.get("ev_prosent", 0) or 0
    clv_est = pick.get("clv_beregnet", 0) or 0
    msg = f"""<b>⚽ SESOMNOD ENGINE</b>
{mode_tag} {tier_emoji} TIER {pick.get('tier', 3)}

<b>Kamp:</b> {pick.get('kamp', 'N/A')}
<b>Liga:</b> {pick.get('liga', 'N/A')}
<b>Pick:</b> {pick.get('pick', 'N/A')}
<b>Odds:</b> {float(pick.get('odds', 0)):.2f}
<b>Bookie:</b> {pick.get('bookie', 'N/A')}
<b>Stake:</b> {float(pick.get('stake_planlagt', 0)):.1f}%

📈 <b>EV:</b> +{float(ev):.1f}%
📊 <b>Est. CLV:</b> +{float(clv_est):.1f}%

<i>⚠️ {DISCLAIMER}</i>

<i>SesomNod Engine · {date.today().strftime('%d.%m.%Y')}</i>"""
    return msg

def format_result_message(pick: dict) -> str:
    result = pick.get("resultat", "P")
    result_emoji = {"W": "✅", "L": "❌", "P": "↩️"}.get(result, "❓")
    pl = float(pick.get("pl_beregnet", 0) or 0)
    pl_str = f"+{pl:.2f}%" if pl > 0 else f"{pl:.2f}%"
    clv = float(pick.get("clv_beregnet", 0) or 0)
    msg = f"""<b>📋 RESULTAT REGISTRERT</b>

<b>Kamp:</b> {pick.get('kamp', 'N/A')}
<b>Pick:</b> {pick.get('pick', 'N/A')} @ {float(pick.get('odds', 0)):.2f}
<b>Resultat:</b> {result_emoji} {result}
<b>P/L:</b> {pl_str}
<b>CLV:</b> {clv:+.2f}%

<i>⚠️ {DISCLAIMER}</i>
<i>SesomNod Engine · {date.today().strftime('%d.%m.%Y')}</i>"""
    return msg

# ── PYDANTIC MODELS ───────────────────────────────────────────
class PickCreate(BaseModel):
    dato: str
    kamp: str
    liga: str
    pick: str
    odds: float
    bookie: str
    stake_planlagt: float
    tier: int = Field(ge=1, le=3)
    ev_prosent: Optional[float] = None
    kickoff_odds: Optional[float] = None
    closing_odds: Optional[float] = None

class ResultUpdate(BaseModel):
    pick_id: int
    resultat: str = Field(pattern="^[WLP]$")
    closing_odds: Optional[float] = None

class SettingUpdate(BaseModel):
    key: str
    value: str

# ── LIFESPAN ──────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
       # Initialize database tables (optional - app works without DB)
    try:
        await ensure_dagens_kamp_table()
        await ensure_bankroll_tables(db_execute)
    except Exception as e:
        print(f"[LIFESPAN] Database initialization warning: {e}")
        print("[LIFESPAN] Continuing without database...")
    
    # Start background scheduler
    task = asyncio.create_task(background_scheduler())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

async def ensure_dagens_kamp_table():
    try:
        await db_execute("""
            CREATE TABLE IF NOT EXISTS dagens_kamp (
                id SERIAL PRIMARY KEY,
                dato DATE NOT NULL DEFAULT CURRENT_DATE,
                league TEXT,
                league_flag TEXT,
                home_team TEXT NOT NULL,
                away_team TEXT NOT NULL,
                commence_time TIMESTAMPTZ,
                pick TEXT,
                odds NUMERIC(6,3),
                ev_pct NUMERIC(6,2),
                confidence INTEGER,
                home_win_pct NUMERIC(5,1),
                draw_pct NUMERIC(5,1),
                away_win_pct NUMERIC(5,1),
                over25_pct NUMERIC(5,1),
                btts_pct NUMERIC(5,1),
                kelly_stake NUMERIC(5,2),
                simulation_data JSONB,
                rationale TEXT,
                resultat TEXT,
                home_score INTEGER,
                away_score INTEGER,
                result_source TEXT,
                result_checked_at TIMESTAMPTZ,
                posted_telegram BOOLEAN DEFAULT FALSE,
                result_posted_telegram BOOLEAN DEFAULT FALSE,
                matches_analyzed INTEGER DEFAULT 0,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(dato)
            )
        """)
    except Exception as e:
        print(f"[DB] Table creation note: {e}")

# ── BACKGROUND SCHEDULER ──────────────────────────────────────
async def background_scheduler():
    """
    Runs continuously in the background:
    - Every hour: check if we need to run 06:00 analysis
    - Every hour after kickoff: check for match results
    - At 23:00: send daily summary
    """
    print("[Scheduler] Background scheduler started")
    last_analysis_date = None
    last_summary_date = None

    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            now_cet = now_utc + timedelta(hours=1)  # CET/CEST approx
            today = now_cet.date()
            hour = now_cet.hour
            minute = now_cet.minute

            # ── 06:00 ANALYSIS ────────────────────────────────
            if hour == 6 and minute < 30 and last_analysis_date != today:
                print(f"[Scheduler] 06:00 — Running Dagens Kamp analysis for {today}")
                try:
                    analysis = await analyze_and_store()
                    if "error" not in analysis:
                        await post_dagens_kamp_telegram_internal()
                        last_analysis_date = today
                        print(f"[Scheduler] Analysis complete and posted to Telegram")
                except Exception as e:
                    print(f"[Scheduler] Analysis error: {e}")

            # ── RESULT CHECK (every hour, 90min+ after kickoff) ──
            await check_pending_results()

            # ── 23:00 DAILY SUMMARY ───────────────────────────
            if hour == 23 and minute < 30 and last_summary_date != today:
                print(f"[Scheduler] 23:00 — Sending daily summary")
                try:
                    await send_daily_summary_internal()
                    last_summary_date = today
                except Exception as e:
                    print(f"[Scheduler] Summary error: {e}")

        except Exception as e:
            print(f"[Scheduler] Loop error: {e}")

        # Sleep 30 minutes between checks
        await asyncio.sleep(30 * 60)


async def check_pending_results():
    """Check if today's Dagens Kamp has a result to fetch"""
    try:
        today = date.today().isoformat()
        rows = await db_query(f"""
            SELECT * FROM dagens_kamp 
            WHERE dato = '{today}' 
            AND resultat IS NULL
            AND commence_time IS NOT NULL
        """)

        if not rows:
            return  # No pending match today

        row = rows[0]
        kickoff_str = str(row.get("commence_time", ""))

        # Check if match should be finished (kickoff + 2.5 hours)
        try:
            kickoff_dt = datetime.fromisoformat(kickoff_str.replace("Z", "+00:00"))
            now_utc = datetime.now(timezone.utc)
            if kickoff_dt.tzinfo is None:
                kickoff_dt = kickoff_dt.replace(tzinfo=timezone.utc)

            match_end_estimate = kickoff_dt + timedelta(hours=2, minutes=30)

            if now_utc < match_end_estimate:
                return  # Match not finished yet
        except Exception as e:
            print(f"[ResultCheck] Kickoff parse error: {e}")
            # If we can't parse kickoff, check anyway

        home_team = row.get("home_team", "")
        away_team = row.get("away_team", "")
        league = row.get("league", "")
        pick = row.get("pick", "")
        odds = float(row.get("odds", 0) or 0)
        dk_id = row.get("id")

        print(f"[ResultCheck] Checking result for {home_team} vs {away_team}")

        # Try Football-Data.org first, then Odds API
        result_data = await check_result_football_data(home_team, away_team, league, kickoff_str)
        if not result_data:
            result_data = await check_result_odds_api(home_team, away_team, league, kickoff_str)

        if not result_data:
            print(f"[ResultCheck] No result found yet for {home_team} vs {away_team}")
            return

        home_score = result_data["home_score"]
        away_score = result_data["away_score"]
        source = result_data.get("source", "unknown")

        # Determine W/L/P
        wlp = determine_result(pick, home_score, away_score)

        print(f"[ResultCheck] Result: {home_team} {home_score}-{away_score} {away_team} → {wlp}")

        # Update dagens_kamp table
        await db_execute(f"""
            UPDATE dagens_kamp SET
                resultat = '{wlp}',
                home_score = {home_score},
                away_score = {away_score},
                result_source = '{source}',
                result_checked_at = NOW()
            WHERE id = {dk_id}
        """)

        # Update bankroll
        bankroll_result = None
        if wlp == "W":
            bankroll_result = await apply_win(db_execute, db_query, odds, dk_id)
        elif wlp == "L":
            bankroll_result = await apply_loss(db_execute, db_query, dk_id)
        else:
            bankroll_result = await apply_push(db_execute, db_query, dk_id)

        # Send Telegram result message
        already_posted = row.get("result_posted_telegram", False)
        if not already_posted and bankroll_result:
            if wlp == "W":
                msg = format_win_telegram(
                    home_team, away_team, home_score, away_score,
                    pick, odds,
                    bankroll_result["before"], bankroll_result["after"],
                    BANKROLL_GOAL,
                )
            elif wlp == "L":
                msg = format_loss_telegram(
                    home_team, away_team, home_score, away_score,
                    pick, odds,
                    bankroll_result["after"], BANKROLL_GOAL,
                )
            else:
                msg = format_push_telegram(
                    home_team, away_team, home_score, away_score,
                    pick, bankroll_result["after"], BANKROLL_GOAL,
                )

            success = await send_telegram(msg)
            if success:
                await db_execute(f"""
                    UPDATE dagens_kamp SET result_posted_telegram = TRUE WHERE id = {dk_id}
                """)
                print(f"[ResultCheck] Telegram result posted: {wlp}")

    except Exception as e:
        print(f"[ResultCheck] Error: {e}")


async def send_daily_summary_internal():
    """Send the 23:00 daily summary to Telegram"""
    today = date.today().isoformat()

    # Get today's Dagens Kamp result
    dk_rows = await db_query(f"""
        SELECT home_team, away_team, pick, resultat FROM dagens_kamp WHERE dato = '{today}'
    """)
    dk_result = None
    dk_match = None
    if dk_rows:
        dk = dk_rows[0]
        dk_result = dk.get("resultat")
        dk_match = f"{dk.get('home_team','')} vs {dk.get('away_team','')}"

    # Get overall stats
    stats_rows = await db_query("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN resultat = 'W' THEN 1 END) as wins,
            COUNT(CASE WHEN resultat = 'L' THEN 1 END) as losses
        FROM dagens_kamp WHERE resultat IS NOT NULL
    """)
    stats = stats_rows[0] if stats_rows else {}

    # Get current bankroll
    bankroll = await get_current_bankroll(db_query)

    msg = format_daily_summary_telegram(
        bankroll=bankroll,
        bankroll_goal=BANKROLL_GOAL,
        dagens_kamp_result=dk_result,
        dagens_kamp_match=dk_match,
        total_picks=int(stats.get("total", 0) or 0),
        wins=int(stats.get("wins", 0) or 0),
        losses=int(stats.get("losses", 0) or 0),
    )

    await send_telegram(msg)
    print(f"[Summary] Daily summary sent")


# ── FASTAPI APP ───────────────────────────────────────────────
app = FastAPI(
    title="SesomNod Engine API",
    description="Elite betting intelligence backend v3.0 — Full Automation",
    version="3.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── HEALTH ────────────────────────────────────────────────────
@app.get("/health")
async def health():
    
    return {
        "status": "ok",
        "service": "sesomnod-api"
        
        
        
        
    }

# ── PICKS ENDPOINTS ───────────────────────────────────────────
@app.get("/picks")
async def get_picks(limit: int = 50, offset: int = 0, resultat: Optional[str] = None):
    where = f"WHERE resultat = '{resultat}'" if resultat else ""
    sql = f"""
        SELECT * FROM picks 
        {where}
        ORDER BY dato DESC, created_at DESC 
        LIMIT {limit} OFFSET {offset}
    """
    return await db_query(sql)

@app.get("/picks/{pick_id}")
async def get_pick(pick_id: int):
    rows = await db_query(f"SELECT * FROM picks WHERE pick_id = {pick_id}")
    if not rows:
        raise HTTPException(status_code=404, detail="Pick not found")
    return rows[0]

@app.post("/picks")
async def create_pick(pick: PickCreate):
    ev = pick.ev_prosent or 0
    kickoff = pick.kickoff_odds or "NULL"
    closing = pick.closing_odds or "NULL"
    clv = "NULL"
    if pick.closing_odds and pick.odds:
        clv = round(((pick.closing_odds - pick.odds) / pick.odds) * 100, 4)
    sql = f"""
        INSERT INTO picks (dato, kamp, liga, pick, odds, kickoff_odds, closing_odds, 
                           clv_beregnet, bookie, stake_planlagt, ev_prosent, tier)
        VALUES (
            '{pick.dato}', 
            '{pick.kamp.replace("'", "''")}', 
            '{pick.liga.replace("'", "''")}', 
            '{pick.pick.replace("'", "''")}', 
            {pick.odds}, 
            {kickoff if kickoff != "NULL" else "NULL"}, 
            {closing if closing != "NULL" else "NULL"}, 
            {clv}, 
            '{pick.bookie.replace("'", "''")}', 
            {pick.stake_planlagt}, 
            {ev}, 
            {pick.tier}
        )
        RETURNING *
    """
    rows = await db_execute(sql)
    return rows[0] if rows else {"status": "created"}

@app.put("/picks/{pick_id}/result")
async def update_result(pick_id: int, update: ResultUpdate):
    closing_sql = f", closing_odds = {update.closing_odds}" if update.closing_odds else ""
    pick_rows = await db_query(f"SELECT * FROM picks WHERE pick_id = {pick_id}")
    if not pick_rows:
        raise HTTPException(status_code=404, detail="Pick not found")
    pick = pick_rows[0]
    odds = float(pick["odds"])
    stake = float(pick["stake_planlagt"])
    if update.resultat == "W":
        pl = round((odds - 1) * stake, 4)
    elif update.resultat == "L":
        pl = -stake
    else:
        pl = 0
    clv_sql = ""
    if update.closing_odds:
        clv = round(((update.closing_odds - odds) / odds) * 100, 4)
        clv_sql = f", clv_beregnet = {clv}"
    sql = f"""
        UPDATE picks 
        SET resultat = '{update.resultat}', 
            pl_beregnet = {pl},
            updated_at = NOW()
            {closing_sql}
            {clv_sql}
        WHERE pick_id = {pick_id}
        RETURNING *
    """
    rows = await db_execute(sql)
    return rows[0] if rows else {"status": "updated"}

# ── STATS ENDPOINTS ───────────────────────────────────────────
@app.get("/stats")
async def get_stats():
    rows = await db_query("""
        SELECT 
            COUNT(*) as total_picks,
            COUNT(CASE WHEN resultat = 'W' THEN 1 END) as wins,
            COUNT(CASE WHEN resultat = 'L' THEN 1 END) as losses,
            COUNT(CASE WHEN resultat = 'P' THEN 1 END) as pushes,
            COUNT(CASE WHEN resultat IS NULL THEN 1 END) as pending,
            COALESCE(SUM(pl_beregnet), 0) as total_pl,
            COALESCE(AVG(CASE WHEN resultat IS NOT NULL AND resultat != 'P' THEN clv_beregnet END), 0) as avg_clv,
            COALESCE(AVG(ev_prosent), 0) as avg_ev
        FROM picks
    """)
    stats = rows[0] if rows else {}
    total = int(stats.get("total_picks", 0) or 0)
    wins = int(stats.get("wins", 0) or 0)
    losses = int(stats.get("losses", 0) or 0)
    settled = wins + losses
    winrate = round((wins / settled * 100), 1) if settled > 0 else 0
    roi = round((float(stats.get("total_pl", 0) or 0) / (settled * 2) * 100), 2) if settled > 0 else 0
    return {
        "total_picks": total,
        "wins": wins,
        "losses": losses,
        "pushes": int(stats.get("pushes", 0) or 0),
        "pending": int(stats.get("pending", 0) or 0),
        "winrate": winrate,
        "total_pl": float(stats.get("total_pl", 0) or 0),
        "roi": roi,
        "avg_clv": float(stats.get("avg_clv", 0) or 0),
        "avg_ev": float(stats.get("avg_ev", 0) or 0),
    }

@app.get("/stats/daily")
async def get_daily_stats(days: int = 30):
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    rows = await db_query(f"""
        SELECT 
            dato,
            COUNT(*) as picks,
            COUNT(CASE WHEN resultat = 'W' THEN 1 END) as wins,
            COALESCE(SUM(pl_beregnet), 0) as pl
        FROM picks
        WHERE dato >= '{cutoff}' AND resultat IS NOT NULL
        GROUP BY dato
        ORDER BY dato ASC
    """)
    return rows

# ── KELLY CALCULATOR ──────────────────────────────────────────
@app.get("/kelly")
async def kelly_calculator(
    odds: float,
    prob: float,
    bankroll: float = 10000,
    fraction: float = 0.25,
):
    if odds <= 1 or prob <= 0 or prob >= 1:
        raise HTTPException(status_code=400, detail="Invalid parameters")
    b = odds - 1
    q = 1 - prob
    kelly_full = (b * prob - q) / b
    kelly_fractional = kelly_full * fraction
    stake_pct = max(0, kelly_fractional * 100)
    stake_amount = bankroll * max(0, kelly_fractional)
    ev = (b * prob - q) * 100
    return {
        "kelly_full": round(kelly_full * 100, 2),
        "kelly_fractional": round(kelly_fractional * 100, 2),
        "stake_pct": round(stake_pct, 2),
        "stake_amount": round(stake_amount, 2),
        "ev_pct": round(ev, 2),
        "recommended_tier": 1 if stake_pct >= 3 else (2 if stake_pct >= 1.5 else 3),
    }

# ── SETTINGS ENDPOINTS ────────────────────────────────────────
@app.get("/settings")
async def get_settings():
    rows = await db_query("SELECT key, value FROM settings ORDER BY key")
    return {row["key"]: row["value"] for row in rows}

@app.put("/settings")
async def update_setting(update: SettingUpdate):
    await db_execute(f"""
        INSERT INTO settings (key, value) VALUES ('{update.key}', '{update.value}')
        ON CONFLICT (key) DO UPDATE SET value = '{update.value}', updated_at = NOW()
    """)
    return {"status": "updated", "key": update.key, "value": update.value}

# ── BANKROLL ENDPOINTS ────────────────────────────────────────
@app.get("/bankroll")
async def get_bankroll():
    """Get current bankroll and goal progress"""
    current = await get_current_bankroll(db_query)
    history = await get_bankroll_history(db_query)
    progress_pct = min(100, round(current / BANKROLL_GOAL * 100, 2))
    return {
        "current": current,
        "goal": BANKROLL_GOAL,
        "start": BANKROLL_START,
        "progress_pct": progress_pct,
        "history": history,
    }

@app.get("/bankroll/history")
async def get_bankroll_history_endpoint(days: int = 90):
    """Get bankroll history for equity curve"""
    history = await get_bankroll_history(db_query, days)
    return history

@app.post("/bankroll/reset")
async def reset_bankroll():
    """Reset bankroll to starting amount (for testing)"""
    await db_execute(f"""
        INSERT INTO bankroll (dato, amount, change_amount, change_reason)
        VALUES (CURRENT_DATE, {BANKROLL_START}, 0, 'Manual reset')
    """)
    return {"status": "reset", "amount": BANKROLL_START}

# ── TELEGRAM ENDPOINTS ────────────────────────────────────────
@app.post("/telegram/pick/{pick_id}")
async def post_pick_to_telegram(pick_id: int, safe_mode: bool = True):
    pick_rows = await db_query(f"SELECT * FROM picks WHERE pick_id = {pick_id}")
    if not pick_rows:
        raise HTTPException(status_code=404, detail="Pick not found")
    pick = pick_rows[0]
    message = format_pick_message(pick, safe_mode)
    success = await send_telegram(message)
    return {"success": success, "message": message[:100] + "..."}

@app.post("/telegram/result/{pick_id}")
async def post_result_to_telegram(pick_id: int):
    pick_rows = await db_query(f"SELECT * FROM picks WHERE pick_id = {pick_id}")
    if not pick_rows:
        raise HTTPException(status_code=404, detail="Pick not found")
    pick = pick_rows[0]
    if not pick.get("resultat"):
        raise HTTPException(status_code=400, detail="No result registered yet")
    message = format_result_message(pick)
    success = await send_telegram(message)
    return {"success": success}

@app.post("/telegram/summary")
async def post_daily_summary():
    await send_daily_summary_internal()
    return {"success": True}

@app.post("/telegram/test")
async def test_telegram():
    bankroll = await get_current_bankroll(db_query)
    progress_pct = min(100, round(bankroll / BANKROLL_GOAL * 100, 2))
    msg = f"""<b>🔧 SESOMNOD ENGINE v3.0 — TEST</b>

✅ Backend API tilkoblet
✅ Supabase PostgreSQL aktiv
✅ Telegram-integrasjon fungerer
✅ Dagens Kamp AI-engine klar
✅ Automatisk resultat-sjekk aktiv
✅ Bankroll-tracker aktiv

💰 Bankroll: {bankroll:,.0f}kr ({progress_pct:.1f}% av {BANKROLL_GOAL:,.0f}kr)

<i>⚠️ {DISCLAIMER}</i>
<i>Alt er klart for full automatisering!</i>"""
    success = await send_telegram(msg)
    return {"success": success}

# ── DAGENS KAMP ENDPOINTS ─────────────────────────────────────
@app.get("/dagens-kamp")
async def get_dagens_kamp():
    today = date.today().isoformat()
    rows = await db_query(f"SELECT * FROM dagens_kamp WHERE dato = '{today}'")

    if rows:
        row = rows[0]
        sim_data = row.get("simulation_data") or {}
        if isinstance(sim_data, str):
            sim_data = json.loads(sim_data)

        return {
            "status": "cached",
            "analyzed_at": str(row.get("created_at", "")),
            "match": {
                "league": row.get("league", ""),
                "league_flag": row.get("league_flag", ""),
                "home_team": row.get("home_team", ""),
                "away_team": row.get("away_team", ""),
                "commence_time": str(row.get("commence_time", "")),
                "kickoff_display": _format_kickoff_str(str(row.get("commence_time", ""))),
            },
            "probabilities": {
                "home_win": float(row.get("home_win_pct", 0) or 0),
                "draw": float(row.get("draw_pct", 0) or 0),
                "away_win": float(row.get("away_win_pct", 0) or 0),
                "over25": float(row.get("over25_pct", 0) or 0),
                "btts": float(row.get("btts_pct", 0) or 0),
            },
            "recommendation": {
                "pick": row.get("pick", ""),
                "odds": float(row.get("odds", 0) or 0),
                "ev_pct": float(row.get("ev_pct", 0) or 0),
                "confidence": int(row.get("confidence", 0) or 0),
                "kelly_stake_pct": float(row.get("kelly_stake", 0) or 0),
            },
            "simulations": sim_data,
            "rationale": row.get("rationale", ""),
            "disclaimer": DISCLAIMER,
            "matches_analyzed": int(row.get("matches_analyzed", 0) or 0),
            "resultat": row.get("resultat"),
            "home_score": row.get("home_score"),
            "away_score": row.get("away_score"),
        }

    return await analyze_and_store()

@app.post("/dagens-kamp/analyze")
async def trigger_analysis(background_tasks: BackgroundTasks):
    background_tasks.add_task(analyze_and_store_bg)
    return {"status": "analyzing", "message": "Analyse startet — sjekk /dagens-kamp om 10-15 sekunder"}

@app.post("/dagens-kamp/analyze/sync")
async def trigger_analysis_sync():
    return await analyze_and_store()

@app.post("/dagens-kamp/check-result")
async def manual_check_result():
    """Manually trigger result check for today's Dagens Kamp"""
    await check_pending_results()
    today = date.today().isoformat()
    rows = await db_query(f"SELECT resultat, home_score, away_score FROM dagens_kamp WHERE dato = '{today}'")
    if rows and rows[0].get("resultat"):
        return {"status": "found", "result": rows[0]}
    return {"status": "pending", "message": "Ingen resultat funnet ennå"}

async def analyze_and_store() -> dict:
    analysis = await analyze_dagens_kamp(ODDS_API_KEY)

    if "error" in analysis:
        return analysis

    m = analysis["match"]
    probs = analysis["probabilities"]
    rec = analysis["recommendation"]
    sims = analysis["simulations"]

    sim_json = json.dumps(sims).replace("'", "''")
    rationale = (analysis.get("rationale", "") or "").replace("'", "''")
    today = date.today().isoformat()

    try:
        await db_execute(f"""
            INSERT INTO dagens_kamp (
                dato, league, league_flag, home_team, away_team, commence_time,
                pick, odds, ev_pct, confidence,
                home_win_pct, draw_pct, away_win_pct, over25_pct, btts_pct,
                kelly_stake, simulation_data, rationale, matches_analyzed
            ) VALUES (
                '{today}',
                '{(m.get("league","") or "").replace("'","''")}',
                '{(m.get("league_flag","") or "").replace("'","''")}',
                '{(m.get("home_team","") or "").replace("'","''")}',
                '{(m.get("away_team","") or "").replace("'","''")}',
                '{m.get("commence_time","") or ""}',
                '{(rec.get("pick","") or "").replace("'","''")}',
                {rec.get("odds", 0) or 0},
                {rec.get("ev_pct", 0) or 0},
                {rec.get("confidence", 50) or 50},
                {probs.get("home_win", 0) or 0},
                {probs.get("draw", 0) or 0},
                {probs.get("away_win", 0) or 0},
                {probs.get("over25", 0) or 0},
                {probs.get("btts", 0) or 0},
                {rec.get("kelly_stake_pct", 0) or 0},
                '{sim_json}'::jsonb,
                '{rationale}',
                {analysis.get("matches_analyzed", 0) or 0}
            )
            ON CONFLICT (dato) DO UPDATE SET
                league = EXCLUDED.league,
                league_flag = EXCLUDED.league_flag,
                home_team = EXCLUDED.home_team,
                away_team = EXCLUDED.away_team,
                commence_time = EXCLUDED.commence_time,
                pick = EXCLUDED.pick,
                odds = EXCLUDED.odds,
                ev_pct = EXCLUDED.ev_pct,
                confidence = EXCLUDED.confidence,
                home_win_pct = EXCLUDED.home_win_pct,
                draw_pct = EXCLUDED.draw_pct,
                away_win_pct = EXCLUDED.away_win_pct,
                over25_pct = EXCLUDED.over25_pct,
                btts_pct = EXCLUDED.btts_pct,
                kelly_stake = EXCLUDED.kelly_stake,
                simulation_data = EXCLUDED.simulation_data,
                rationale = EXCLUDED.rationale,
                matches_analyzed = EXCLUDED.matches_analyzed,
                created_at = NOW()
        """)
    except Exception as e:
        print(f"[DagensKamp] DB store error: {e}")

    return analysis

async def analyze_and_store_bg():
    try:
        await analyze_and_store()
    except Exception as e:
        print(f"[DagensKamp] Background analysis error: {e}")

async def post_dagens_kamp_telegram_internal():
    today = date.today().isoformat()
    rows = await db_query(f"SELECT * FROM dagens_kamp WHERE dato = '{today}'")
    if not rows:
        return
    row = rows[0]
    msg = f"""🎯 <b>DAGENS KAMP FUNNET!</b>

<b>{row.get('league_flag','')} {row.get('league','')}</b>
<b>{row.get('home_team','')} vs {row.get('away_team','')}</b>
Kickoff: {_format_kickoff_str(str(row.get('commence_time','')))}

📊 <b>Sannsynligheter:</b>
• Over 2.5 mål: <b>{float(row.get('over25_pct',0) or 0):.0f}%</b>
• Begge lag scorer: <b>{float(row.get('btts_pct',0) or 0):.0f}%</b>
• {row.get('home_team','')} vinner: {float(row.get('home_win_pct',0) or 0):.0f}%
• Uavgjort: {float(row.get('draw_pct',0) or 0):.0f}%
• {row.get('away_team','')} vinner: {float(row.get('away_win_pct',0) or 0):.0f}%

🎯 <b>Anbefalt pick:</b> {row.get('pick','')} @ {float(row.get('odds',0) or 0):.2f}
📈 EV: +{float(row.get('ev_pct',0) or 0):.1f}% | Stake: {float(row.get('kelly_stake',0) or 0):.1f}%

🔬 <b>Match Confidence: {int(row.get('confidence',0) or 0)}%</b>
Basert på 100 scenario-simuleringer

<i>⚠️ {DISCLAIMER}</i>
<i>SesomNod Engine · Se full analyse i app</i>"""

    success = await send_telegram(msg)
    if success:
        await db_execute(f"UPDATE dagens_kamp SET posted_telegram = TRUE WHERE dato = '{today}'")

@app.post("/dagens-kamp/telegram")
async def post_dagens_kamp_telegram():
    await post_dagens_kamp_telegram_internal()
    return {"success": True}

@app.get("/dagens-kamp/history")
async def get_dagens_kamp_history(days: int = 30):
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    rows = await db_query(f"""
        SELECT dato, league, league_flag, home_team, away_team, pick, odds, 
               ev_pct, confidence, over25_pct, btts_pct, resultat,
               home_score, away_score, posted_telegram, result_posted_telegram
        FROM dagens_kamp
        WHERE dato >= '{cutoff}'
        ORDER BY dato DESC
    """)
    return rows

def _format_kickoff_str(dt_str: str) -> str:
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00").split("+")[0])
        cet = dt + timedelta(hours=1)
        return cet.strftime("%-d. %b kl. %H:%M")
    except Exception:
        return dt_str

# ── ODDS API PROXY ────────────────────────────────────────────
@app.get("/odds/live")
async def get_live_odds(sport: str = "soccer_epl", regions: str = "eu"):
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"https://api.the-odds-api.com/v4/sports/{sport}/odds/",
            params={
                "apiKey": ODDS_API_KEY,
                "regions": regions,
                "markets": "h2h,totals",
                "oddsFormat": "decimal",
            },
            timeout=15,
        )
        if resp.status_code == 200:
            return resp.json()
        return {"error": "Failed to fetch odds", "status": resp.status_code}

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port, reload=False)
