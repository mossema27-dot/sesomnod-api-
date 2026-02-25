"""
SESOMNOD ENGINE — Automatic Result Checker v1.0
Checks match results from Football-Data.org (primary) or The Odds API (fallback)
Runs automatically after match kickoff — no user interaction needed
"""

import os
import httpx
from datetime import datetime, timezone, timedelta
from typing import Optional

# ── FOOTBALL-DATA.ORG CONFIG ──────────────────────────────────
FOOTBALL_DATA_KEY = os.getenv("FOOTBALL_DATA_KEY", "")  # Set after email confirmation

# Competition IDs on football-data.org
COMPETITION_MAP = {
    "Premier League": "PL",
    "La Liga": "PD",
    "Serie A": "SA",
    "Bundesliga": "BL1",
    "Ligue 1": "FL1",
}

# ── ODDS API CONFIG ───────────────────────────────────────────
ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")

SPORT_MAP = {
    "Premier League": "soccer_epl",
    "La Liga": "soccer_spain_la_liga",
    "Serie A": "soccer_italy_serie_a",
    "Bundesliga": "soccer_germany_bundesliga",
    "Ligue 1": "soccer_france_ligue_one",
}

DISCLAIMER = "Dette er underholdning og analyse. Gamble aldri mer enn du har råd til å tape."


async def check_result_football_data(
    home_team: str,
    away_team: str,
    league: str,
    kickoff_time: str,
) -> Optional[dict]:
    """
    Fetch match result from Football-Data.org
    Returns: {"home_score": int, "away_score": int, "status": "FINISHED"} or None
    """
    if not FOOTBALL_DATA_KEY:
        return None

    competition = COMPETITION_MAP.get(league)
    if not competition:
        return None

    try:
        # Parse kickoff time
        if kickoff_time:
            try:
                dt = datetime.fromisoformat(kickoff_time.replace("Z", "+00:00"))
                date_from = dt.strftime("%Y-%m-%d")
                date_to = (dt + timedelta(days=1)).strftime("%Y-%m-%d")
            except Exception:
                date_from = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                date_to = date_from
        else:
            date_from = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            date_to = date_from

        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"https://api.football-data.org/v4/competitions/{competition}/matches",
                headers={"X-Auth-Token": FOOTBALL_DATA_KEY},
                params={
                    "dateFrom": date_from,
                    "dateTo": date_to,
                    "status": "FINISHED",
                },
                timeout=15,
            )

            if resp.status_code != 200:
                print(f"[FootballData] API error {resp.status_code}: {resp.text[:200]}")
                return None

            data = resp.json()
            matches = data.get("matches", [])

            # Find our match by team names (fuzzy match)
            for match in matches:
                home = match.get("homeTeam", {}).get("name", "").lower()
                away = match.get("awayTeam", {}).get("name", "").lower()
                home_short = match.get("homeTeam", {}).get("shortName", "").lower()
                away_short = match.get("awayTeam", {}).get("shortName", "").lower()

                our_home = home_team.lower()
                our_away = away_team.lower()

                # Check if team names match (partial match allowed)
                home_match = (
                    our_home in home or home in our_home or
                    our_home in home_short or home_short in our_home
                )
                away_match = (
                    our_away in away or away in our_away or
                    our_away in away_short or away_short in our_away
                )

                if home_match and away_match:
                    score = match.get("score", {})
                    full_time = score.get("fullTime", {})
                    home_score = full_time.get("home")
                    away_score = full_time.get("away")

                    if home_score is not None and away_score is not None:
                        return {
                            "home_score": int(home_score),
                            "away_score": int(away_score),
                            "status": match.get("status", "FINISHED"),
                            "source": "football-data.org",
                        }

    except Exception as e:
        print(f"[FootballData] Error: {e}")

    return None


async def check_result_odds_api(
    home_team: str,
    away_team: str,
    league: str,
    kickoff_time: str,
) -> Optional[dict]:
    """
    Fetch match result from The Odds API (scores endpoint)
    Returns: {"home_score": int, "away_score": int, "status": "completed"} or None
    """
    sport = SPORT_MAP.get(league)
    if not sport:
        # Try all sports
        for league_name, sport_key in SPORT_MAP.items():
            if league_name.lower() in league.lower():
                sport = sport_key
                break

    if not sport:
        sport = "soccer_epl"  # fallback

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"https://api.the-odds-api.com/v4/sports/{sport}/scores/",
                params={
                    "apiKey": ODDS_API_KEY,
                    "daysFrom": 1,
                },
                timeout=15,
            )

            if resp.status_code != 200:
                print(f"[OddsAPI Scores] Error {resp.status_code}: {resp.text[:200]}")
                return None

            games = resp.json()
            our_home = home_team.lower()
            our_away = away_team.lower()

            for game in games:
                if game.get("completed") is not True:
                    continue

                api_home = game.get("home_team", "").lower()
                api_away = game.get("away_team", "").lower()

                home_match = our_home in api_home or api_home in our_home
                away_match = our_away in api_away or api_away in our_away

                if home_match and away_match:
                    scores = game.get("scores") or []
                    home_score = None
                    away_score = None
                    for s in scores:
                        if s.get("name", "").lower() == api_home:
                            home_score = int(s.get("score", 0))
                        elif s.get("name", "").lower() == api_away:
                            away_score = int(s.get("score", 0))

                    if home_score is not None and away_score is not None:
                        return {
                            "home_score": home_score,
                            "away_score": away_score,
                            "status": "completed",
                            "source": "the-odds-api",
                        }

    except Exception as e:
        print(f"[OddsAPI Scores] Error: {e}")

    return None


def determine_result(pick: str, home_score: int, away_score: int) -> str:
    """
    Determine W/L/P for a given pick based on match score.
    Handles: 1X2, Over/Under, BTTS, Asian Handicap
    """
    total_goals = home_score + away_score
    pick_lower = pick.lower().strip()

    # ── OVER/UNDER ────────────────────────────────────────────
    if "over" in pick_lower:
        # Extract line: "Over 2.5", "Over 2", etc.
        try:
            line = float(pick_lower.replace("over", "").strip())
            if total_goals > line:
                return "W"
            elif total_goals == line:
                return "P"  # Push (rare with .5 lines)
            else:
                return "L"
        except ValueError:
            pass

    if "under" in pick_lower:
        try:
            line = float(pick_lower.replace("under", "").strip())
            if total_goals < line:
                return "W"
            elif total_goals == line:
                return "P"
            else:
                return "L"
        except ValueError:
            pass

    # ── BTTS (Both Teams To Score) ────────────────────────────
    if "btts" in pick_lower or "begge lag scorer" in pick_lower or "both teams" in pick_lower:
        if "nei" in pick_lower or "no" in pick_lower:
            return "W" if (home_score == 0 or away_score == 0) else "L"
        else:
            return "W" if (home_score > 0 and away_score > 0) else "L"

    # ── 1X2 ───────────────────────────────────────────────────
    if home_score > away_score:
        actual = "1"  # home win
    elif home_score == away_score:
        actual = "X"  # draw
    else:
        actual = "2"  # away win

    # Match pick against result
    if "1" in pick_lower and "x" not in pick_lower and "2" not in pick_lower:
        return "W" if actual == "1" else "L"
    if "x" in pick_lower and "1" not in pick_lower and "2" not in pick_lower:
        return "W" if actual == "X" else "L"
    if "2" in pick_lower and "x" not in pick_lower and "1" not in pick_lower:
        return "W" if actual == "2" else "L"
    if "1x" in pick_lower or "double chance 1x" in pick_lower:
        return "W" if actual in ("1", "X") else "L"
    if "x2" in pick_lower or "double chance x2" in pick_lower:
        return "W" if actual in ("X", "2") else "L"
    if "12" in pick_lower or "double chance 12" in pick_lower:
        return "W" if actual in ("1", "2") else "L"

    # ── ASIAN HANDICAP ────────────────────────────────────────
    if "handicap" in pick_lower or "ah" in pick_lower:
        # Simple AH: "Home -1", "Away +1.5"
        try:
            if "home" in pick_lower or "1" in pick_lower:
                handicap = float(pick_lower.split("-")[-1].split("+")[0].strip().split()[0])
                adj_home = home_score - handicap
                if adj_home > away_score:
                    return "W"
                elif adj_home == away_score:
                    return "P"
                else:
                    return "L"
        except Exception:
            pass

    # Fallback: try to match team name in pick
    return "P"  # Push if we can't determine


def format_win_telegram(
    home_team: str,
    away_team: str,
    home_score: int,
    away_score: int,
    pick: str,
    odds: float,
    bankroll_before: float,
    bankroll_after: float,
    bankroll_goal: float = 30000,
) -> str:
    """Format the WIN Telegram message"""
    profit = bankroll_after - bankroll_before
    progress_pct = min(100, (bankroll_after / bankroll_goal) * 100)
    progress_bar = _progress_bar(progress_pct)

    return f"""🎯✅ <b>RIKTIG! SesomNod Engine leverte!</b>

<b>{home_team} {home_score} – {away_score} {away_team}</b>
<b>{pick}</b> @ {odds:.2f} ✅

Gratulerer! Med riktig system, matematikk og
sannsynlighetsberegning kommer du nærmere
målet ditt på {bankroll_goal:,.0f}kr!

💰 <b>Din bankroll: {bankroll_after:,.0f}kr</b> (+{profit:,.0f}kr)
{progress_bar} {progress_pct:.1f}% av målet

📅 Neste analyse: I morgen kl. 06:00

<i>⚠️ {DISCLAIMER}</i>
<i>SesomNod Engine · Automatisk resultat</i>"""


def format_loss_telegram(
    home_team: str,
    away_team: str,
    home_score: int,
    away_score: int,
    pick: str,
    odds: float,
    bankroll: float,
    bankroll_goal: float = 30000,
) -> str:
    """Format the LOSS Telegram message"""
    progress_pct = min(100, (bankroll / bankroll_goal) * 100)
    progress_bar = _progress_bar(progress_pct)

    return f"""❌ <b>Denne gangen gikk det ikke.</b>

<b>{home_team} {home_score} – {away_score} {away_team}</b>
Kampen endte {home_score}-{away_score} — {pick} gikk ikke inn.

💰 <b>Din bankroll: {bankroll:,.0f}kr</b> (uendret)
{progress_bar} {progress_pct:.1f}% av målet

Statistikk er på vår side over tid!
📅 Neste analyse: I morgen kl. 06:00

<i>⚠️ {DISCLAIMER}</i>
<i>SesomNod Engine · Automatisk resultat</i>"""


def format_push_telegram(
    home_team: str,
    away_team: str,
    home_score: int,
    away_score: int,
    pick: str,
    bankroll: float,
    bankroll_goal: float = 30000,
) -> str:
    """Format the PUSH/VOID Telegram message"""
    progress_pct = min(100, (bankroll / bankroll_goal) * 100)
    progress_bar = _progress_bar(progress_pct)

    return f"""↩️ <b>Kampen endte uavgjort (Push).</b>

<b>{home_team} {home_score} – {away_score} {away_team}</b>
{pick} — Innsats returnert.

💰 <b>Din bankroll: {bankroll:,.0f}kr</b> (uendret)
{progress_bar} {progress_pct:.1f}% av målet

📅 Neste analyse: I morgen kl. 06:00

<i>⚠️ {DISCLAIMER}</i>
<i>SesomNod Engine · Automatisk resultat</i>"""


def _progress_bar(pct: float, length: int = 10) -> str:
    """Generate a text progress bar"""
    filled = int(pct / 100 * length)
    bar = "█" * filled + "░" * (length - filled)
    return f"[{bar}]"
