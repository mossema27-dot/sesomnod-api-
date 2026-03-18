"""
Signal 4 — RefereeSignal
========================
Henter dommer-data fra football-data.org for planlagte kamper.
v10.1.0: Data-innsamlingsfase — 0 atomic points, returnerer dommernavn.
Fremtidige versjoner vil score basert på historisk data.
Returnerer ALLTID et resultat — feiler aldri stille.
"""

import logging
from typing import Optional

import httpx

logger = logging.getLogger("sesomnod.referee")

_LEAGUE_MAP: dict[str, str] = {
    "Premier League": "PL",
    "La Liga": "PD",
    "Bundesliga": "BL1",
    "Serie A": "SA",
    "Ligue 1": "FL1",
    "Eredivisie": "DED",
    "Champions League": "CL",
}

_UNAVAILABLE_LEAGUES = {"Europa League"}


def _fuzzy_match(search: str, api_name: str) -> bool:
    """Fuzzy team name match — fjerner suffikser, normaliserer umlauts."""
    def norm(s: str) -> str:
        s = s.lower().strip()
        s = s.replace("ü","u").replace("ö","o").replace("ä","a").replace("é","e")
        s = s.replace("münchen","munich").replace("köln","cologne")
        for t in [" fc"," cf"," afc"," sc"," ac","fc ","afc ","as ","1. "," 04"," 05"]:
            s = s.replace(t," ")
        return " ".join(s.split())
    s, a = norm(search), norm(api_name)
    return s in a or a in s or (len(s) >= 5 and s[:5] == a[:5])


class RefereeSignal:
    """
    Async context manager for referee signal.
    Bruk: async with RefereeSignal(api_key) as rs:
              result = await rs.get_signal(home_team, away_team, league_name)
    """

    def __init__(self, api_key: str = None):
        self._api_key = api_key or ""
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(timeout=8)
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()

    async def get_signal(
        self,
        home_team: str,
        away_team: str,
        league_name: str,
    ) -> dict:
        """
        Slår opp dommer for kamp via football-data.org.
        v10.1.0: Data-innsamling, atomic_points=0.
        Returnerer alltid — feiler aldri stille.
        """
        if not self._api_key:
            return {
                "signal": "REFEREE_UNAVAILABLE",
                "atomic_points": 0,
                "reason": "Mangler FOOTBALL_DATA_API_KEY",
            }

        if league_name in _UNAVAILABLE_LEAGUES:
            return {"signal": "REFEREE_LEAGUE_UNAVAILABLE", "atomic_points": 0}

        fd_league = _LEAGUE_MAP.get(league_name)
        if not fd_league:
            return {"signal": "REFEREE_LEAGUE_UNKNOWN", "atomic_points": 0}

        try:
            resp = await self._client.get(
                f"https://api.football-data.org/v4/competitions/{fd_league}/matches",
                params={"status": "SCHEDULED"},
                headers={"X-Auth-Token": self._api_key},
            )
            if resp.status_code == 429:
                return {"signal": "REFEREE_RATE_LIMITED", "atomic_points": 0}
            if resp.status_code != 200:
                return {"signal": "REFEREE_API_ERROR", "atomic_points": 0, "http_status": resp.status_code}

            matches = resp.json().get("matches", [])

            # Fuzzy match mot kampen
            match = None
            for m in matches:
                ht = m.get("homeTeam", {}).get("name", "")
                at = m.get("awayTeam", {}).get("name", "")
                if _fuzzy_match(home_team, ht) and _fuzzy_match(away_team, at):
                    match = m
                    break

            if not match:
                return {
                    "signal": "REFEREE_MATCH_NOT_FOUND",
                    "atomic_points": 0,
                    "searched": f"{home_team} vs {away_team}",
                }

            referees = match.get("referees", [])
            main_ref = next(
                (r for r in referees if r.get("type") == "MAIN"),
                referees[0] if referees else None,
            )

            if not main_ref:
                return {"signal": "REFEREE_NOT_ASSIGNED", "atomic_points": 0}

            referee_name = main_ref.get("name", "Unknown")
            nationality = main_ref.get("nationality", "?")

            # Signal 4 gate: minimum 20 kamper for pålitelig scoring
            matches_count = main_ref.get("matches_count", 0)
            if matches_count < 20:
                return {
                    "signal": "NEUTRAL_REF",
                    "atomic_points": 0,
                    "referee_name": referee_name,
                    "referee_matches_count": matches_count,
                    "reason": f"Insufficient data: {matches_count}/20 matches",
                }

            # v10.1.0: Data-innsamling — atomic_points=0
            # Fremtidig: score basert på historisk kort/straffe-rate
            return {
                "signal": "REFEREE_DATA_COLLECTED",
                "atomic_points": 0,
                "referee_name": referee_name,
                "referee_nationality": nationality,
                "referee_matches_count": matches_count,
                "note": "Scoring aktiviseres i v10.2.0 etter datainnsamling",
            }

        except httpx.TimeoutException:
            return {"signal": "REFEREE_TIMEOUT", "atomic_points": 0}
        except Exception as e:
            logger.warning(f"[RefereeSignal] Feil: {e}")
            return {"signal": "REFEREE_ERROR", "atomic_points": 0, "error": str(e)[:100]}
