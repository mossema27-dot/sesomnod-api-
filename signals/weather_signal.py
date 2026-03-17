"""
Signal 3 — WeatherSignal
========================
Henter værmeldingsdata fra OpenWeatherMap og klassifiserer
spilleforhold. Returnerer ALLTID et resultat — feiler aldri stille.
Bruker async context manager med httpx.AsyncClient.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Optional, Tuple

import httpx

logger = logging.getLogger("sesomnod.weather")

# ── By-mapping: lag → (by, landkode) ─────────────────────────────────────────
TEAM_CITY_MAP: dict[str, tuple[str, str]] = {
    # Premier League
    "Arsenal": ("London", "GB"),
    "Chelsea": ("London", "GB"),
    "Manchester City": ("Manchester", "GB"),
    "Manchester United": ("Manchester", "GB"),
    "Liverpool": ("Liverpool", "GB"),
    "Tottenham Hotspur": ("London", "GB"),
    "Brighton & Hove Albion": ("Brighton", "GB"),
    "Brighton": ("Brighton", "GB"),
    "Newcastle United": ("Newcastle upon Tyne", "GB"),
    "Aston Villa": ("Birmingham", "GB"),
    "West Ham United": ("London", "GB"),
    "Nottingham Forest": ("Nottingham", "GB"),
    "Fulham": ("London", "GB"),
    "Brentford": ("London", "GB"),
    "Crystal Palace": ("London", "GB"),
    "Everton": ("Liverpool", "GB"),
    "Leicester City": ("Leicester", "GB"),
    "Wolverhampton Wanderers": ("Wolverhampton", "GB"),
    "Wolverhampton": ("Wolverhampton", "GB"),
    "Southampton": ("Southampton", "GB"),
    "Ipswich Town": ("Ipswich", "GB"),
    "Bournemouth": ("Bournemouth", "GB"),
    "Sunderland": ("Sunderland", "GB"),
    "Leeds United": ("Leeds", "GB"),
    # Bundesliga
    "Bayern Munich": ("Munich", "DE"),
    "FC Bayern München": ("Munich", "DE"),
    "Borussia Dortmund": ("Dortmund", "DE"),
    "Bayer Leverkusen": ("Leverkusen", "DE"),
    "RB Leipzig": ("Leipzig", "DE"),
    "Eintracht Frankfurt": ("Frankfurt am Main", "DE"),
    "SC Freiburg": ("Freiburg im Breisgau", "DE"),
    "Borussia Monchengladbach": ("Mönchengladbach", "DE"),
    "Borussia Mönchengladbach": ("Mönchengladbach", "DE"),
    "Union Berlin": ("Berlin", "DE"),
    "VfL Wolfsburg": ("Wolfsburg", "DE"),
    "VfB Stuttgart": ("Stuttgart", "DE"),
    "FC Augsburg": ("Augsburg", "DE"),
    "1. FSV Mainz 05": ("Mainz", "DE"),
    "SV Werder Bremen": ("Bremen", "DE"),
    "TSG 1899 Hoffenheim": ("Sinsheim", "DE"),
    "1. FC Heidenheim 1846": ("Heidenheim an der Brenz", "DE"),
    "FC St. Pauli": ("Hamburg", "DE"),
    "Hamburger SV": ("Hamburg", "DE"),
    # La Liga
    "Real Madrid": ("Madrid", "ES"),
    "Barcelona": ("Barcelona", "ES"),
    "Atletico Madrid": ("Madrid", "ES"),
    "Athletic Bilbao": ("Bilbao", "ES"),
    "Athletic Club": ("Bilbao", "ES"),
    "Real Sociedad": ("San Sebastián", "ES"),
    "Villarreal": ("Villarreal", "ES"),
    "Valencia": ("Valencia", "ES"),
    "Sevilla": ("Sevilla", "ES"),
    "Real Betis": ("Seville", "ES"),
    "Getafe": ("Madrid", "ES"),
    "Osasuna": ("Pamplona", "ES"),
    "Girona": ("Girona", "ES"),
    "Celta Vigo": ("Vigo", "ES"),
    "Mallorca": ("Palma", "ES"),
    "Rayo Vallecano": ("Madrid", "ES"),
    "Alaves": ("Vitoria", "ES"),
    "Espanyol": ("Barcelona", "ES"),
    "Las Palmas": ("Las Palmas", "ES"),
    "Valladolid": ("Valladolid", "ES"),
    # Serie A
    "Juventus": ("Turin", "IT"),
    "AC Milan": ("Milan", "IT"),
    "Inter Milan": ("Milan", "IT"),
    "Napoli": ("Naples", "IT"),
    "AS Roma": ("Rome", "IT"),
    "Lazio": ("Rome", "IT"),
    "Atalanta": ("Bergamo", "IT"),
    "Fiorentina": ("Florence", "IT"),
    "Bologna": ("Bologna", "IT"),
    "Torino": ("Turin", "IT"),
    "Udinese": ("Udine", "IT"),
    "Sassuolo": ("Sassuolo", "IT"),
    "Lecce": ("Lecce", "IT"),
    "Cagliari": ("Cagliari", "IT"),
    "Hellas Verona": ("Verona", "IT"),
    "Genoa": ("Genoa", "IT"),
    "Como": ("Como", "IT"),
    "Parma": ("Parma", "IT"),
    "Pisa": ("Pisa", "IT"),
    # Ligue 1
    "Paris Saint-Germain": ("Paris", "FR"),
    "Olympique de Marseille": ("Marseille", "FR"),
    "Marseille": ("Marseille", "FR"),
    "Olympique Lyonnais": ("Lyon", "FR"),
    "Lyon": ("Lyon", "FR"),
    "AS Monaco": ("Monaco", "MC"),
    "Monaco": ("Monaco", "MC"),
    "OGC Nice": ("Nice", "FR"),
    "Nice": ("Nice", "FR"),
    "Lille": ("Lille", "FR"),
    "Stade Rennais": ("Rennes", "FR"),
    "Rennes": ("Rennes", "FR"),
    "RC Strasbourg": ("Strasbourg", "FR"),
    "Strasbourg": ("Strasbourg", "FR"),
    "Toulouse": ("Toulouse", "FR"),
    "Metz": ("Metz", "FR"),
    "Angers": ("Angers", "FR"),
    "Le Havre": ("Le Havre", "FR"),
    "Paris FC": ("Paris", "FR"),
    # Eredivisie
    "Ajax": ("Amsterdam", "NL"),
    "PSV Eindhoven": ("Eindhoven", "NL"),
    "PSV": ("Eindhoven", "NL"),
    "Feyenoord": ("Rotterdam", "NL"),
    "AZ": ("Alkmaar", "NL"),
    "FC Utrecht": ("Utrecht", "NL"),
    "FC Twente Enschede": ("Enschede", "NL"),
    "FC Twente": ("Enschede", "NL"),
    "Vitesse": ("Arnhem", "NL"),
    "Go Ahead Eagles": ("Deventer", "NL"),
    "NAC Breda": ("Breda", "NL"),
    "Sparta Rotterdam": ("Rotterdam", "NL"),
}

# Enkel in-memory cache: (city, date_str) → result
_weather_cache: dict[str, dict] = {}
_CACHE_TTL_SEC = 3 * 3600  # 3 timer


def _find_city(home_team: str) -> Optional[Tuple[str, str]]:
    """Slår opp by og landkode for et lag. Fuzzy match på delstreng."""
    # Eksakt match
    if home_team in TEAM_CITY_MAP:
        return TEAM_CITY_MAP[home_team]
    # Fuzzy: sjekk om noe nøkkelord matcher
    ht_lower = home_team.lower()
    for team, city in TEAM_CITY_MAP.items():
        if team.lower() in ht_lower or ht_lower in team.lower():
            return city
    return None


def _assess_conditions(temp_c: float, wind_ms: float, rain_3h_mm: float) -> Tuple[str, int]:
    """
    Klassifiserer spilleforhold.
    Returnerer (signal_type, atomic_points).
    Aldri negativt — kun additivt.
    """
    if wind_ms > 12.0 or rain_3h_mm > 5.0:
        return "WEATHER_ADVERSE", 0   # Additive: 0, ikke -1
    if 8.0 <= temp_c <= 22.0 and wind_ms < 5.0 and rain_3h_mm < 0.5:
        return "WEATHER_FAVORABLE", 1
    return "WEATHER_NEUTRAL", 0


class WeatherSignal:
    """
    Async context manager for weather signal.
    Bruk: async with WeatherSignal(api_key) as ws:
              result = await ws.get_signal(home_team, kickoff_dt)
    """

    def __init__(self, api_key: str = None):
        self._api_key = api_key or ""
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(timeout=6)
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()

    async def get_signal(self, home_team: str, kickoff_dt: datetime) -> dict:
        """
        Henter værsignal for hjemmelaget ved kickoff.
        Returnerer alltid et resultat — feiler aldri stille.
        """
        if not self._api_key:
            return {
                "signal": "WEATHER_UNAVAILABLE",
                "atomic_points": 0,
                "reason": "Mangler OPENWEATHER_API_KEY",
            }

        city_info = _find_city(home_team)
        if not city_info:
            return {
                "signal": "WEATHER_CITY_UNKNOWN",
                "atomic_points": 0,
                "home_team": home_team,
            }

        city, country = city_info
        cache_key = f"{city}_{kickoff_dt.date()}"

        # Cache-sjekk
        cached = _weather_cache.get(cache_key)
        if cached and time.time() - cached.get("_ts", 0) < _CACHE_TTL_SEC:
            result = {k: v for k, v in cached.items() if k != "_ts"}
            result["from_cache"] = True
            return result

        try:
            resp = await self._client.get(
                "https://api.openweathermap.org/data/2.5/forecast",
                params={
                    "q": f"{city},{country}",
                    "appid": self._api_key,
                    "units": "metric",
                    "cnt": 16,  # 48 timer = 16 × 3h slots
                },
            )
            if resp.status_code == 401:
                return {"signal": "WEATHER_AUTH_FAIL", "atomic_points": 0}
            if resp.status_code != 200:
                return {"signal": "WEATHER_API_ERROR", "atomic_points": 0, "http_status": resp.status_code}

            data = resp.json()
            slots = data.get("list", [])
            if not slots:
                return {"signal": "WEATHER_NO_DATA", "atomic_points": 0}

            # Finn nærmeste forecast-slot til kickoff
            ko_ts = kickoff_dt.timestamp()
            best_slot = min(slots, key=lambda s: abs(s["dt"] - ko_ts))

            temp_c    = best_slot["main"]["temp"]
            wind_ms   = best_slot["wind"]["speed"]
            rain_mm   = best_slot.get("rain", {}).get("3h", 0.0)
            weather_desc = best_slot["weather"][0]["description"] if best_slot.get("weather") else "?"

            signal_type, atomic_pts = _assess_conditions(temp_c, wind_ms, rain_mm)

            result = {
                "signal": signal_type,
                "atomic_points": atomic_pts,
                "city": city,
                "temperature_c": round(temp_c, 1),
                "wind_ms": round(wind_ms, 1),
                "rain_3h_mm": round(rain_mm, 2),
                "description": weather_desc,
                "forecast_time": datetime.fromtimestamp(best_slot["dt"], tz=timezone.utc).isoformat(),
                "from_cache": False,
            }
            # Lagre i cache
            _weather_cache[cache_key] = {**result, "_ts": time.time()}
            return result

        except httpx.TimeoutException:
            return {"signal": "WEATHER_TIMEOUT", "atomic_points": 0}
        except Exception as e:
            logger.warning(f"[WeatherSignal] Feil: {e}")
            return {"signal": "WEATHER_ERROR", "atomic_points": 0, "error": str(e)[:100]}
