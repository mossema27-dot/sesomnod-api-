import os
import httpx
import logging

MIROFISH_BASE_URL = os.getenv(
    "MIROFISH_BASE_URL",
    "https://mirofish-service-production.up.railway.app"
)
logger = logging.getLogger(__name__)

async def mirofish_track_pick(
    pick_id: str, match: str, home_team: str,
    away_team: str, our_odds: float, kickoff: str,
    edge_at_pick: float, market: str = "h2h"
) -> dict | None:
    try:
        async with httpx.AsyncClient(timeout=8.0) as c:
            r = await c.post(f"{MIROFISH_BASE_URL}/track",
                json={"pick_id": pick_id, "match": match,
                      "home_team": home_team,
                      "away_team": away_team,
                      "our_odds": our_odds,
                      "kickoff": kickoff,
                      "edge_at_pick": edge_at_pick,
                      "market": market})
            if r.status_code == 200:
                logger.info(f"MiroFish tracked {pick_id}")
                return r.json()
            logger.warning(f"MiroFish /track HTTP {r.status_code}")
            return None
    except Exception as e:
        logger.error(f"MiroFish track feil: {e}")
        return None

async def mirofish_close_clv(
    pick_id: str, closing_odds: float, outcome: str
) -> dict | None:
    try:
        async with httpx.AsyncClient(timeout=8.0) as c:
            r = await c.post(f"{MIROFISH_BASE_URL}/close-clv",
                json={"pick_id": pick_id,
                      "closing_odds": closing_odds,
                      "outcome": outcome})
            if r.status_code == 200:
                result = r.json()
                logger.info(
                    f"MiroFish CLV {pick_id}: "
                    f"{result.get('clv_percent','?')}%")
                return result
            return None
    except Exception as e:
        logger.error(f"MiroFish close-clv feil: {e}")
        return None

async def mirofish_get_summary() -> dict | None:
    try:
        async with httpx.AsyncClient(timeout=8.0) as c:
            r = await c.get(f"{MIROFISH_BASE_URL}/summary")
            return r.json() if r.status_code == 200 else None
    except Exception as e:
        logger.error(f"MiroFish summary feil: {e}")
        return None
