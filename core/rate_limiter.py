import asyncio
import time


class RateLimiter:
    """
    Token bucket rate limiter.
    Thread-safe med asyncio.Lock.
    """
    def __init__(
        self,
        calls_per_minute: int,
        name: str = "",
    ):
        self.calls_per_minute = calls_per_minute
        self.interval = 60.0 / calls_per_minute
        self.last_call = 0.0
        self.lock = asyncio.Lock()
        self.name = name

    async def acquire(self):
        async with self.lock:
            now = time.monotonic()
            wait = self.interval - (now - self.last_call)
            if wait > 0:
                await asyncio.sleep(wait)
            self.last_call = time.monotonic()


# Globale instanser
odds_limiter = RateLimiter(
    calls_per_minute=30,
    name="TheOddsAPI",
)
football_limiter = RateLimiter(
    calls_per_minute=10,
    name="FootballData",
)
weather_limiter = RateLimiter(
    calls_per_minute=40,
    name="OpenWeather",
)
