import asyncio
from datetime import datetime
from enum import Enum
from typing import Optional
import functools


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """
    Circuit breaker for eksterne APIer.
    3 failures → OPEN i 5 minutter.
    MERK: State er in-memory.
    Ved Railway restart = reset.
    (Redis-persistering i v10.2)
    """
    def __init__(
        self,
        failure_threshold: int = 3,
        recovery_timeout: int = 300,
        name: str = "",
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.name = name
        self.failures = 0
        self.state = CircuitState.CLOSED
        self.last_failure: Optional[datetime] = None
        self.half_open_calls = 0
        self.half_open_max = 1
        self._lock = asyncio.Lock()

    def _should_reset(self) -> bool:
        if not self.last_failure:
            return True
        elapsed = (datetime.now() - self.last_failure).total_seconds()
        return elapsed >= self.recovery_timeout

    async def _on_success(self):
        async with self._lock:
            self.failures = 0
            self.state = CircuitState.CLOSED

    async def _on_failure(self):
        async with self._lock:
            self.failures += 1
            self.last_failure = datetime.now()
            if self.failures >= self.failure_threshold:
                self.state = CircuitState.OPEN

    def protect(self, fallback: dict):
        """
        Decorator som beskytter async
        funksjoner med circuit breaker.
        """
        def decorator(func):
            @functools.wraps(func)
            async def wrapper(*args, **kwargs):
                async with self._lock:
                    if self.state == CircuitState.OPEN:
                        if self._should_reset():
                            self.state = CircuitState.HALF_OPEN
                            self.half_open_calls = 0
                        else:
                            return {
                                **fallback,
                                "circuit_state": "OPEN",
                                "error": f"{self.name} circuit open",
                            }

                    if self.state == CircuitState.HALF_OPEN:
                        if self.half_open_calls >= self.half_open_max:
                            return {
                                **fallback,
                                "circuit_state": "HALF_OPEN",
                            }
                        self.half_open_calls += 1

                try:
                    result = await func(*args, **kwargs)
                    await self._on_success()
                    return result
                except Exception as e:
                    await self._on_failure()
                    return {
                        **fallback,
                        "circuit_state": "FAILED",
                        "error": str(e)[:100],
                    }
            return wrapper
        return decorator


# Globale circuit breakers
referee_breaker = CircuitBreaker(
    failure_threshold=3,
    recovery_timeout=300,
    name="RefereeAPI",
)
weather_breaker = CircuitBreaker(
    failure_threshold=3,
    recovery_timeout=300,
    name="WeatherAPI",
)
