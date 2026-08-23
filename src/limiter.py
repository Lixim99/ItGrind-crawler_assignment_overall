import asyncio
from time import monotonic


class RateLimiter:
    def __init__(
        self,
        requests_per_second: float = 1.0,
        per_domain: bool = True,
    ):
        self._interval = 1 / requests_per_second
        self._per_domain = per_domain

        self._last_request: dict[str, float] = {}
        self._global_last_request = 0.0

        self._domain_locks: dict[str, asyncio.Lock] = {}
        self._global_lock = asyncio.Lock()

    async def acquire(self, domain: str | None = None):
        if self._per_domain:
            if domain is None:
                raise ValueError(
                    "domain is required when per_domain=True"
                )

            lock = self._domain_locks.setdefault(
                domain,
                asyncio.Lock(),
            )

            async with lock:
                now = monotonic()

                last_request = self._last_request.get(
                    domain,
                    0.0,
                )

                wait_time = (
                    self._interval
                    - (now - last_request)
                )

                if wait_time > 0:
                    await asyncio.sleep(wait_time)

                self._last_request[domain] = monotonic()

        else:
            async with self._global_lock:
                now = monotonic()

                wait_time = (
                    self._interval
                    - (now - self._global_last_request)
                )

                if wait_time > 0:
                    await asyncio.sleep(wait_time)

                self._global_last_request = monotonic()
