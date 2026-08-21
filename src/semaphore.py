import asyncio
from urllib.parse import urlparse


class SemaphoreManager:
    def __init__(
        self,
        max_tasks=10,
        max_tasks_per_domain=3
    ):
        self._global = asyncio.Semaphore(max_tasks)
        self._max_tasks_per_domain = max_tasks_per_domain

        self._domains: dict[str, asyncio.Semaphore] = {}

        self._active_tasks = 0

    def _get_domain_semaphore(
        self,
        url: str
    ) -> asyncio.Semaphore:
        domain = urlparse(url).netloc

        if domain not in self._domains:
            self._domains[domain] = asyncio.Semaphore(
                self._max_tasks_per_domain
            )

        return self._domains[domain]

    async def acquire(self, url: str) -> None:
        domain_semaphore = self._get_domain_semaphore(url)

        await self._global.acquire()

        try:
            await domain_semaphore.acquire()
        except:
            self._global.release()
            raise

        self._active_tasks += 1

    def release(self, url: str) -> None:
        domain_semaphore = self._get_domain_semaphore(url)

        domain_semaphore.release()
        self._global.release()

        self._active_tasks -= 1

    def get_stats(self) -> dict:
        return {
            'active_tasks': self._active_tasks
        }
