import asyncio


class CrawlerQueue:
    def __init__(self):
        self._queue = asyncio.PriorityQueue()
        self._urls: set[str] = set()
        self._in_progress: set[str] = set()
        self._processed: set[str] = set()
        self._failed: dict[str, str] = {}
        self._counter = 0

    async def join(self) -> None:
        await self._queue.join()

    def add_url(
        self,
        url: str,
        priority: int = 0
    ) -> None:
        if (
            url in self._urls
            or url in self._in_progress
            or url in self._processed
            or url in self._failed
        ):
            return

        self._counter += 1
        self._queue.put_nowait((priority, self._counter, url))
        self._urls.add(url)

    async def get_next(self) -> str | None:
        _, _, url = await self._queue.get()

        self._urls.remove(url)
        self._in_progress.add(url)

        return url

    def mark_processed(
        self,
        url: str
    ) -> None:
        self._in_progress.discard(url)
        self._processed.add(url)
        self._queue.task_done()

    def mark_failed(
        self,
        url: str,
        error: str
    ) -> None:
        self._in_progress.discard(url)
        self._failed[url] = error
        self._queue.task_done()

    def get_stats(self) -> dict:
        return {
            "queued": self._queue.qsize(),
            "in_progress": len(self._in_progress),
            "processed": len(self._processed),
            "failed": len(self._failed)
        }
