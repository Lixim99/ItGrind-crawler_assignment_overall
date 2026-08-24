from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from src.models import AsyncCrawler
from src.queue import CrawlerQueue
from src.semaphore import SemaphoreManager


ROOT_URL = "https://example.test/"


def parsed_page(url: str, links: list[str]) -> dict[str, object]:
    return {
        "url": url,
        "title": url,
        "text": "page",
        "links": links,
        "metadata": {},
        "images": [],
        "headings": [],
        "tables": [],
        "lists": [],
        "text_length": 4,
        "links_count": len(links),
        "images_count": 0,
    }


class CrawlerQueueTests(unittest.IsolatedAsyncioTestCase):
    async def test_queue_returns_urls_by_priority(self) -> None:
        queue = CrawlerQueue()
        queue.add_url("https://example.test/low", priority=10)
        queue.add_url("https://example.test/high", priority=0)
        queue.add_url("https://example.test/medium", priority=5)

        received = []

        for _ in range(3):
            url = await asyncio.wait_for(queue.get_next(), timeout=0.1)
            received.append(url)
            queue.mark_processed(url)

        await asyncio.wait_for(queue.join(), timeout=0.1)

        self.assertEqual(
            received,
            [
                "https://example.test/high",
                "https://example.test/medium",
                "https://example.test/low",
            ],
        )
        self.assertEqual(
            queue.get_stats(),
            {
                "queued": 0,
                "in_progress": 0,
                "processed": 3,
                "failed": 0,
            },
        )

    async def test_queue_tracks_failed_urls(self) -> None:
        queue = CrawlerQueue()
        url = "https://example.test/failed"
        queue.add_url(url)

        received = await asyncio.wait_for(queue.get_next(), timeout=0.1)
        queue.mark_failed(received, "network error")
        await asyncio.wait_for(queue.join(), timeout=0.1)

        self.assertEqual(
            queue.get_stats(),
            {
                "queued": 0,
                "in_progress": 0,
                "processed": 0,
                "failed": 1,
            },
        )

    async def test_queue_does_not_add_duplicate_urls(self) -> None:
        queue = CrawlerQueue()
        url = "https://example.test/duplicate"

        queue.add_url(url)
        queue.add_url(url)

        self.assertEqual(queue.get_stats()["queued"], 1)

        received = await asyncio.wait_for(queue.get_next(), timeout=0.1)
        queue.mark_processed(received)
        await asyncio.wait_for(queue.join(), timeout=0.1)

        queue.add_url(url)

        self.assertEqual(queue.get_stats()["queued"], 0)
        self.assertEqual(queue.get_stats()["processed"], 1)


class SemaphoreManagerTests(unittest.IsolatedAsyncioTestCase):
    async def measure_peak(
        self,
        manager: SemaphoreManager,
        urls: list[str],
    ) -> int:
        active = 0
        peak = 0

        async def task(url: str) -> None:
            nonlocal active, peak

            await manager.acquire(url)
            try:
                active += 1
                peak = max(peak, active)
                self.assertEqual(
                    manager.get_stats()["active_tasks"],
                    active,
                )
                await asyncio.sleep(0.02)
            finally:
                active -= 1
                manager.release(url)

        await asyncio.gather(*(task(url) for url in urls))
        self.assertEqual(manager.get_stats()["active_tasks"], 0)
        return peak

    async def test_global_concurrency_limit(self) -> None:
        manager = SemaphoreManager(
            max_tasks=2,
            max_tasks_per_domain=2,
        )
        urls = [
            f"https://domain-{number}.test/page"
            for number in range(5)
        ]

        peak = await self.measure_peak(manager, urls)

        self.assertEqual(peak, 2)

    async def test_per_domain_concurrency_limit(self) -> None:
        manager = SemaphoreManager(
            max_tasks=5,
            max_tasks_per_domain=2,
        )
        urls = [
            f"https://example.test/page-{number}"
            for number in range(5)
        ]

        peak = await self.measure_peak(manager, urls)

        self.assertEqual(peak, 2)


class AsyncCrawlerDay3Tests(unittest.IsolatedAsyncioTestCase):
    def make_crawler(
        self,
        *,
        max_concurrent: int = 3,
        max_depth: int = 2,
    ) -> AsyncCrawler:
        session = AsyncMock()

        with patch(
            "src.models.aiohttp.ClientSession",
            return_value=session,
        ):
            crawler = AsyncCrawler(
                max_concurrent=max_concurrent,
                max_depth=max_depth,
            )

        self.addAsyncCleanup(crawler.close)
        return crawler

    async def test_crawler_uses_max_concurrent_as_global_limit(self) -> None:
        crawler = self.make_crawler(max_concurrent=2)
        active = 0
        peak = 0

        async def task(url: str) -> None:
            nonlocal active, peak

            await crawler._semaphore.acquire(url)
            try:
                active += 1
                peak = max(peak, active)
                await asyncio.sleep(0.02)
            finally:
                active -= 1
                crawler._semaphore.release(url)

        await asyncio.gather(
            *(
                task(f"https://domain-{number}.test/page")
                for number in range(5)
            )
        )

        self.assertEqual(peak, 2)

    @staticmethod
    def install_graph(
        crawler: AsyncCrawler,
        graph: dict[str, list[str]],
        *,
        failed_urls: set[str] | None = None,
    ) -> AsyncMock:
        failed_urls = failed_urls or set()

        async def fetch(
            url: str,
            timeout_multiplier: float = 1.0,
        ) -> str:
            return "" if url in failed_urls else "<html>page</html>"

        async def parse(_html: str, url: str) -> dict[str, object]:
            return parsed_page(url, graph.get(url, []))

        fetch_mock = AsyncMock(side_effect=fetch)
        crawler.fetch_url = fetch_mock
        crawler._parser.parse_html = AsyncMock(side_effect=parse)
        return fetch_mock

    async def test_crawl_respects_max_depth(self) -> None:
        crawler = self.make_crawler(max_depth=2)
        graph = {
            ROOT_URL: ["https://example.test/level-1"],
            "https://example.test/level-1": [
                "https://example.test/level-2"
            ],
            "https://example.test/level-2": [
                "https://example.test/level-3"
            ],
        }
        self.install_graph(crawler, graph)

        results = await asyncio.wait_for(
            crawler.crawl([ROOT_URL], max_pages=10),
            timeout=1,
        )

        self.assertEqual(
            set(results),
            {
                ROOT_URL,
                "https://example.test/level-1",
                "https://example.test/level-2",
            },
        )
        self.assertNotIn(
            "https://example.test/level-3",
            crawler.visited_urls,
        )
        self.assertEqual(crawler._url_depths[ROOT_URL], 0)
        self.assertEqual(
            crawler._url_depths["https://example.test/level-2"],
            2,
        )

    async def test_crawl_filters_external_urls(self) -> None:
        crawler = self.make_crawler(max_depth=1)
        inside_url = "https://example.test/inside"
        external_url = "https://external.test/page"
        graph = {ROOT_URL: [inside_url, external_url]}
        self.install_graph(crawler, graph)

        results = await crawler.crawl(
            [ROOT_URL],
            same_domain_only=True,
        )

        self.assertEqual(set(results), {ROOT_URL, inside_url})
        self.assertNotIn(external_url, crawler.visited_urls)

    async def test_crawl_applies_include_and_exclude_patterns(self) -> None:
        crawler = self.make_crawler(max_depth=1)
        allowed_url = "https://example.test/articles/one"
        excluded_url = "https://example.test/articles/private"
        not_included_url = "https://example.test/about"
        graph = {
            ROOT_URL: [
                allowed_url,
                excluded_url,
                not_included_url,
            ]
        }
        self.install_graph(crawler, graph)

        results = await crawler.crawl(
            [ROOT_URL],
            include_patterns=[r"/articles/"],
            exclude_patterns=[r"/private$"],
        )

        self.assertEqual(set(results), {ROOT_URL, allowed_url})

    async def test_crawl_does_not_visit_duplicate_urls(self) -> None:
        crawler = self.make_crawler(max_depth=2)
        child_url = "https://example.test/child"
        graph = {
            ROOT_URL: [child_url, child_url],
            child_url: [ROOT_URL, child_url],
        }
        fetch_mock = self.install_graph(crawler, graph)

        results = await crawler.crawl([ROOT_URL])

        self.assertEqual(set(results), {ROOT_URL, child_url})
        self.assertEqual(crawler.visited_urls, {ROOT_URL, child_url})
        self.assertEqual(fetch_mock.await_count, 2)

    async def test_crawl_respects_max_pages(self) -> None:
        crawler = self.make_crawler(max_depth=1)
        graph = {
            ROOT_URL: [
                f"https://example.test/page-{number}"
                for number in range(10)
            ]
        }
        self.install_graph(crawler, graph)

        results = await crawler.crawl([ROOT_URL], max_pages=3)

        self.assertEqual(len(results), 3)
        self.assertEqual(len(crawler.visited_urls), 3)

    async def test_crawl_tracks_failures_and_logs_progress(self) -> None:
        crawler = self.make_crawler(max_depth=1)
        failed_url = "https://example.test/failed"
        graph = {ROOT_URL: [failed_url]}
        self.install_graph(crawler, graph, failed_urls={failed_url})

        with self.assertLogs("async_crawler", level="INFO") as logs:
            results = await crawler.crawl([ROOT_URL])

        self.assertEqual(set(results), {ROOT_URL})
        self.assertIn(failed_url, crawler.failed_urls)
        self.assertNotIn(failed_url, crawler.processed_urls)
        self.assertEqual(
            crawler._queue.get_stats(),
            {
                "queued": 0,
                "in_progress": 0,
                "processed": 1,
                "failed": 1,
            },
        )
        messages = "\n".join(logs.output)
        self.assertIn("Прогресс", messages)
        self.assertIn("скорость", messages)


if __name__ == "__main__":
    unittest.main()
