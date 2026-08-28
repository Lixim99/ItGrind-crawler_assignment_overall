from __future__ import annotations

import asyncio
import unittest
from dataclasses import dataclass
from unittest.mock import AsyncMock, call, patch

import aiohttp

from src.exception import (
    NetworkError,
    PermanentError,
    TransientError,
)
from src.models import AsyncCrawler
from src.retry_strategy import RetryStrategy


URL = "https://example.test/page"


@dataclass(frozen=True)
class Outcome:
    body: str = "<html>page</html>"
    status: int = 200
    error: BaseException | None = None


class FakeResponse:
    def __init__(self, outcome: Outcome) -> None:
        self.status = outcome.status
        self.content_type = "text/html"
        self._body = outcome.body
        self._error = outcome.error

    async def __aenter__(self) -> FakeResponse:
        if self._error is not None:
            raise self._error

        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    async def text(self) -> str:
        return self._body


class SequenceSession:
    def __init__(
        self,
        outcomes: dict[str, list[Outcome]],
    ) -> None:
        self._outcomes = {
            url: list(url_outcomes)
            for url, url_outcomes in outcomes.items()
        }
        self.requested_urls: list[str] = []
        self.timeouts: list[aiohttp.ClientTimeout | None] = []
        self.closed = False

    def get(
        self,
        url: str,
        *,
        timeout: aiohttp.ClientTimeout | None = None,
    ) -> FakeResponse:
        self.requested_urls.append(url)
        self.timeouts.append(timeout)
        return FakeResponse(self._outcomes[url].pop(0))

    async def close(self) -> None:
        self.closed = True


def parsed_page(url: str) -> dict[str, object]:
    return {
        "url": url,
        "title": "Page",
        "text": "page",
        "links": [],
        "metadata": {},
        "images": [],
        "headings": [],
        "tables": [],
        "lists": [],
        "text_length": 4,
        "links_count": 0,
        "images_count": 0,
    }


class RetryStrategyTests(unittest.IsolatedAsyncioTestCase):
    async def test_exponential_backoff(self) -> None:
        operation = AsyncMock(
            side_effect=[
                TransientError("temporary-1", status=503),
                TransientError("temporary-2", status=503),
                TransientError("temporary-3", status=503),
                "success",
            ]
        )
        strategy = RetryStrategy(
            max_retries=3,
            backoff_factor=2.0,
            retry_on=[TransientError],
        )

        with patch(
            "src.retry_strategy.asyncio.sleep",
            new=AsyncMock(),
        ) as sleep:
            result = await strategy.execute_with_retry(operation)

        self.assertEqual(result, "success")
        self.assertEqual(operation.await_count, 4)
        self.assertEqual(
            sleep.await_args_list,
            [call(1.0), call(2.0), call(4.0)],
        )

    async def test_permanent_error_is_not_retried(self) -> None:
        operation = AsyncMock(
            side_effect=PermanentError("HTTP 404", status=404)
        )
        strategy = RetryStrategy(
            max_retries=3,
            retry_on=[TransientError, NetworkError],
        )

        with (
            patch(
                "src.retry_strategy.asyncio.sleep",
                new=AsyncMock(),
            ) as sleep,
            self.assertRaises(PermanentError),
        ):
            await strategy.execute_with_retry(operation)

        self.assertEqual(operation.await_count, 1)
        sleep.assert_not_awaited()

    async def test_429_uses_longer_backoff(self) -> None:
        operation = AsyncMock(
            side_effect=[
                TransientError("HTTP 429", status=429),
                "success",
            ]
        )
        strategy = RetryStrategy(
            max_retries=1,
            backoff_factor=2.0,
            retry_on=[TransientError],
        )

        with patch(
            "src.retry_strategy.asyncio.sleep",
            new=AsyncMock(),
        ) as sleep:
            result = await strategy.execute_with_retry(operation)

        self.assertEqual(result, "success")
        sleep.assert_awaited_once_with(2.0)

    async def test_retry_statistics(self) -> None:
        strategy = RetryStrategy(
            max_retries=2,
            retry_on=[TransientError, NetworkError],
        )
        succeeds_after_retry = AsyncMock(
            side_effect=[TransientError("temporary"), "success"]
        )
        always_fails = AsyncMock(side_effect=NetworkError("offline"))

        with patch(
            "src.retry_strategy.asyncio.sleep",
            new=AsyncMock(),
        ):
            await strategy.execute_with_retry(succeeds_after_retry)

            with self.assertRaises(NetworkError):
                await strategy.execute_with_retry(always_fails)

        self.assertEqual(
            strategy.get_stats(),
            {
                "total_retries": 3,
                "successful_after_retry": 1,
                "failed_after_retries": 1,
                "average_retry_delay": 4 / 3,
                "errors_by_type": {
                    "TransientError": 1,
                    "NetworkError": 3,
                },
            },
        )


class AsyncCrawlerRetryTests(unittest.IsolatedAsyncioTestCase):
    def make_crawler(self, session: SequenceSession) -> AsyncCrawler:
        with patch(
            "src.models.aiohttp.ClientSession",
            return_value=session,
        ):
            crawler = AsyncCrawler(
                max_concurrent=1,
                max_depth=0,
                requests_per_second=1_000_000,
                min_delay=0,
                connect_timeout=1,
                read_timeout=2,
                total_timeout=3,
            )

        crawler._wait_before_request = AsyncMock()
        crawler._parser.parse_html = AsyncMock(
            side_effect=lambda _html, url: parsed_page(url)
        )
        self.addAsyncCleanup(crawler.close)
        return crawler

    async def test_timeout_is_retried_and_timeout_increases(self) -> None:
        session = SequenceSession(
            {
                URL: [
                    Outcome(error=asyncio.TimeoutError()),
                    Outcome(),
                ]
            }
        )
        crawler = self.make_crawler(session)

        with (
            patch(
                "src.retry_strategy.asyncio.sleep",
                new=AsyncMock(),
            ) as sleep,
            self.assertLogs("crawler", level="INFO"),
        ):
            results = await crawler.crawl([URL], max_pages=1)

        self.assertIn(URL, results)
        self.assertEqual(session.requested_urls, [URL, URL])
        self.assertEqual(
            [timeout.total for timeout in session.timeouts],
            [3, 6],
        )
        sleep.assert_awaited_once_with(1.0)
        self.assertEqual(
            crawler._retry_strategy.get_stats()[
                "successful_after_retry"
            ],
            1,
        )

    async def test_503_is_retried(self) -> None:
        session = SequenceSession(
            {
                URL: [
                    Outcome(status=503),
                    Outcome(status=200),
                ]
            }
        )
        crawler = self.make_crawler(session)

        with patch(
            "src.retry_strategy.asyncio.sleep",
            new=AsyncMock(),
        ) as sleep:
            results = await crawler.crawl([URL], max_pages=1)

        self.assertIn(URL, results)
        self.assertEqual(session.requested_urls, [URL, URL])
        sleep.assert_awaited_once_with(1.0)

    async def test_404_is_not_retried_and_is_saved(self) -> None:
        session = SequenceSession(
            {URL: [Outcome(status=404)]}
        )
        crawler = self.make_crawler(session)

        with patch(
            "src.retry_strategy.asyncio.sleep",
            new=AsyncMock(),
        ) as sleep:
            results = await crawler.crawl([URL], max_pages=1)

        self.assertEqual(results, {})
        self.assertEqual(session.requested_urls, [URL])
        sleep.assert_not_awaited()
        self.assertEqual(
            crawler.failed_urls[URL],
            {
                "type": "PermanentError",
                "message": "HTTP 404",
            },
        )
        stats = crawler.get_stats()
        self.assertEqual(stats["errors_by_type"], {"PermanentError": 1})
        self.assertEqual(stats["permanent_error_urls"], [URL])
        self.assertEqual(stats["retry_stats"]["total_retries"], 0)

    async def test_network_error_is_retried(self) -> None:
        session = SequenceSession(
            {
                URL: [
                    Outcome(
                        error=aiohttp.ClientConnectionError("offline")
                    ),
                    Outcome(),
                ]
            }
        )
        crawler = self.make_crawler(session)

        with patch(
            "src.retry_strategy.asyncio.sleep",
            new=AsyncMock(),
        ):
            results = await crawler.crawl([URL], max_pages=1)

        self.assertIn(URL, results)
        self.assertEqual(session.requested_urls, [URL, URL])
        stats = crawler.get_stats()
        self.assertEqual(stats["retry_stats"]["total_retries"], 1)
        self.assertEqual(
            stats["retry_stats"]["successful_after_retry"],
            1,
        )
        self.assertEqual(stats["retry_stats"]["average_retry_delay"], 1.0)
        self.assertEqual(stats["errors_by_type"], {"NetworkError": 1})

    async def test_parse_error_is_saved_without_fetch_retry(self) -> None:
        session = SequenceSession({URL: [Outcome()]})
        crawler = self.make_crawler(session)
        crawler._parser.parse_html = AsyncMock(
            side_effect=ValueError("broken html")
        )

        results = await crawler.crawl([URL], max_pages=1)

        self.assertEqual(results, {})
        self.assertEqual(session.requested_urls, [URL])
        self.assertEqual(
            crawler.failed_urls[URL]["type"],
            "ParseError",
        )
        self.assertEqual(
            crawler.get_stats()["errors_by_type"],
            {"ParseError": 1},
        )


if __name__ == "__main__":
    unittest.main()
