from __future__ import annotations

import asyncio
import unittest
from dataclasses import dataclass
from time import perf_counter
from unittest.mock import patch

import aiohttp

from src.exception import NetworkError, PermanentError, TransientError
from src.models import AsyncCrawler
from src.retry_strategy import RetryStrategy


@dataclass(frozen=True)
class Route:
    body: str = ""
    status: int = 200
    delay: float = 0
    error: BaseException | None = None


class FakeResponse:
    def __init__(self, url: str, route: Route) -> None:
        self.url = url
        self.status = route.status
        self.content_type = "text/html"
        self._body = route.body

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise aiohttp.ClientResponseError(
                request_info=None,
                history=(),
                status=self.status,
                message="HTTP error",
            )

    async def text(self) -> str:
        return self._body


class FakeRequestContextManager:
    def __init__(self, session: FakeSession, url: str, route: Route) -> None:
        self._session = session
        self._url = url
        self._route = route
        self._entered = False

    async def __aenter__(self) -> FakeResponse:
        self._entered = True
        self._session.active_requests += 1
        self._session.peak_requests = max(
            self._session.peak_requests,
            self._session.active_requests,
        )

        try:
            await asyncio.sleep(self._route.delay)
            if self._route.error is not None:
                raise self._route.error
        except BaseException:
            self._session.active_requests -= 1
            self._entered = False
            raise

        return FakeResponse(self._url, self._route)

    async def __aexit__(self, *args: object) -> None:
        if self._entered:
            self._session.active_requests -= 1
            self._entered = False


class FakeSession:
    def __init__(self, routes: dict[str, Route]) -> None:
        self.routes = routes
        self.active_requests = 0
        self.peak_requests = 0
        self.closed = False

    def get(
        self,
        url: str,
        *,
        timeout: aiohttp.ClientTimeout | None = None,
    ) -> FakeRequestContextManager:
        return FakeRequestContextManager(self, url, self.routes[url])

    async def close(self) -> None:
        self.closed = True


class AsyncCrawlerTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def make_crawler(
        session: FakeSession,
        *,
        max_concurrent: int = 10,
    ) -> AsyncCrawler:
        with patch("src.models.aiohttp.ClientSession", return_value=session):
            crawler = AsyncCrawler(
                max_concurrent=max_concurrent,
                requests_per_second=1_000_000,
                min_delay=0,
                retry_strategy=RetryStrategy(max_retries=0),
            )

        return crawler

    async def test_client_session_uses_configured_timeouts(self) -> None:
        session = FakeSession({})

        with patch(
            "src.models.aiohttp.ClientSession",
            return_value=session,
        ) as session_constructor:
            crawler = AsyncCrawler(
                connect_timeout=1.5,
                read_timeout=2.5,
                total_timeout=7.5,
            )

        timeout = session_constructor.call_args.kwargs["timeout"]
        self.assertEqual(timeout.connect, 1.5)
        self.assertEqual(timeout.sock_read, 2.5)
        self.assertEqual(timeout.total, 7.5)
        await crawler.close()

    async def test_fetch_url_returns_complete_page_and_logs_success(self) -> None:
        url = "https://example.test/valid"
        body = "<html><body>Complete page</body></html>"
        session = FakeSession({url: Route(body=body)})
        crawler = self.make_crawler(session)

        with self.assertLogs("crawler", level="INFO") as logs:
            result = await crawler.fetch_url(url)

        self.assertEqual(result, body)
        self.assertIn(f"Начало загрузки: {url}", "\n".join(logs.output))
        self.assertIn("HTTP 200", "\n".join(logs.output))

    async def test_fetch_url_handles_http_error(self) -> None:
        url = "https://example.test/missing"
        session = FakeSession({url: Route(status=404)})
        crawler = self.make_crawler(session)

        with (
            self.assertLogs("crawler", level="ERROR") as logs,
            self.assertRaises(PermanentError) as raised,
        ):
            await crawler.fetch_url(url)

        self.assertEqual(raised.exception.status, 404)
        message = "\n".join(logs.output)
        self.assertIn(url, message)
        self.assertIn("PermanentError", message)
        self.assertIn("404", message)

    async def test_fetch_url_logs_transient_http_error(self) -> None:
        url = "https://example.test/unavailable"
        session = FakeSession({url: Route(status=503)})
        crawler = self.make_crawler(session)

        with (
            self.assertLogs("crawler", level="ERROR") as logs,
            self.assertRaises(TransientError) as raised,
        ):
            await crawler.fetch_url(url)

        self.assertEqual(raised.exception.status, 503)
        message = "\n".join(logs.output)
        self.assertIn(url, message)
        self.assertIn("TransientError", message)
        self.assertIn("503", message)

    async def test_fetch_url_handles_timeout(self) -> None:
        url = "https://example.test/slow"
        session = FakeSession({url: Route(error=asyncio.TimeoutError())})
        crawler = self.make_crawler(session)

        with (
            self.assertLogs("crawler", level="ERROR") as logs,
            self.assertRaises(TransientError),
        ):
            await crawler.fetch_url(url)

        self.assertIn(url, "\n".join(logs.output))
        self.assertIn("TimeoutError", "\n".join(logs.output))

    async def test_fetch_url_handles_nonexistent_host(self) -> None:
        url = "https://host-does-not-exist.test"
        error = aiohttp.ClientConnectionError("Host is unavailable")
        session = FakeSession({url: Route(error=error)})
        crawler = self.make_crawler(session)

        with (
            self.assertLogs("crawler", level="ERROR") as logs,
            self.assertRaises(NetworkError),
        ):
            await crawler.fetch_url(url)

        self.assertIn(url, "\n".join(logs.output))
        self.assertIn("ClientConnectionError", "\n".join(logs.output))

    async def test_fetch_urls_returns_mapping_and_respects_limit(self) -> None:
        urls = [f"https://example.test/{number}" for number in range(5)]
        routes = {
            url: Route(body=f"page-{number}", delay=0.02)
            for number, url in enumerate(urls)
        }
        session = FakeSession(routes)
        crawler = self.make_crawler(session, max_concurrent=2)

        with self.assertLogs("crawler", level="INFO"):
            results = await crawler.fetch_urls(urls)

        self.assertEqual(
            results,
            {url: f"page-{number}" for number, url in enumerate(urls)},
        )
        self.assertEqual(session.peak_requests, 2)

    async def test_fetch_urls_keeps_successes_when_some_requests_fail(
        self,
    ) -> None:
        success_url = "https://example.test/valid"
        missing_url = "https://example.test/missing"
        timeout_url = "https://example.test/slow"
        session = FakeSession({
            success_url: Route(body="valid page"),
            missing_url: Route(status=404),
            timeout_url: Route(error=asyncio.TimeoutError()),
        })
        crawler = self.make_crawler(session, max_concurrent=3)

        with self.assertLogs("crawler", level="ERROR") as logs:
            results = await crawler.fetch_urls([
                success_url,
                missing_url,
                timeout_url,
            ])

        self.assertEqual(results, {success_url: "valid page"})
        self.assertIn(missing_url, "\n".join(logs.output))
        self.assertIn(timeout_url, "\n".join(logs.output))

    async def test_sequential_fetch_returns_mapping(self) -> None:
        urls = [f"https://example.test/{number}" for number in range(2)]
        routes = {
            url: Route(body=f"page-{number}")
            for number, url in enumerate(urls)
        }
        session = FakeSession(routes)
        crawler = self.make_crawler(session)

        with self.assertLogs("crawler", level="INFO"):
            results = await crawler.fetch_urls_sequentially(urls)

        self.assertEqual(
            results,
            {url: f"page-{number}" for number, url in enumerate(urls)},
        )

    async def test_concurrent_fetch_is_faster_than_sequential(self) -> None:
        urls = [f"https://example.test/delay/{number}" for number in range(4)]
        routes = {url: Route(body="page", delay=0.04) for url in urls}
        session = FakeSession(routes)
        crawler = self.make_crawler(session, max_concurrent=4)

        with self.assertLogs("crawler", level="INFO"):
            started = perf_counter()
            await crawler.fetch_urls_sequentially(urls)
            sequential_time = perf_counter() - started

            started = perf_counter()
            await crawler.fetch_urls(urls)
            concurrent_time = perf_counter() - started

        self.assertLess(concurrent_time, sequential_time * 0.6)

    async def test_close_closes_session(self) -> None:
        session = FakeSession({})
        crawler = self.make_crawler(session)

        await crawler.close()

        self.assertTrue(session.closed)


if __name__ == "__main__":
    unittest.main()
