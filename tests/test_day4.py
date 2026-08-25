from __future__ import annotations

import unittest
from dataclasses import dataclass
from time import perf_counter
from unittest.mock import AsyncMock, call, patch

import aiohttp

from src.exception import NetworkError
from src.limiter import RateLimiter
from src.models import AsyncCrawler
from src.retry_strategy import RetryStrategy
from src.robots import RobotsParser

DOMAIN = "https://example.test"
ROBOTS_URL = f"{DOMAIN}/robots.txt"
PRIVATE_URL = f"{DOMAIN}/private"


@dataclass(frozen=True)
class Route:
    body: str = ""
    status: int = 200
    error: BaseException | None = None


class FakeResponse:
    def __init__(self, route: Route) -> None:
        self.status = route.status
        self.content_type = "text/html"
        self._route = route

    async def __aenter__(self) -> FakeResponse:
        if self._route.error is not None:
            raise self._route.error

        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    async def text(self) -> str:
        return self._route.body

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise aiohttp.ClientResponseError(
                request_info=None,  # type: ignore[arg-type]
                history=(),
                status=self.status,
                message="HTTP error",
            )


class FakeSession:
    def __init__(self, routes: dict[str, Route]) -> None:
        self.routes = routes
        self.requested_urls: list[str] = []
        self.request_times: list[float] = []
        self.closed = False

    def get(
        self,
        url: str,
        *,
        timeout: aiohttp.ClientTimeout | None = None,
    ) -> FakeResponse:
        self.requested_urls.append(url)
        self.request_times.append(perf_counter())
        return FakeResponse(self.routes[url])

    async def close(self) -> None:
        self.closed = True


class RateLimiterTests(unittest.IsolatedAsyncioTestCase):
    async def test_rate_limit_for_one_domain(self) -> None:
        limiter = RateLimiter(requests_per_second=25)

        await limiter.acquire("example.test")
        started = perf_counter()
        await limiter.acquire("example.test")

        self.assertGreaterEqual(perf_counter() - started, 0.03)

    async def test_different_domains_have_independent_limits(self) -> None:
        limiter = RateLimiter(requests_per_second=20)

        await limiter.acquire("one.test")
        started = perf_counter()
        await limiter.acquire("two.test")

        self.assertLess(perf_counter() - started, 0.02)

    async def test_global_limit_applies_across_domains(self) -> None:
        limiter = RateLimiter(
            requests_per_second=25,
            per_domain=False,
        )

        await limiter.acquire("one.test")
        started = perf_counter()
        await limiter.acquire("two.test")

        self.assertGreaterEqual(perf_counter() - started, 0.03)


class RobotsParserTests(unittest.IsolatedAsyncioTestCase):
    async def test_parses_rules_delay_and_caches_robots(self) -> None:
        robots = """
        User-agent: MyBot
        Disallow: /private
        Crawl-delay: 2

        User-agent: *
        Allow: /
        """
        session = FakeSession({ROBOTS_URL: Route(body=robots)})
        parser = RobotsParser(session)  # type: ignore[arg-type]

        first = await parser.fetch_robots(f"{DOMAIN}/page")
        second = await parser.fetch_robots(f"{DOMAIN}/other")

        self.assertFalse(first["cached"])
        self.assertTrue(second["cached"])
        self.assertEqual(session.requested_urls, [ROBOTS_URL])
        self.assertTrue(parser.can_fetch(f"{DOMAIN}/public", "MyBot"))
        self.assertFalse(parser.can_fetch(PRIVATE_URL, "MyBot"))
        self.assertEqual(parser.get_crawl_delay("MyBot"), 2.0)


class AsyncCrawlerDay4Tests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def make_crawler(
        session: FakeSession,
        **kwargs: object,
    ) -> AsyncCrawler:
        options = {
            "max_concurrent": 2,
            "requests_per_second": 1_000_000,
            "min_delay": 0,
        }
        options.update(kwargs)

        with patch(
            "src.models.aiohttp.ClientSession",
            return_value=session,
        ):
            return AsyncCrawler(**options)  # type: ignore[arg-type]

    async def test_custom_user_agent_is_used_by_session(self) -> None:
        session = FakeSession({})

        with patch(
            "src.models.aiohttp.ClientSession",
            return_value=session,
        ) as session_constructor:
            crawler = AsyncCrawler(
                requests_per_second=10,
                min_delay=0,
                user_agent="MyBot/1.0",
            )

        self.addAsyncCleanup(crawler.close)

        self.assertEqual(
            session_constructor.call_args.kwargs["headers"],
            {"User-Agent": "MyBot/1.0"},
        )

    async def test_minimum_delay_is_respected_between_requests(self) -> None:
        first_url = f"{DOMAIN}/one"
        second_url = f"{DOMAIN}/two"
        session = FakeSession(
            {
                first_url: Route(body="one"),
                second_url: Route(body="two"),
            }
        )
        crawler = self.make_crawler(
            session,
            requests_per_second=1_000,
            min_delay=0.04,
        )
        self.addAsyncCleanup(crawler.close)

        with (
            patch("src.models.random.uniform", return_value=0),
            self.assertLogs("async_crawler", level="INFO"),
        ):
            await crawler.fetch_url(first_url)
            await crawler.fetch_url(second_url)

        request_gap = session.request_times[1] - session.request_times[0]
        self.assertGreaterEqual(request_gap, 0.03)

    async def test_crawl_delay_from_robots_is_applied(self) -> None:
        robots = """
        User-agent: MyBot
        Allow: /
        Crawl-delay: 2
        """
        session = FakeSession({ROBOTS_URL: Route(body=robots)})
        crawler = self.make_crawler(
            session,
            respect_robots=True,
            user_agent="MyBot",
        )
        self.addAsyncCleanup(crawler.close)
        crawler._rate_limiter.acquire = AsyncMock()
        robots_limiter = AsyncMock()

        with patch.object(
            crawler,
            "_get_robots_limiter",
            return_value=robots_limiter,
        ) as limiter_factory:
            await crawler._wait_before_request(f"{DOMAIN}/public")

        limiter_factory.assert_called_once_with("example.test", 2.0)
        robots_limiter.acquire.assert_awaited_once_with()
        self.assertEqual(session.requested_urls, [ROBOTS_URL])

    async def test_exponential_backoff_retries_transient_errors(self) -> None:
        url = f"{DOMAIN}/unstable"
        session = FakeSession(
            {
                url: Route(
                    error=aiohttp.ClientConnectionError("offline"),
                )
            }
        )
        crawler = self.make_crawler(session)
        crawler._wait_before_request = AsyncMock()
        self.addAsyncCleanup(crawler.close)
        strategy = RetryStrategy(
            max_retries=2,
            retry_on=[NetworkError],
        )

        with (
            patch(
                "src.retry_strategy.asyncio.sleep",
                new=AsyncMock(),
            ) as sleep,
            self.assertLogs("async_crawler", level="INFO"),
            self.assertRaises(NetworkError),
        ):
            await strategy.execute_with_retry(crawler.fetch_url, url)

        self.assertEqual(session.requested_urls, [url, url, url])
        self.assertEqual(sleep.await_args_list, [call(1.0), call(2.0)])

    async def test_crawl_blocks_disallowed_url_and_tracks_it(self) -> None:
        robots = """
        User-agent: MyBot
        Disallow: /private
        """
        session = FakeSession({ROBOTS_URL: Route(body=robots)})
        crawler = self.make_crawler(
            session,
            respect_robots=True,
            user_agent="MyBot",
        )
        self.addAsyncCleanup(crawler.close)

        with self.assertLogs("async_crawler", level="INFO") as logs:
            results = await crawler.crawl([PRIVATE_URL], max_pages=1)

        self.assertEqual(results, {})
        self.assertEqual(crawler.failed_urls, {})
        self.assertEqual(crawler._get_request_stats()["robots_blocked"], 1)
        self.assertEqual(session.requested_urls, [ROBOTS_URL])
        self.assertIn("robots blocked: 1", "\n".join(logs.output))

    async def test_request_statistics_track_rate_delay_and_blocks(self) -> None:
        session = FakeSession({})
        crawler = self.make_crawler(session)
        self.addAsyncCleanup(crawler.close)
        crawler._blocked_urls.add(PRIVATE_URL)

        with patch(
            "src.models.perf_counter",
            side_effect=[10.0, 10.25],
        ):
            crawler._record_request_start()
            crawler._record_request_start()

        stats = crawler._get_request_stats()

        self.assertEqual(stats["requests_per_second"], 2)
        self.assertEqual(stats["average_delay"], 0.25)
        self.assertEqual(stats["robots_blocked"], 1)


if __name__ == "__main__":
    unittest.main()
