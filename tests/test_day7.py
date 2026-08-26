from __future__ import annotations

import json
import logging
import tempfile
import unittest
from pathlib import Path
from time import perf_counter
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp

from src.benchmark import benchmark_scalability, compare_sync_async
from src.cli import apply_cli_overrides, create_parser
from src.config import create_storage, load_config
from src.crawler import AdvancedCrawler
from src.main import DEMO_FUNCTIONS, run_demo
from src.models import AsyncCrawler
from src.queue import CrawlerQueue
from src.sitemap import SitemapParser
from src.stats import CrawlerStats
from src.storage import CSVStorage, JSONStorage, PostgreSQLStorage
from src.utils import crawler_logger, setup_logging


SITEMAP_URL = "https://example.test/sitemap.xml"
CHILD_SITEMAP_URL = "https://example.test/sitemap-pages.xml"
NESTED_SITEMAP_URL = "https://example.test/sitemap-nested.xml"
PAGE_ONE = "https://example.test/one"
PAGE_TWO = "https://example.test/two"


class FakeResponse:
    def __init__(
        self,
        body: str,
        *,
        status: int = 200,
        content_type: str = "application/xml",
    ) -> None:
        self._body = body
        self.status = status
        self.content_type = content_type

    async def __aenter__(self) -> FakeResponse:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    async def text(self) -> str:
        return self._body

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise aiohttp.ClientResponseError(
                request_info=None,  # type: ignore[arg-type]
                history=(),
                status=self.status,
                message="HTTP error",
            )


class FakeSession:
    def __init__(
        self,
        responses: dict[str, FakeResponse],
    ) -> None:
        self.responses = responses
        self.requested_urls: list[str] = []
        self.closed = False

    def get(
        self,
        url: str,
        **_kwargs: object,
    ) -> FakeResponse:
        self.requested_urls.append(url)
        return self.responses[url]

    async def close(self) -> None:
        self.closed = True


class SitemapParserTests(unittest.IsolatedAsyncioTestCase):
    async def test_parses_regular_sitemap(self) -> None:
        xml = f"""
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <url><loc>{PAGE_ONE}</loc></url>
          <url><loc>{PAGE_TWO}</loc></url>
        </urlset>
        """
        session = FakeSession(
            {SITEMAP_URL: FakeResponse(xml)}
        )
        parser = SitemapParser(session)  # type: ignore[arg-type]

        urls = await parser.fetch_sitemap(SITEMAP_URL)

        self.assertEqual(urls, [PAGE_ONE, PAGE_TWO])
        self.assertEqual(session.requested_urls, [SITEMAP_URL])

    async def test_recursively_parses_sitemap_index_and_uses_cache(
        self,
    ) -> None:
        root_xml = f"""
        <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <sitemap><loc>{CHILD_SITEMAP_URL}</loc></sitemap>
          <sitemap><loc>{NESTED_SITEMAP_URL}</loc></sitemap>
        </sitemapindex>
        """
        child_xml = f"""
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <url><loc>{PAGE_ONE}</loc></url>
        </urlset>
        """
        nested_xml = f"""
        <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <sitemap><loc>{CHILD_SITEMAP_URL}</loc></sitemap>
        </sitemapindex>
        """
        session = FakeSession(
            {
                SITEMAP_URL: FakeResponse(root_xml),
                CHILD_SITEMAP_URL: FakeResponse(child_xml),
                NESTED_SITEMAP_URL: FakeResponse(nested_xml),
            }
        )
        parser = SitemapParser(session)  # type: ignore[arg-type]

        urls = await parser.fetch_sitemap(SITEMAP_URL)

        self.assertEqual(urls, [PAGE_ONE])
        self.assertEqual(
            session.requested_urls,
            [SITEMAP_URL, CHILD_SITEMAP_URL, NESTED_SITEMAP_URL],
        )

    async def test_crawler_uses_sitemap_as_url_source(self) -> None:
        sitemap_xml = f"""
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <url><loc>{PAGE_ONE}</loc></url>
        </urlset>
        """
        session = FakeSession(
            {
                SITEMAP_URL: FakeResponse(sitemap_xml),
                PAGE_ONE: FakeResponse(
                    "<html>page</html>",
                    content_type="text/html",
                ),
            }
        )

        with patch(
            "src.models.aiohttp.ClientSession",
            return_value=session,
        ):
            crawler = AsyncCrawler(
                max_concurrent=1,
                max_depth=0,
                requests_per_second=1_000_000,
                min_delay=0,
            )

        self.addAsyncCleanup(crawler.close)
        crawler._wait_before_request = AsyncMock()
        crawler._parser.parse_html = AsyncMock(
            return_value={
                "url": PAGE_ONE,
                "title": "One",
                "text": "page",
                "links": [],
                "metadata": {},
            }
        )

        results = await crawler.crawl(
            start_urls=[],
            sitemap_urls=[SITEMAP_URL],
            max_pages=1,
        )

        self.assertEqual(set(results), {PAGE_ONE})
        self.assertEqual(
            session.requested_urls,
            [SITEMAP_URL, PAGE_ONE],
        )
        self.assertEqual(crawler.get_stats()["successful"], 1)


class CrawlerStatsTests(unittest.TestCase):
    def test_collects_extended_statistics(self) -> None:
        stats = CrawlerStats()

        with patch(
            "src.stats.perf_counter",
            side_effect=[10.0, 14.0],
        ):
            stats.start()
            stats.record_success("https://one.test/a", 200)
            stats.record_success("https://one.test/b", 201)
            stats.record_success("https://two.test/a", 200)
            stats.record_failure("https://one.test/missing", 404)
            stats.finish()

        result = stats.get_stats()

        self.assertEqual(result["total_pages"], 4)
        self.assertEqual(result["successful"], 3)
        self.assertEqual(result["failed"], 1)
        self.assertEqual(result["average_speed"], 1.0)
        self.assertEqual(result["elapsed_time"], 4.0)
        self.assertEqual(result["status_codes"], {200: 2, 201: 1, 404: 1})
        self.assertEqual(
            result["top_domains"],
            [("one.test", 3), ("two.test", 1)],
        )

    def test_exports_json_and_html_report(self) -> None:
        stats = CrawlerStats()
        stats.start()
        stats.record_success("https://example.test/page", 200)
        stats.finish()

        with (
            tempfile.TemporaryDirectory() as directory,
            patch(
                "src.constants.GLOBAL_UPLOAD_PATH",
                Path(directory),
            ),
        ):
            json_path = Path(directory) / "stats.json"
            html_path = Path(directory) / "report.html"

            stats.export_to_json("stats.json")
            stats.export_to_html_report("report.html")

            exported = json.loads(json_path.read_text(encoding="utf-8"))
            html = html_path.read_text(encoding="utf-8")

        self.assertEqual(exported["total_pages"], 1)
        self.assertEqual(exported["successful"], 1)
        self.assertIn("Crawler report", html)
        self.assertIn("Status codes", html)
        self.assertIn("Top domains", html)
        self.assertIn("bar-container", html)


class ConfigurationAndCLITests(unittest.TestCase):
    def test_loads_json_configuration(self) -> None:
        config = {
            "crawler": {"max_depth": 2},
            "crawl": {"start_urls": ["https://example.test"]},
            "storage": {"type": "json", "filename": "pages.jsonl"},
        }

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text(json.dumps(config), encoding="utf-8")

            loaded = load_config(str(path))

        self.assertEqual(loaded, config)

    def test_storage_factory_supports_all_configured_formats(self) -> None:
        with (
            patch("src.config.JSONStorage") as json_storage,
            patch("src.config.CSVStorage") as csv_storage,
            patch("src.config.PostgreSQLStorage") as postgres_storage,
        ):
            create_storage({"type": "json", "filename": "pages.jsonl"})
            create_storage({"type": "csv", "filename": "pages.csv"})
            create_storage({"type": "postgresql", "database": "crawler"})

        json_storage.assert_called_once_with("pages.jsonl")
        csv_storage.assert_called_once_with(
            "pages.csv",
            encoding="utf-8",
        )
        postgres_storage.assert_called_once_with(database="crawler")

    def test_storage_factory_passes_csv_encoding(self) -> None:
        with patch("src.config.CSVStorage") as csv_storage:
            create_storage({
                "type": "csv",
                "filename": "pages.csv",
                "encoding": "utf-8-sig",
            })

        csv_storage.assert_called_once_with(
            "pages.csv",
            encoding="utf-8-sig",
        )

    def test_cli_parses_all_required_arguments(self) -> None:
        parser = create_parser()

        args = parser.parse_args(
            [
                "--urls",
                "https://one.test",
                "https://two.test",
                "--max-pages",
                "100",
                "--max-depth",
                "3",
                "--output",
                "stats.json",
                "--config",
                "crawler.json",
                "--respect-robots",
                "--rate-limit",
                "2.5",
            ]
        )

        self.assertEqual(args.urls, ["https://one.test", "https://two.test"])
        self.assertEqual(args.max_pages, 100)
        self.assertEqual(args.max_depth, 3)
        self.assertEqual(args.output, "stats.json")
        self.assertEqual(args.config, "crawler.json")
        self.assertTrue(args.respect_robots)
        self.assertEqual(args.rate_limit, 2.5)

    def test_cli_overrides_configuration(self) -> None:
        config = {
            "crawler": {
                "max_depth": 1,
                "respect_robots": False,
                "requests_per_second": 1.0,
            },
            "crawl": {
                "start_urls": ["https://old.test"],
                "max_pages": 10,
            },
        }
        args = create_parser().parse_args(
            [
                "--urls",
                "https://new.test",
                "--max-pages",
                "25",
                "--max-depth",
                "4",
                "--respect-robots",
                "--rate-limit",
                "3",
            ]
        )

        result = apply_cli_overrides(config, args)

        self.assertEqual(result["crawl"]["start_urls"], ["https://new.test"])
        self.assertEqual(result["crawl"]["max_pages"], 25)
        self.assertEqual(result["crawler"]["max_depth"], 4)
        self.assertTrue(result["crawler"]["respect_robots"])
        self.assertEqual(result["crawler"]["requests_per_second"], 3.0)


class AdvancedCrawlerTests(unittest.IsolatedAsyncioTestCase):
    async def test_builds_from_config_and_delegates_all_operations(self) -> None:
        config = {
            "crawler": {"max_concurrent": 5, "max_depth": 2},
            "crawl": {
                "start_urls": ["https://example.test"],
                "max_pages": 10,
            },
            "storage": {"type": "json", "filename": "pages.jsonl"},
        }
        storage = MagicMock()
        inner = MagicMock()
        inner.crawl = AsyncMock(return_value={PAGE_ONE: {}})
        inner.close = AsyncMock()
        inner.get_stats.return_value = {"total_pages": 1}

        with (
            patch("src.crawler.create_storage", return_value=storage),
            patch("src.crawler.AsyncCrawler", return_value=inner) as constructor,
        ):
            crawler = AdvancedCrawler.from_config_data(config)
            results = await crawler.crawl()
            stats = crawler.get_stats()
            crawler.export_to_json("stats.json")
            crawler.export_to_html_report("report.html")
            await crawler.close()

        constructor.assert_called_once_with(
            max_concurrent=5,
            max_depth=2,
            storage=storage,
        )
        inner.crawl.assert_awaited_once_with(**config["crawl"])
        self.assertEqual(results, {PAGE_ONE: {}})
        self.assertEqual(stats, {"total_pages": 1})
        inner.export_to_json.assert_called_once_with("stats.json")
        inner.export_to_html_report.assert_called_once_with("report.html")
        inner.close.assert_awaited_once()


class LoggingAndMonitoringTests(unittest.TestCase):
    def test_structured_logging_uses_console_and_rotating_file(self) -> None:
        old_handlers = list(crawler_logger.handlers)

        with tempfile.TemporaryDirectory() as directory:
            log_path = Path(directory) / "crawler.log"

            try:
                setup_logging(str(log_path), level=logging.DEBUG)
                crawler_logger.debug("structured message")

                for handler in crawler_logger.handlers:
                    handler.flush()

                content = log_path.read_text(encoding="utf-8")
                handler_names = {
                    type(handler).__name__
                    for handler in crawler_logger.handlers
                }
            finally:
                for handler in crawler_logger.handlers:
                    handler.close()

                crawler_logger.handlers.clear()
                crawler_logger.handlers.extend(old_handlers)

        self.assertIn("StreamHandler", handler_names)
        self.assertIn("RotatingFileHandler", handler_names)
        self.assertIn("DEBUG", content)
        self.assertIn("structured message", content)

    def test_progress_contains_percent_speed_eta_and_active_tasks(self) -> None:
        session = FakeSession({})

        with patch(
            "src.models.aiohttp.ClientSession",
            return_value=session,
        ):
            crawler = AsyncCrawler(
                requests_per_second=1_000_000,
                min_delay=0,
            )

        crawler._processed_urls[PAGE_ONE] = {}
        crawler._queue = CrawlerQueue()
        crawler._queue.add_url(PAGE_TWO)
        crawler._active_tasks = 1

        with self.assertLogs("crawler", level="INFO") as logs:
            crawler._log_progress(
                started=perf_counter() - 1,
                max_pages=10,
            )

        message = "\n".join(logs.output)
        self.assertIn("%", message)
        self.assertIn("pages/sec", message)
        self.assertIn("ETA", message)
        self.assertIn("active: 1", message)


class PerformanceTests(unittest.IsolatedAsyncioTestCase):
    async def test_compares_synchronous_and_async_speed(self) -> None:
        crawler = MagicMock()
        crawler.fetch_urls = AsyncMock()
        crawler.close = AsyncMock()

        with (
            patch("src.benchmark.AsyncCrawler", return_value=crawler),
            patch(
                "src.benchmark.asyncio.to_thread",
                new=AsyncMock(),
            ) as to_thread,
            patch(
                "src.benchmark.perf_counter",
                side_effect=[0.0, 4.0, 4.0, 5.0],
            ),
        ):
            result = await compare_sync_async(
                ["https://example.test/one"]
            )

        self.assertEqual(result["pages"], 1)
        self.assertEqual(result["sync_time"], 4.0)
        self.assertEqual(result["sequential_time"], 4.0)
        self.assertEqual(result["async_time"], 1.0)
        self.assertEqual(result["speedup"], 4.0)
        to_thread.assert_awaited_once()
        crawler.fetch_urls.assert_awaited_once()
        crawler.close.assert_awaited_once()

    async def test_scalability_runs_100_500_and_1000_pages(self) -> None:
        async def gather_result(
            urls: list[str],
            max_concurrent: int,
        ) -> dict:
            return {
                "type": "gather",
                "pages": len(urls),
                "concurrency": max_concurrent,
            }

        async def workers_result(
            urls: list[str],
            workers_count: int,
        ) -> dict:
            return {
                "type": "workers",
                "pages": len(urls),
                "concurrency": workers_count,
            }

        with (
            patch(
                "src.benchmark.benchmark_gather",
                new=AsyncMock(side_effect=gather_result),
            ) as gather,
            patch(
                "src.benchmark.benchmark_workers",
                new=AsyncMock(side_effect=workers_result),
            ) as workers,
        ):
            results = await benchmark_scalability(
                "http://127.0.0.1:8080",
                max_concurrent=25,
            )

        self.assertEqual(
            [result["pages"] for result in results],
            [100, 500, 1000],
        )
        self.assertEqual(gather.await_count, 3)
        self.assertEqual(workers.await_count, 3)

        for expected_pages, await_call in zip(
            (100, 500, 1000),
            gather.await_args_list,
            strict=True,
        ):
            urls = await_call.args[0]
            self.assertEqual(len(urls), expected_pages)
            self.assertEqual(len(set(urls)), expected_pages)
            self.assertEqual(
                await_call.kwargs["max_concurrent"],
                25,
            )


class DemonstrationTests(unittest.IsolatedAsyncioTestCase):
    def test_all_seven_demo_functions_are_registered(self) -> None:
        self.assertEqual(set(DEMO_FUNCTIONS), set(range(1, 8)))

    async def test_demo_dispatcher_runs_selected_day(self) -> None:
        demo = AsyncMock()

        with patch.dict(DEMO_FUNCTIONS, {3: demo}, clear=True):
            await run_demo(3)

        demo.assert_awaited_once_with()

    async def test_demo_dispatcher_rejects_unknown_day(self) -> None:
        with self.assertRaisesRegex(ValueError, "between 1 and 7"):
            await run_demo(8)


if __name__ == "__main__":
    unittest.main()
