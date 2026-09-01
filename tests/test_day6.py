from __future__ import annotations

import asyncio
import csv
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, call, patch

import aiohttp

from src.exception import StorageError
from src.models import AsyncCrawler
from src.storage import (
    CSVStorage,
    DataStorage,
    JSONStorage,
    PostgreSQLStorage,
)


URL = "https://example.test/page"


def storage_record(
    *,
    url: str = URL,
    title: str = 'Заголовок, "тест"',
) -> dict[str, object]:
    return {
        "url": url,
        "title": title,
        "text": "Первая строка\nВторая строка",
        "links": ["https://example.test/one?x=1&y=2"],
        "metadata": {
            "description": "Описание с запятой, кавычками \"и Unicode\"",
        },
        "crawled_at": datetime(
            2026,
            8,
            25,
            12,
            30,
            tzinfo=timezone.utc,
        ),
        "status_code": 200,
        "content_type": "text/html",
    }


class TemporaryStorageTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self._temporary_directory.cleanup)

        upload_path = Path(self._temporary_directory.name)
        self._path_patch = patch(
            "src.constants.GLOBAL_UPLOAD_PATH",
            upload_path,
        )
        self._path_patch.start()
        self.addCleanup(self._path_patch.stop)


class DataStorageTests(unittest.TestCase):
    def test_base_storage_is_abstract(self) -> None:
        with self.assertRaises(TypeError):
            DataStorage()


class JSONStorageTests(TemporaryStorageTestCase):
    async def test_supports_formatted_json_output(self) -> None:
        storage = JSONStorage(
            "pages-pretty.json",
            formatted=True,
            indent=2,
        )
        storage._batch_size = 1
        first = storage_record()
        second = storage_record(
            url="https://example.test/second",
        )

        await storage.save(first)
        await storage.save(second)
        await storage.close()

        raw = storage._full_path.read_text(encoding="utf-8")
        parsed = json.loads(raw)
        saved = [row async for row in storage.read()]

        self.assertTrue(raw.startswith("[\n"))
        self.assertIn('\n  {\n    "url"', raw)
        self.assertEqual(len(parsed), 2)
        self.assertEqual(saved, parsed)
        self.assertEqual(parsed[0]["url"], first["url"])
        self.assertEqual(parsed[1]["url"], second["url"])

    async def test_saves_json_lines_and_preserves_data(self) -> None:
        storage = JSONStorage("pages.jsonl")
        storage._batch_size = 2
        first = storage_record()
        second = storage_record(
            url="https://example.test/second",
            title="Вторая страница",
        )

        await asyncio.gather(
            storage.save(first),
            storage.save(second),
        )
        await storage.close()

        saved = [row async for row in storage.read()]
        raw_lines = storage._full_path.read_text(
            encoding="utf-8"
        ).splitlines()

        self.assertEqual(len(raw_lines), 2)
        self.assertEqual(len(saved), 2)
        self.assertEqual(saved[0]["url"], first["url"])
        self.assertEqual(saved[0]["title"], first["title"])
        self.assertEqual(saved[0]["text"], first["text"])
        self.assertEqual(saved[0]["links"], first["links"])
        self.assertEqual(saved[0]["metadata"], first["metadata"])
        self.assertEqual(saved[0]["crawled_at"], str(first["crawled_at"]))
        self.assertEqual(saved[0]["status_code"], 200)
        self.assertEqual(saved[0]["content_type"], "text/html")
        self.assertEqual(
            storage.get_stats(),
            {
                "saved": 2,
                "flushes": 1,
                "write_errors": 0,
                "buffered": 0,
            },
        )

    async def test_close_flushes_incomplete_batch(self) -> None:
        storage = JSONStorage("partial.jsonl")
        storage._batch_size = 10

        await storage.save(storage_record())

        self.assertFalse(storage._full_path.exists())
        self.assertEqual(storage.get_stats()["buffered"], 1)

        await storage.close()

        self.assertTrue(storage._full_path.exists())
        self.assertEqual(storage.get_stats()["saved"], 1)
        self.assertEqual(storage.get_stats()["buffered"], 0)

    async def test_write_error_is_retried_with_backoff(self) -> None:
        storage = JSONStorage("retry.jsonl")
        await storage.save(storage_record())
        original_flush = storage._flush
        flush = AsyncMock(
            side_effect=[
                StorageError("first"),
                StorageError("second"),
                None,
            ]
        )

        with (
            patch.object(storage, "_flush", flush),
            patch(
                "src.storage.asyncio.sleep",
                new=AsyncMock(),
            ) as sleep,
        ):
            await storage.close()

        self.assertEqual(flush.await_count, 3)
        self.assertEqual(sleep.await_args_list, [call(1), call(2)])

        # Записываем оставшийся буфер настоящим методом для очистки.
        await original_flush()


class CSVStorageTests(TemporaryStorageTestCase):
    async def test_detects_headers_and_handles_special_characters(self) -> None:
        storage = CSVStorage("pages.csv")
        storage._batch_size = 2
        first = storage_record()
        second = storage_record(
            url="https://example.test/second",
            title="Простое название",
        )

        await storage.save(first)
        await storage.save(second)
        await storage.close()

        saved = await storage.read()

        with storage._full_path.open(
            encoding="utf-8",
            newline="",
        ) as file:
            rows = list(csv.reader(file))

        self.assertEqual(rows[0], list(first.keys()))
        self.assertEqual(len(rows), 3)
        self.assertEqual(saved[0]["title"], first["title"])
        self.assertEqual(saved[0]["text"], first["text"])
        self.assertEqual(json.loads(saved[0]["links"]), first["links"])
        self.assertEqual(
            json.loads(saved[0]["metadata"]),
            first["metadata"],
        )
        self.assertEqual(saved[0]["status_code"], "200")
        self.assertEqual(storage.get_stats()["saved"], 2)

    async def test_supports_custom_encoding(self) -> None:
        storage = CSVStorage(
            "pages-cp1251.csv",
            encoding="cp1251",
        )
        record = storage_record(title="Страница на русском")

        await storage.save(record)
        await storage.close()

        saved = await storage.read()
        decoded = storage._full_path.read_bytes().decode("cp1251")

        self.assertEqual(saved[0]["title"], record["title"])
        self.assertIn("Страница на русском", decoded)


class PostgreSQLStorageTests(unittest.IsolatedAsyncioTestCase):
    async def test_initializes_schema_batches_and_reads_records(self) -> None:
        pool = AsyncMock()
        record = storage_record()
        inserted: list[tuple] = []

        async def capture_batch(
            _query: str,
            records: list[tuple],
        ) -> None:
            inserted.extend(records)

        pool.executemany.side_effect = capture_batch
        pool.fetch.return_value = [record]
        create_pool = AsyncMock(return_value=pool)
        storage = PostgreSQLStorage("crawler_test")
        storage._batch_size = 2

        with patch(
            "src.storage.asyncpg.create_pool",
            create_pool,
        ):
            await storage.save(record)
            await storage.save(
                storage_record(url="https://example.test/second")
            )
            saved = await storage.read()
            await storage.close()

        create_pool.assert_awaited_once()
        self.assertEqual(pool.execute.await_count, 2)
        pool.executemany.assert_awaited_once()
        self.assertEqual(len(inserted), 2)
        self.assertEqual(inserted[0][0], URL)
        self.assertEqual(json.loads(inserted[0][3]), record["links"])
        self.assertEqual(json.loads(inserted[0][4]), record["metadata"])
        self.assertEqual(saved, [record])
        pool.close.assert_awaited_once()
        self.assertEqual(
            storage.get_stats(),
            {
                "saved": 2,
                "flushes": 1,
                "write_errors": 0,
                "buffered": 0,
            },
        )

    async def test_database_write_is_retried(self) -> None:
        pool = AsyncMock()
        pool.executemany.side_effect = [OSError("disk"), None]
        storage = PostgreSQLStorage("crawler_test")
        storage._pool = pool
        storage._batch_size = 1

        with patch(
            "src.storage.asyncio.sleep",
            new=AsyncMock(),
        ) as sleep:
            await storage.save(storage_record())

        self.assertEqual(pool.executemany.await_count, 2)
        sleep.assert_awaited_once_with(1)
        self.assertEqual(storage.get_stats()["saved"], 1)
        self.assertEqual(storage.get_stats()["write_errors"], 1)


class FakeResponse:
    status = 200
    content_type = "text/html"

    async def __aenter__(self) -> FakeResponse:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    async def text(self) -> str:
        return "<html>page</html>"


class FakeSession:
    def __init__(self) -> None:
        self.closed = False

    def get(
        self,
        url: str,
        *,
        timeout: aiohttp.ClientTimeout | None = None,
    ) -> FakeResponse:
        return FakeResponse()

    async def close(self) -> None:
        self.closed = True


class AsyncCrawlerStorageTests(unittest.IsolatedAsyncioTestCase):
    def make_crawler(
        self,
        storage: AsyncMock,
    ) -> tuple[AsyncCrawler, FakeSession]:
        session = FakeSession()

        with patch(
            "src.models.aiohttp.ClientSession",
            return_value=session,
        ):
            crawler = AsyncCrawler(
                max_concurrent=1,
                max_depth=0,
                requests_per_second=1_000_000,
                min_delay=0,
                storage=storage,
            )

        crawler._wait_before_request = AsyncMock()
        crawler._parser.parse_html = AsyncMock(
            return_value={
                "url": URL,
                "title": "Page title",
                "text": "Page text",
                "links": [],
                "metadata": {"description": "Description"},
            }
        )
        return crawler, session

    async def test_crawler_saves_standardized_page_and_closes_storage(
        self,
    ) -> None:
        storage = AsyncMock(spec=DataStorage)
        crawler, session = self.make_crawler(storage)

        results = await crawler.crawl([URL], max_pages=1)
        await crawler.close()

        self.assertIn(URL, results)
        storage.save.assert_awaited_once()
        saved = storage.save.await_args.args[0]
        self.assertEqual(
            set(saved),
            {
                "url",
                "title",
                "text",
                "links",
                "metadata",
                "crawled_at",
                "status_code",
                "content_type",
            },
        )
        self.assertEqual(saved["url"], URL)
        self.assertEqual(saved["title"], "Page title")
        self.assertEqual(saved["status_code"], 200)
        self.assertEqual(saved["content_type"], "text/html")
        self.assertIsInstance(saved["crawled_at"], datetime)
        storage.close.assert_awaited_once()
        self.assertTrue(session.closed)

    async def test_storage_error_does_not_stop_crawling(self) -> None:
        storage = AsyncMock(spec=DataStorage)
        storage.save.side_effect = StorageError("write failed")
        crawler, _session = self.make_crawler(storage)
        self.addAsyncCleanup(crawler.close)

        with self.assertLogs("crawler", level="ERROR") as logs:
            results = await crawler.crawl([URL], max_pages=1)

        self.assertIn(URL, results)
        self.assertEqual(crawler.failed_urls, {})
        storage.save.assert_awaited_once()
        messages = "\n".join(logs.output)
        self.assertIn(URL, messages)
        self.assertIn("StorageError", messages)

    async def test_final_storage_error_is_logged_without_crash(self) -> None:
        storage = AsyncMock(spec=DataStorage)
        storage.close.side_effect = StorageError("final flush failed")
        crawler, session = self.make_crawler(storage)

        await crawler.crawl([URL], max_pages=1)

        with self.assertLogs("crawler", level="ERROR") as logs:
            await crawler.close()

        self.assertTrue(session.closed)
        storage.close.assert_awaited_once()
        messages = "\n".join(logs.output)
        self.assertIn("Ошибка закрытия хранилища", messages)
        self.assertIn("StorageError", messages)
        self.assertIn("final flush failed", messages)


if __name__ == "__main__":
    unittest.main()
