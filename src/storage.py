import asyncio
import csv
import io
import json
import os
from abc import ABC, abstractmethod
from pathlib import Path

import aiofiles
import asyncpg

from .constants import GLOBAL_UPLOAD_PATH
from .exception import StorageError


class DataStorage(ABC):
    @abstractmethod
    async def save(self, data: dict) -> None:
        ...

    @abstractmethod
    async def close(self) -> None:
        ...


class JSONStorage(DataStorage):
    def __init__(self, file: str):
        self._full_path = (
            Path.cwd()
            / GLOBAL_UPLOAD_PATH
            / file
        )

        self._full_path.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        self._saved_count = 0
        self._flush_count = 0
        self._write_errors = 0
        self._batch_size = 10
        self._buffer: list[str] = []
        self._lock = asyncio.Lock()

    async def _flush_with_retry(self) -> None:
        for attempt in range(3):
            try:
                await self._flush()
                return

            except StorageError:
                if attempt == 2:
                    raise

                await asyncio.sleep(2 ** attempt)

    async def _flush(self):
        if not self._buffer:
            return

        content = "".join(self._buffer)

        count = len(self._buffer)

        try:
            async with aiofiles.open(
                file=self._full_path,
                mode="a",
                encoding="utf-8",
            ) as file:
                await file.write(content)
        except OSError as error:
            self._write_errors += 1

            raise StorageError(
                f"JSON write error: {error}"
            ) from error

        self._saved_count += count
        self._flush_count += 1

        self._buffer.clear()

    async def save(self, data: dict) -> None:
        row = (
            json.dumps(
                data,
                ensure_ascii=False,
                default=str,
            )
            + "\n"
        )

        async with self._lock:
            self._buffer.append(row)

            if len(self._buffer) >= self._batch_size:
                await self._flush_with_retry()

    async def close(self) -> None:
        async with self._lock:
            await self._flush_with_retry()

    async def read(self):
        async with aiofiles.open(
            self._full_path,
            mode="r",
            encoding="utf-8",
        ) as file:
            async for line in file:
                line = line.strip()

                if line:
                    yield json.loads(line)

    def get_stats(self) -> dict:
        return {
            "saved": self._saved_count,
            "flushes": self._flush_count,
            "write_errors": self._write_errors,
            "buffered": len(self._buffer),
        }


class CSVStorage(DataStorage):
    def __init__(
        self,
        file: str,
        encoding: str = "utf-8",
    ):
        self._full_path = (
            Path.cwd()
            / GLOBAL_UPLOAD_PATH
            / file
        )

        self._full_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )
        self._saved_count = 0
        self._flush_count = 0
        self._write_errors = 0
        self._encoding = encoding
        self._headers: list[str] | None = None
        self._batch_size = 10
        self._buffer: list[str] = []
        self._lock = asyncio.Lock()

    async def _flush_with_retry(self) -> None:
        for attempt in range(3):
            try:
                await self._flush()
                return

            except StorageError:
                if attempt == 2:
                    raise

                await asyncio.sleep(2 ** attempt)

    @staticmethod
    def _make_csv_row(
        values: list,
    ) -> str:
        buffer = io.StringIO()

        writer = csv.writer(buffer)

        writer.writerow(values)

        return buffer.getvalue()

    async def _flush(self):
        if not self._buffer:
            return

        content = "".join(self._buffer)

        count = len(self._buffer)

        try:
            async with aiofiles.open(
                file=self._full_path,
                mode="a",
                encoding=self._encoding,
            ) as file:
                await file.write(content)
        except OSError as error:
            self._write_errors += 1

            raise StorageError(
                f"CSV write error: {error}"
            ) from error

        self._saved_count += count
        self._flush_count += 1

        self._buffer.clear()

    async def save(self, data: dict) -> None:
        async with self._lock:
            if self._headers is None:
                self._headers = list(data.keys())

                header = self._make_csv_row(
                    self._headers
                )

                try:
                    async with aiofiles.open(
                        self._full_path,
                        mode="a",
                        encoding=self._encoding,
                    ) as file:
                        await file.write(header)
                except OSError as error:
                    self._write_errors += 1

                    raise StorageError(
                        f"CSV header write error: {error}"
                    ) from error

            values = []

            for header in self._headers:
                value = data.get(header)

                if isinstance(value, (dict, list)):
                    value = json.dumps(
                        value,
                        ensure_ascii=False,
                        default=str,
                    )

                values.append(value)

            row = self._make_csv_row(values)

            self._buffer.append(row)

            if len(self._buffer) >= self._batch_size:
                await self._flush_with_retry()

    async def read(self) -> list[dict]:
        async with aiofiles.open(
            self._full_path,
            mode="r",
            encoding=self._encoding,
        ) as file:
            content = await file.read()

        buffer = io.StringIO(content)

        reader = csv.DictReader(buffer)

        return list(reader)

    async def close(self) -> None:
        async with self._lock:
            await self._flush_with_retry()

    def get_stats(self) -> dict:
        return {
            "saved": self._saved_count,
            "flushes": self._flush_count,
            "write_errors": self._write_errors,
            "buffered": len(self._buffer),
        }


class PostgreSQLStorage(DataStorage):
    def __init__(
        self,
        database: str,
    ):
        if not database:
            raise ValueError("Database is empty")

        self._saved_count = 0
        self._flush_count = 0
        self._write_errors = 0
        self._database = database
        self._batch_size = 10
        self._pool: asyncpg.Pool | None = None
        self._buffer: list[tuple] = []
        self._lock = asyncio.Lock()

        self._host = os.getenv(
            "POSTGRES_HOST",
            "localhost",
        )
        self._port = int(
            os.getenv(
                "POSTGRES_PORT",
                "5432",
            )
        )
        self._user = os.getenv(
            "POSTGRES_USER",
            "postgres",
        )
        self._password = os.getenv(
            "POSTGRES_PASSWORD",
        )
        self._database = os.getenv(
            "POSTGRES_DB",
            self._database or "crawler",
        )

    async def _flush_with_retry(self) -> None:
        for attempt in range(3):
            try:
                await self._flush()
                return

            except StorageError:
                if attempt == 2:
                    raise

                await asyncio.sleep(2 ** attempt)

    async def _flush(self):
        if not self._buffer:
            return

        count = len(self._buffer)

        try:
            await self._pool.executemany(
                """
                INSERT INTO crawler_pages (
                    url,
                    title,
                    text,
                    links,
                    metadata,
                    crawled_at,
                    status_code,
                    content_type
                )
                VALUES (
                    $1, $2, $3, $4,
                    $5, $6, $7, $8
                )
                """,
                self._buffer,
            )

        except (
            asyncpg.PostgresError,
            OSError,
        ) as error:
            self._write_errors += 1

            raise StorageError(
                f"DB write error: {error}"
            ) from error

        self._saved_count += count
        self._flush_count += 1

        self._buffer.clear()

    async def init_db(self):
        self._pool = await asyncpg.create_pool(
            host=self._host,
            port=self._port,
            user=self._user,
            password=self._password,
            database=self._database,
        )

        await self._pool.execute("""
            CREATE TABLE IF NOT EXISTS crawler_pages (
                id BIGSERIAL PRIMARY KEY,
                url TEXT NOT NULL,
                title TEXT,
                text TEXT,
                links JSONB,
                metadata JSONB,
                crawled_at TIMESTAMPTZ NOT NULL,
                status_code INTEGER,
                content_type TEXT
            )
        """)

        await self._pool.execute("""
            CREATE INDEX IF NOT EXISTS idx_crawler_pages_url
            ON crawler_pages(url)
        """)

    async def save(self, data: dict) -> None:
        record = (
            data["url"],
            data["title"],
            data["text"],
            json.dumps(
                data["links"],
                ensure_ascii=False,
            ),
            json.dumps(
                data["metadata"],
                ensure_ascii=False,
            ),
            data["crawled_at"],
            data["status_code"],
            data["content_type"],
        )
        async with self._lock:
            if not self._pool:
                await self.init_db()

            self._buffer.append(record)

            if len(self._buffer) >= self._batch_size:
                await self._flush_with_retry()

    async def read(self) -> list[dict]:
        if not self._pool:
            await self.init_db()

        rows = await self._pool.fetch(
            """
            SELECT
                url,
                title,
                text,
                links,
                metadata,
                crawled_at,
                status_code,
                content_type
            FROM crawler_pages
            ORDER BY id
            """
        )

        return [
            dict(row)
            for row in rows
        ]

    async def close(self) -> None:
        async with self._lock:
            await self._flush_with_retry()

            if self._pool:
                await self._pool.close()
                self._pool = None

    def get_stats(self) -> dict:
        return {
            "saved": self._saved_count,
            "flushes": self._flush_count,
            "write_errors": self._write_errors,
            "buffered": len(self._buffer),
        }
