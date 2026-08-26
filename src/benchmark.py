import asyncio
import json
import os
import tracemalloc
from collections.abc import Iterable
from time import perf_counter
from urllib.request import ProxyHandler, Request, build_opener

from .models import AsyncCrawler


DEFAULT_PAGE_COUNTS = (100, 500, 1000)
DIRECT_OPENER = build_opener(ProxyHandler({}))


def fetch_urls_sync(
    urls: list[str],
    timeout: float = 30.0,
) -> None:
    """Последовательно загрузить URL обычным блокирующим клиентом."""

    for url in urls:
        request = Request(
            url,
            headers={"User-Agent": "CrawlerBenchmark/1.0"},
        )

        with DIRECT_OPENER.open(request, timeout=timeout) as response:
            response.read()


async def compare_sync_async(
    urls: list[str],
) -> dict:
    """Сравнить блокирующую загрузку с параллельным AsyncCrawler."""

    started = perf_counter()
    await asyncio.to_thread(fetch_urls_sync, urls)
    sync_time = perf_counter() - started

    crawler = AsyncCrawler(
        max_concurrent=10,
        max_concurrent_per_domain=10,
        min_delay=0,
        requests_per_second=10_000,
    )

    try:
        started = perf_counter()
        await crawler.fetch_urls(urls)
        async_time = perf_counter() - started
    finally:
        await crawler.close()

    speedup = sync_time / async_time if async_time > 0 else 0.0

    return {
        "pages": len(urls),
        "sync_time": sync_time,
        # Оставлено для совместимости со старыми отчётами.
        "sequential_time": sync_time,
        "async_time": async_time,
        "speedup": speedup,
    }


async def fetch_with_workers(
    crawler: AsyncCrawler,
    urls: list[str],
    workers_count: int,
) -> None:
    queue: asyncio.Queue[str] = asyncio.Queue()

    for url in urls:
        queue.put_nowait(url)

    async def worker() -> None:
        while True:
            url = await queue.get()

            try:
                await crawler.fetch_url(url)
            finally:
                queue.task_done()

    workers = [
        asyncio.create_task(worker())
        for _ in range(workers_count)
    ]

    try:
        await queue.join()
    finally:
        for worker_task in workers:
            worker_task.cancel()

        await asyncio.gather(
            *workers,
            return_exceptions=True,
        )


async def benchmark_workers(
    urls: list[str],
    workers_count: int,
) -> dict:
    crawler = AsyncCrawler(
        max_concurrent=workers_count,
        max_concurrent_per_domain=workers_count,
        min_delay=0,
        requests_per_second=10_000,
    )

    tracemalloc.start()

    try:
        started = perf_counter()
        await fetch_with_workers(
            crawler,
            urls,
            workers_count,
        )
        elapsed = perf_counter() - started
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
        await crawler.close()

    pages = len(urls)

    return {
        "type": "workers",
        "pages": pages,
        "concurrency": workers_count,
        "elapsed": elapsed,
        "pages_per_second": pages / elapsed if elapsed > 0 else 0.0,
        "peak_memory_mb": peak / 1024 / 1024,
    }


async def benchmark_gather(
    urls: list[str],
    max_concurrent: int,
) -> dict:
    crawler = AsyncCrawler(
        max_concurrent=max_concurrent,
        max_concurrent_per_domain=max_concurrent,
        min_delay=0,
        requests_per_second=10_000,
    )

    tracemalloc.start()

    try:
        started = perf_counter()
        await crawler.fetch_urls(urls)
        elapsed = perf_counter() - started
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
        await crawler.close()

    pages = len(urls)

    return {
        "type": "gather",
        "pages": pages,
        "concurrency": max_concurrent,
        "elapsed": elapsed,
        "pages_per_second": pages / elapsed if elapsed > 0 else 0.0,
        "peak_memory_mb": peak / 1024 / 1024,
    }


async def benchmark_scalability(
    server_url: str,
    *,
    page_counts: Iterable[int] = DEFAULT_PAGE_COUNTS,
    max_concurrent: int = 50,
) -> list[dict]:
    """Измерить время и память для 100, 500 и 1000 страниц."""

    results = []

    for page_count in page_counts:
        if page_count <= 0:
            raise ValueError("page counts must be greater than zero")

        urls = [
            f"{server_url}/slow?request={number}"
            for number in range(page_count)
        ]

        gather_result = await benchmark_gather(
            urls,
            max_concurrent=max_concurrent,
        )
        workers_result = await benchmark_workers(
            urls,
            workers_count=max_concurrent,
        )

        results.append({
            "pages": page_count,
            "gather": gather_result,
            "workers": workers_result,
        })

    return results


async def benchmark_main() -> None:
    host = os.getenv(
        "SERVER_HOST",
        os.getenv("SERVIER_HOST", "127.0.0.1"),
    )
    port = os.getenv("SERVER_PORT", "8080")
    server_url = f"http://{host}:{port}"

    comparison_urls = [
        f"{server_url}/slow?comparison={number}"
        for number in range(20)
    ]
    sync_comparison = await compare_sync_async(comparison_urls)
    scalability = await benchmark_scalability(server_url)

    print(json.dumps(
        {
            "sync_vs_async": sync_comparison,
            "scalability": scalability,
        },
        ensure_ascii=False,
        indent=2,
    ))


if __name__ == "__main__":
    asyncio.run(benchmark_main())
