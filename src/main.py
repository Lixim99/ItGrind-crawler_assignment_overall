import asyncio
from time import perf_counter

from .models import AsyncCrawler


async def main():
    crawler = AsyncCrawler(max_concurrent=5)

    urls = [
        "https://example.com",
        "https://httpbin.org/delay/1",
        "https://httpbin.org/delay/2",
        "https://test.com",
        "https://apple.ru",
    ]
    try:
        started = perf_counter()
        results = await crawler.fetch_urls(urls)
        elapsed = perf_counter() - started

        n_start = perf_counter()
        await crawler.fetch_urls_sequentially(urls)
        n_elapsed = perf_counter() - n_start

        print(f"Время выполнения конкурентностью: {elapsed:.2f} сек.")
        print(f"Время выполнения послдеовательно: {n_elapsed:.2f} сек.")
    finally:
        await crawler.close()

    print(f"Загружено {len(results)} страниц")

asyncio.run(main())
