import asyncio
from time import perf_counter

from .models import AsyncCrawler
from .utils import setup_logging

setup_logging()


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
        # Day1
        print("====Day 1====\n")
        started = perf_counter()
        results = await crawler.fetch_urls(urls)
        elapsed = perf_counter() - started

        n_start = perf_counter()
        await crawler.fetch_urls_sequentially(urls)
        n_elapsed = perf_counter() - n_start

        print(f"Время выполнения конкурентностью: {elapsed:.2f} сек.")
        print(f"Время выполнения послдеовательно: {n_elapsed:.2f} сек.")

        # Day2
        print("====Day 2====\n")
        parsed_pages = await asyncio.gather(
            *(crawler.fetch_and_parse(url) for url in urls)
        )

        for page in parsed_pages:
            print({
                "url": page["url"],
                "title": page["title"],
                "text_length": page["text_length"],
                "links_count": page["links_count"],
                "images_count": page["images_count"],
            })

    finally:
        await crawler.close()

    print(f"Загружено {len(results)} страниц")

asyncio.run(main())
