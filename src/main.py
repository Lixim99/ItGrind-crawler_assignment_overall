import asyncio
from time import perf_counter

from dotenv import load_dotenv

from .exception import NetworkError, TransientError
from .models import AsyncCrawler
from .retry_strategy import RetryStrategy
from .storage import CSVStorage, JSONStorage, PostgreSQLStorage
from .utils import setup_logging

load_dotenv()

setup_logging()


async def main():
    # crawler = AsyncCrawler(max_concurrent=5)

    urls = [
        # "https://example.com",
        # "https://httpbin.org/delay/1",
        # "https://httpbin.org/delay/2",
        "https://test.com",
        "https://apple.ru",
    ]
    try:
        # Day1
        # print("====Day 1====\n")
        # started = perf_counter()
        # results = await crawler.fetch_urls(urls)
        # elapsed = perf_counter() - started

        # n_start = perf_counter()
        # await crawler.fetch_urls_sequentially(urls)
        # n_elapsed = perf_counter() - n_start

        # print(f"Время выполнения конкурентностью: {elapsed:.2f} сек.")
        # print(f"Время выполнения последовательно: {n_elapsed:.2f} сек.")

        # # Day2
        # print("====Day 2====\n")
        # parsed_pages = await asyncio.gather(
        #     *(crawler.fetch_and_parse(url) for url in urls)
        # )

        # for page in parsed_pages:
        #     print({
        #         "url": page["url"],
        #         "title": page["title"],
        #         "headings": page["headings"],
        #         "links": page["links"],
        #         "text_length": page["text_length"],
        #         "links_count": page["links_count"],
        #         "images_count": page["images_count"],
        #     })

        # # Day3
        # print("====Day 3====\n")
        # crawler2 = AsyncCrawler(max_concurrent=5, max_depth=2)
        # results = await crawler2.crawl(
        #     start_urls=["https://apple.ru"],
        #     max_pages=50,
        #     same_domain_only=True
        # )
        # print(f"Обработано {len(results)} страниц")

        # Day4
        # print("====Day 4====\n")
        # crawler3 = AsyncCrawler(
        #     max_concurrent=5,
        #     requests_per_second=2.0,  # 2 запроса в секунду
        #     respect_robots=True,
        #     min_delay=0.5,  # минимум 0.5 сек между запросами
        #     user_agent="MyBot/1.0"
        # )
        # results = await crawler3.crawl(urls)
        # print(results)
        # print(crawler3.failed_urls)
        # print(crawler3.visited_urls)

        # Day5
        # print("====Day 5====\n")
        # retry_strategy = RetryStrategy(
        #     max_retries=3,
        #     backoff_factor=2.0,
        #     retry_on=[TransientError, NetworkError]
        # )

        # crawler4 = AsyncCrawler(
        #     max_concurrent=5,
        #     requests_per_second=2.0,  # 2 запроса в секунду
        #     respect_robots=True,
        #     min_delay=0.5,  # минимум 0.5 сек между запросами
        #     user_agent="MyBot/1.0",
        #     retry_strategy=retry_strategy
        # )

        # results = await crawler4.crawl(urls)

        # print(results)
        # print(crawler4.failed_urls)
        # print(crawler4.visited_urls)

        # Day6
        # print("====Day 6====\n")
        # db_storage = PostgreSQLStorage("crawler.db")

        json_storage = JSONStorage("crawl.json")
        await json_storage.save({"url": "test", "text": "testText"})

        # crawler5 = AsyncCrawler(
        #     max_concurrent=5,
        #     requests_per_second=2.0,  # 2 запроса в секунду
        #     respect_robots=True,
        #     min_delay=0.5,  # минимум 0.5 сек между запросами
        #     user_agent="MyBot/1.0",
        #     storage=json_storage
        # )

        # results = await crawler5.crawl(urls)

    finally:

        # await crawler.close()
        # await crawler2.close()
        # await crawler3.close()
        # await crawler4.close()
        # await crawler5.close()

        # print(f"Загружено {len(results)} страниц")
        print(f"Загружено {len('here')} страниц")


async def main2():
    print("=== Day6 JSON===\n")
    storage = JSONStorage(
        "crawler.jsonl"
    )

    crawler = AsyncCrawler(
        storage=storage,
    )

    try:
        await crawler.crawl(
            start_urls=[
                "https://example.com"
            ],
            max_pages=10,
        )
    finally:
        await crawler.close()

    async for page in storage.read():
        print(
            page["url"],
            page["title"]
        )

    print(storage.get_stats())

    print("=== Day6 CSV===\n")
    storage = CSVStorage(
        "crawler.csv"
    )

    crawler = AsyncCrawler(
        storage=storage,
    )

    try:
        await crawler.crawl(
            start_urls=[
                "https://example.com"
            ],
            max_pages=10,
        )
    finally:
        await crawler.close()

    pages = await storage.read()

    for page in pages:
        print(
            page["url"],
            page["title"],
        )

    print(storage.get_stats())

    print("=== Day6 DB===\n")
    storage = PostgreSQLStorage(
        database="crawler"
    )

    crawler = AsyncCrawler(
        storage=storage,
    )

    try:
        await crawler.crawl(
            start_urls=[
                "https://example.com"
            ],
            max_pages=10,
        )
    finally:
        await crawler.close()

    pages = await storage.read()

    for page in pages:
        print(
            page["url"],
            page["title"],
        )

    print(storage.get_stats())

    await storage.close()

# asyncio.run(main())
asyncio.run(main2())
