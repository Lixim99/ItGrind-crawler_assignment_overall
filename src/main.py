import asyncio
from time import perf_counter

from .models import AsyncCrawler
from .utils import setup_logging

setup_logging()


async def main():
    # crawler = AsyncCrawler(max_concurrent=5)

    urls = [
        # "https://example.com",
        # "https://httpbin.org/delay/1",
        # "https://httpbin.org/delay/2",
        # "https://test.com",
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
        print("====Day 4====\n")
        crawler3 = AsyncCrawler(
            max_concurrent=5,
            requests_per_second=2.0,  # 2 запроса в секунду
            respect_robots=True,
            min_delay=0.5,  # минимум 0.5 сек между запросами
            user_agent="MyBot/1.0"
        )
        results = await crawler3.crawl(urls)
        print(results)
        print(crawler3.failed_urls)
        print(crawler3.visited_urls)

    finally:
        # await crawler.close()
        # await crawler2.close()
        await crawler3.close()

    # print(f"Загружено {len(results)} страниц")

asyncio.run(main())
