import asyncio
import json
import os
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from time import perf_counter

import aiofiles
from aiohttp import web
from dotenv import load_dotenv

from .cli import apply_cli_overrides, parse_args
from .config import load_config
from .constants import get_upload_path
from .crawler import AdvancedCrawler
from .exception import NetworkError, TransientError
from .models import AsyncCrawler
from .retry_strategy import RetryStrategy
from .storage import CSVStorage, DataStorage, JSONStorage, PostgreSQLStorage
from .utils import setup_logging

load_dotenv()
setup_logging()

DEMO_USER_AGENT = "MyCrawlerDemo/1.0"


async def _write_json(filename: str | Path, data: object) -> Path:
    output_path = get_upload_path(filename)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    async with aiofiles.open(
        output_path,
        mode="w",
        encoding="utf-8",
    ) as file:
        await file.write(
            json.dumps(
                data,
                ensure_ascii=False,
                indent=2,
                default=str,
            )
        )

    return output_path


@asynccontextmanager
async def _demo_site() -> AsyncGenerator[str]:
    """Запустить локальный сайт, используемый демонстрациями."""

    flaky_attempts: dict[str, int] = {}

    def base_url(request: web.Request) -> str:
        return f"{request.scheme}://{request.host}"

    async def index(request: web.Request) -> web.Response:
        base = base_url(request)
        html = f"""
        <!doctype html>
        <html lang="ru">
          <head>
            <title>Demo index</title>
            <meta name="description" content="Crawler demo site">
            <meta name="keywords" content="asyncio,crawler,aiohttp">
          </head>
          <body>
            <h1>Demo index</h1>
            <h2>Pages</h2>
            <a href="/page/1">Page 1</a>
            <a href="/page/2">Page 2</a>
            <a href="/articles/one">Article</a>
            <a href="/articles/private">Private article</a>
            <a href="/private">Robots private page</a>
            <img src="{base}/image.png" alt="Demo image">
            <table>
              <tr><th>Name</th><th>Value</th></tr>
              <tr><td>crawler</td><td>async</td></tr>
            </table>
            <ul><li>aiohttp</li><li>asyncio</li></ul>
          </body>
        </html>
        """
        return web.Response(text=html, content_type="text/html")

    async def page(request: web.Request) -> web.Response:
        number = request.match_info.get("number", "page")
        html = f"""
        <!doctype html>
        <html lang="ru">
          <head>
            <title>Demo page {number}</title>
            <meta name="description" content="Page {number}">
          </head>
          <body>
            <h1>Page {number}</h1>
            <p>Текст демонстрационной страницы {number}.</p>
            <a href="/">Index</a>
            <a href="/page/2">Page 2</a>
            <ol><li>first</li><li>second</li></ol>
          </body>
        </html>
        """
        return web.Response(text=html, content_type="text/html")

    async def delayed(request: web.Request) -> web.Response:
        delay = float(request.match_info["delay"])
        await asyncio.sleep(delay)
        return web.Response(text=f"Delayed for {delay:.2f} sec")

    async def status(request: web.Request) -> web.Response:
        status_code = int(request.match_info["status"])
        return web.Response(text=f"HTTP {status_code}", status=status_code)

    async def flaky(request: web.Request) -> web.Response:
        status_code = int(request.match_info["status"])
        failures = int(request.query.get("failures", "1"))
        key = request.query.get("key", str(status_code))
        flaky_attempts[key] = flaky_attempts.get(key, 0) + 1

        if flaky_attempts[key] <= failures:
            return web.Response(
                text=f"Temporary HTTP {status_code}",
                status=status_code,
            )

        return web.Response(
            text="<html><title>Recovered</title><body>OK</body></html>",
            content_type="text/html",
        )

    async def robots(_request: web.Request) -> web.Response:
        return web.Response(
            text=(
                f"User-agent: {DEMO_USER_AGENT}\n"
                "Disallow: /private\n"
                "Crawl-delay: 1\n\n"
                "User-agent: *\n"
                "Allow: /\n"
            ),
            content_type="text/plain",
        )

    async def sitemap(request: web.Request) -> web.Response:
        base = base_url(request)
        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <url><loc>{base}/page/1</loc></url>
          <url><loc>{base}/page/2</loc></url>
        </urlset>
        """
        return web.Response(text=xml, content_type="application/xml")

    async def sitemap_index(request: web.Request) -> web.Response:
        base = base_url(request)
        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
        <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <sitemap><loc>{base}/sitemap-pages.xml</loc></sitemap>
        </sitemapindex>
        """
        return web.Response(text=xml, content_type="application/xml")

    app = web.Application()
    app.router.add_get("/", index)
    app.router.add_get("/page/{number}", page)
    app.router.add_get("/articles/{number}", page)
    app.router.add_get("/private", page)
    app.router.add_get("/delay/{delay}", delayed)
    app.router.add_get("/status/{status}", status)
    app.router.add_get("/flaky/{status}", flaky)
    app.router.add_get("/robots.txt", robots)
    app.router.add_get("/sitemap-pages.xml", sitemap)
    app.router.add_get("/sitemap-index.xml", sitemap_index)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="127.0.0.1", port=0)
    await site.start()

    host, port = runner.addresses[0]

    try:
        yield f"http://{host}:{port}"
    finally:
        await runner.cleanup()


def _print_page_summary(page: dict) -> None:
    print({
        "url": page["url"],
        "title": page["title"],
        "text_length": page["text_length"],
        "links_count": page["links_count"],
        "links": page["links"],
        "images_count": page["images_count"],
        "headings": page["headings"],
    })


async def demo_day_1() -> None:
    """День 1: сравнить последовательную и параллельную загрузку."""

    print("\n=== День 1: асинхронный HTTP-клиент ===")

    async with _demo_site() as base:
        urls = [
            f"{base}/delay/0.1?request={number}"
            for number in range(1, 7)
        ]

        sequential_crawler = AsyncCrawler(
            max_concurrent=3,
            requests_per_second=10_000,
            min_delay=0,
        )

        try:
            started = perf_counter()
            await sequential_crawler.fetch_urls_sequentially(urls)
            sequential_time = perf_counter() - started
        finally:
            await sequential_crawler.close()

        parallel_crawler = AsyncCrawler(
            max_concurrent=3,
            requests_per_second=10_000,
            min_delay=0,
        )

        try:
            started = perf_counter()
            results = await parallel_crawler.fetch_urls(urls)
            parallel_time = perf_counter() - started
        finally:
            await parallel_crawler.close()

    for url, content in results.items():
        print(f"OK | {url} | символов: {len(content)}")

    speedup = sequential_time / parallel_time if parallel_time else 0.0
    print(f"Последовательно: {sequential_time:.3f} сек")
    print(f"Параллельно:     {parallel_time:.3f} сек")
    print(f"Ускорение:       {speedup:.2f}x")


async def demo_day_2() -> None:
    """День 2: загрузить HTML и вывести извлечённые данные."""

    print("\n=== День 2: парсинг HTML ===")

    async with _demo_site() as base:
        crawler = AsyncCrawler(
            max_concurrent=3,
            requests_per_second=10_000,
            min_delay=0,
        )
        urls = [base, f"{base}/page/1", f"{base}/page/2"]

        try:
            pages = await asyncio.gather(
                *(crawler.fetch_and_parse(url) for url in urls)
            )
        finally:
            await crawler.close()

    for parsed_page in pages:
        _print_page_summary(parsed_page)


async def demo_day_3() -> None:
    """День 3: обойти сайт через очередь с глубиной и фильтрами."""

    print("\n=== День 3: очередь и управление конкурентностью ===")

    async with _demo_site() as base:
        crawler = AsyncCrawler(
            max_concurrent=3,
            max_depth=2,
            requests_per_second=10_000,
            min_delay=0,
        )

        try:
            results = await crawler.crawl(
                start_urls=[f"{base}/"],
                max_pages=5,
                same_domain_only=True,
                exclude_patterns=[r"/private$"],
            )
            failed_urls = dict(crawler.failed_urls)
            visited_urls = sorted(crawler.visited_urls)
        finally:
            await crawler.close()

    output = await _write_json("demos/day3_pages.json", results)

    print(f"Обработано: {len(results)}")
    print(f"Посещено URL: {len(visited_urls)}")
    print(f"Ошибок: {len(failed_urls)}")
    print(f"Данные сохранены: {output}")


async def demo_day_4() -> None:
    """День 4: показать rate limiting и блокировку robots.txt."""

    print("\n=== День 4: rate limiting и robots.txt ===")

    async with _demo_site() as base:
        crawler = AsyncCrawler(
            max_concurrent=1,
            max_depth=0,
            requests_per_second=10,
            respect_robots=True,
            min_delay=0.1,
            user_agent=DEMO_USER_AGENT,
        )

        try:
            results = await crawler.crawl(
                start_urls=[
                    base,
                    f"{base}/page/1",
                    f"{base}/private",
                ],
                max_pages=3,
            )
            request_stats = crawler.get_request_stats()
        finally:
            await crawler.close()

    print(f"Разрешённых страниц: {len(results)}")
    print(f"Текущая скорость: {request_stats['requests_per_second']} req/sec")
    print(f"Средняя задержка: {request_stats['average_delay']:.3f} сек")
    print(f"Заблокировано robots.txt: {request_stats['robots_blocked']}")


async def demo_day_5() -> None:
    """День 5: показать retry для 503 и отсутствие retry для 404."""

    print("\n=== День 5: ошибки и автоматические повторы ===")

    retry_strategy = RetryStrategy(
        max_retries=2,
        backoff_factor=2.0,
        retry_on=[TransientError, NetworkError],
    )

    async with _demo_site() as base:
        crawler = AsyncCrawler(
            max_concurrent=2,
            max_depth=0,
            requests_per_second=10_000,
            min_delay=0,
            retry_strategy=retry_strategy,
        )

        try:
            results = await crawler.crawl(
                start_urls=[
                    f"{base}/flaky/503?key=service&failures=1",
                    f"{base}/status/404",
                ],
                max_pages=2,
            )
            failed_urls = dict(crawler.failed_urls)
        finally:
            await crawler.close()

    error_report = {
        "retry_stats": retry_strategy.get_stats(),
        "permanent_and_final_errors": failed_urls,
    }
    output = await _write_json("demos/day5_errors.json", error_report)

    print(f"Успешных страниц: {len(results)}")
    print(f"Ошибок: {len(failed_urls)}")
    print(f"Статистика повторов: {retry_strategy.get_stats()}")
    print(f"Отчёт об ошибках: {output}")


async def _crawl_with_storage(
    storage: DataStorage,
    start_url: str,
) -> dict:
    crawler = AsyncCrawler(
        max_concurrent=2,
        max_depth=0,
        requests_per_second=10_000,
        min_delay=0,
        storage=storage,
    )

    try:
        return await crawler.crawl(
            start_urls=[start_url],
            max_pages=1,
        )
    finally:
        await crawler.close()


async def demo_day_6() -> None:
    """День 6: сохранить и прочитать JSONL, CSV и PostgreSQL."""

    print("\n=== День 6: асинхронное сохранение данных ===")
    suffix = datetime.now().strftime("%Y%m%d-%H%M%S-%f")

    async with _demo_site() as base:
        json_name = f"day6-{suffix}.jsonl"
        json_storage = JSONStorage(json_name)
        await _crawl_with_storage(json_storage, base)
        json_pages = [page async for page in json_storage.read()]
        print(
            "JSONL:",
            get_upload_path(json_name),
            json_storage.get_stats(),
            f"прочитано: {len(json_pages)}",
        )

        csv_name = f"day6-{suffix}.csv"
        csv_storage = CSVStorage(csv_name, encoding="utf-8-sig")
        await _crawl_with_storage(csv_storage, base)
        csv_pages = await csv_storage.read()
        print(
            "CSV:",
            get_upload_path(csv_name),
            csv_storage.get_stats(),
            f"прочитано: {len(csv_pages)}",
        )

        if os.getenv("RUN_POSTGRES_DEMO") == "1":
            postgres_storage = PostgreSQLStorage(database="crawler")

            try:
                await _crawl_with_storage(postgres_storage, base)
                postgres_pages = await postgres_storage.read()
                print(
                    "PostgreSQL:",
                    postgres_storage.get_stats(),
                    f"прочитано: {len(postgres_pages)}",
                )
            except Exception as error:
                print(
                    "PostgreSQL недоступен:",
                    f"{type(error).__name__}: {error}",
                )
            finally:
                await postgres_storage.close()
        else:
            print(
                "PostgreSQL: пропущено. Запустите compose и задайте "
                "RUN_POSTGRES_DEMO=1 для демонстрации БД."
            )


async def demo_day_7() -> None:
    """День 7: запустить AdvancedCrawler из JSON-конфигурации."""

    print("\n=== День 7: финальная интеграция ===")
    suffix = datetime.now().strftime("%Y%m%d-%H%M%S-%f")

    async with _demo_site() as base:
        config = {
            "crawler": {
                "max_concurrent": 3,
                "max_depth": 1,
                "requests_per_second": 10_000,
                "respect_robots": True,
                "min_delay": 0,
                "user_agent": DEMO_USER_AGENT,
            },
            "crawl": {
                "start_urls": [f"{base}/"],
                "sitemap_urls": [f"{base}/sitemap-index.xml"],
                "max_pages": 5,
                "same_domain_only": True,
                "include_patterns": [],
                "exclude_patterns": [r"/private$"],
            },
            "storage": {
                "type": "json",
                "filename": f"day7-{suffix}.jsonl",
            },
        }
        config_path = await _write_json(
            "demos/day7_config.json",
            config,
        )

        crawler = AdvancedCrawler.from_config(str(config_path))

        try:
            await crawler.crawl()
            stats = crawler.get_stats()

            json_report = get_upload_path("demos/day7_stats.json")
            html_report = get_upload_path("demos/day7_report.html")
            crawler.export_to_json(str(json_report))
            crawler.export_to_html_report(str(html_report))
        finally:
            await crawler.close()

    print(f"Обработано: {stats['total_pages']} страниц")
    print(f"Успешно: {stats['successful']}")
    print(f"Ошибок: {stats['failed']}")
    print(f"Средняя скорость: {stats['average_speed']:.2f} pages/sec")
    print(f"JSON-отчёт: {json_report}")
    print(f"HTML-отчёт: {html_report}")


DEMO_FUNCTIONS: dict[int, Callable[[], Awaitable[None]]] = {
    1: demo_day_1,
    2: demo_day_2,
    3: demo_day_3,
    4: demo_day_4,
    5: demo_day_5,
    6: demo_day_6,
    7: demo_day_7,
}


async def run_demo(day: int) -> None:
    try:
        demonstration = DEMO_FUNCTIONS[day]
    except KeyError as error:
        raise ValueError("demo day must be between 1 and 7") from error

    await demonstration()


async def _run_crawler_cli(args) -> None:
    config = load_config(args.config)
    config = apply_cli_overrides(config, args)
    crawler = AdvancedCrawler.from_config_data(config)

    try:
        results = await crawler.crawl()
        stats = crawler.get_stats()

        print(f"Обработано: {stats['total_pages']}")
        print(f"Успешно: {stats['successful']}")
        print(f"Ошибок: {stats['failed']}")
        print(f"Скорость: {stats['average_speed']:.2f} pages/sec")

        if args.output:
            output_path = await _write_json(
                args.output,
                results,
            )
            print(f"Результаты: {output_path}")

        if args.html_report:
            crawler.export_to_html_report(args.html_report)
    finally:
        await crawler.close()


async def main() -> None:
    args = parse_args()

    if args.demo_day is not None:
        await run_demo(args.demo_day)
        return

    await _run_crawler_cli(args)


if __name__ == "__main__":
    asyncio.run(main())
