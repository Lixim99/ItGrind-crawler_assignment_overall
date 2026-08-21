import asyncio
import re
from time import perf_counter
from urllib.parse import urlparse

import aiohttp

from .parser import HTMLParser
from .queue import CrawlerQueue
from .semaphore import SemaphoreManager
from .utils import crawler_logger, normalize_url


class AsyncCrawler:
    @property
    def visited_urls(self) -> set:
        return self._visited_urls

    @property
    def failed_urls(self) -> dict:
        return self._failed_urls

    @property
    def processed_urls(self) -> dict:
        return self._processed_urls

    def __init__(
        self,
        *,
        max_concurrent: int = 10,
        max_depth: int = 2
    ) -> None:
        timeout = aiohttp.ClientTimeout(
            total=30,
            connect=5,
            sock_read=10,
        )

        self._semaphore = SemaphoreManager(
            max_tasks=max_concurrent,
        )
        self._max_concurrent = max_concurrent
        self._session = aiohttp.ClientSession(timeout=timeout)
        self._parser = HTMLParser()
        self._max_depth = max_depth
        self._visited_urls = set()
        self._failed_urls = {}
        self._processed_urls = {}
        self._queue = CrawlerQueue()
        self._url_depths = {}

    async def fetch_url(self, url: str) -> str:
        crawler_logger.info("Начало загрузки: %s", url)

        await self._semaphore.acquire(url)

        try:
            async with self._session.get(url) as response:
                response.raise_for_status()
                content = await response.text()

                crawler_logger.info(
                    "Успешно загружено: %s | HTTP %s | символов: %s",
                    url,
                    response.status,
                    len(content),
                )

                return content

        except aiohttp.ClientResponseError as error:
            crawler_logger.error(
                "HTTP-ошибка | URL: %s | тип: %s | статус: %s",
                url,
                type(error).__name__,
                error.status,
            )

        except TimeoutError as error:
            crawler_logger.error(
                "Таймаут | URL: %s | тип: %s",
                url,
                type(error).__name__,
            )

        except aiohttp.ClientError as error:
            crawler_logger.error(
                "Сетевая ошибка | URL: %s | тип: %s | сообщение: %s",
                url,
                type(error).__name__,
                error,
            )

        finally:
            self._semaphore.release(url)

        return ""

    async def fetch_urls_sequentially(self, urls: list[str]) -> dict[str, str]:
        results = {}

        for url in urls:
            results[url] = await self.fetch_url(url)

        return results

    async def fetch_urls(self, urls: list[str]) -> dict[str, str]:
        responses = await asyncio.gather(
            *(self.fetch_url(url) for url in urls),
        )

        return dict(zip(urls, responses, strict=True))

    async def close(self) -> None:
        await self._session.close()

    async def fetch_and_parse(self, url: str) -> dict:
        page_html = await self.fetch_url(url)

        return await self._parser.parse_html(page_html, url)

    async def crawl(
        self,
        start_urls: list[str],
        max_pages: int = 100,
        same_domain_only: bool = True,
        include_patterns: list[str] | None = None,
        exclude_patterns: list[str] | None = None,
    ) -> dict:
        # CrawlerQueue такой же в init
        self._queue = CrawlerQueue()
        self._visited_urls.clear()
        self._failed_urls.clear()
        self._processed_urls.clear()
        self._url_depths.clear()

        include_patterns = include_patterns or []
        exclude_patterns = exclude_patterns or []

        allowed_domains = {
            urlparse(url).netloc
            for url in start_urls
        }

        for url in start_urls:
            # поправить 2 параметр
            url = normalize_url(url, url)

            if url is None:
                continue

            if len(self._visited_urls) >= max_pages:
                break

            if url in self._visited_urls:
                continue

            self._visited_urls.add(url)
            self._url_depths[url] = 0
            self._queue.add_url(url, priority=0)

        started = perf_counter()

        workers = [
            asyncio.create_task(
                self._worker(
                    max_pages=max_pages,
                    allowed_domains=allowed_domains,
                    same_domain_only=same_domain_only,
                    include_patterns=include_patterns,
                    exclude_patterns=exclude_patterns,
                    started=started
                )
            )
            for _ in range(min(self._max_concurrent, max_pages))
        ]

        try:
            # что дает join
            await self._queue.join()
        finally:
            for worker in workers:
                worker.cancel()

            await asyncio.gather(*workers, return_exceptions=True)

        return dict(self._processed_urls)

    async def _worker(
        self,
        *,
        depth: int = 0,
        max_pages: int = 100,
        same_domain_only: bool = True,
        allowed_domains: set[str],
        include_patterns: list[str],
        exclude_patterns: list[str],
        started: float
    ):
        while True:
            url = await self._queue.get_next()
            depth = self._url_depths[url]

            try:
                html = await self.fetch_url(url)

                if not html:
                    raise RuntimeError("Не удалось загрузить страницу")

                parsed_page = await self._parser.parse_html(html, url)

                if depth < self._max_depth:
                    for link in parsed_page.get("links", []):
                        if len(self._visited_urls) >= max_pages:
                            break

                        next_url = normalize_url(
                            value=link,
                            base_url=url
                        )

                        if next_url is None:
                            continue

                        if next_url in self._visited_urls:
                            continue

                        if not self._is_allowed_url(
                            next_url,
                            allowed_domains=allowed_domains,
                            same_domain_only=same_domain_only,
                            include_patterns=include_patterns,
                            exclude_patterns=exclude_patterns,
                        ):
                            continue

                        next_depth = depth + 1

                        if next_depth > self._max_depth:
                            continue

                        self._visited_urls.add(next_url)
                        self._url_depths[next_url] = next_depth
                        self._queue.add_url(next_url, priority=next_depth)
            except Exception as error:
                self._failed_urls[url] = str(error)
                self._queue.mark_failed(url, error=str(error))
            else:
                self._queue.mark_processed(url)
                self._processed_urls[url] = parsed_page
            finally:
                processed = len(self._processed_urls)
                failed = len(self._failed_urls)
                completed = processed + failed

                elapsed = perf_counter() - started
                stats = self._queue.get_stats()

                crawler_logger.info(
                    "Прогресс | обработано: %s | очередь: %s | "
                    "ошибок: %s | скорость: %.2f стр/сек",
                    processed,
                    stats["queued"],
                    len(self._failed_urls),
                    completed / elapsed if elapsed else 0,
                )

    def _is_allowed_url(
        self,
        url: str,
        *,
        allowed_domains: set[str],
        same_domain_only: bool,
        include_patterns: list[str],
        exclude_patterns: list[str],
    ) -> bool:
        domain = urlparse(url).netloc

        if same_domain_only and domain not in allowed_domains:
            return False

        if exclude_patterns and any(
            re.search(pattern, url)
            for pattern in exclude_patterns
        ):
            return False

        return not include_patterns or any(
            re.search(pattern, url)
            for pattern in include_patterns
        )
