import asyncio
import random
import re
from collections import deque
from time import perf_counter
from urllib.parse import urlparse

import aiohttp

from .exception import RobotsBlockedError
from .limiter import RateLimiter
from .parser import HTMLParser
from .queue import CrawlerQueue
from .robots import RobotsParser
from .semaphore import SemaphoreManager
from .utils import crawler_logger, normalize_url


class AsyncCrawler:
    MAX_ATTEMPTS = 3

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
        max_depth: int = 2,
        requests_per_second: float = 2.0,
        respect_robots: bool = False,
        min_delay: float = 1.0,
        user_agent: str = "*"

    ) -> None:
        if requests_per_second <= 0:
            raise ValueError(
                "requests_per_second must be greater than 0"
            )

        if min_delay < 0:
            raise ValueError(
                "min_delay cannot be negative"
            )

        timeout = aiohttp.ClientTimeout(
            total=30,
            connect=5,
            sock_read=10,
        )

        self._max_concurrent = max_concurrent
        self._max_depth = max_depth
        self._respect_robots = respect_robots
        self._min_delay = min_delay
        self._user_agent = user_agent

        self._session = aiohttp.ClientSession(
            timeout=timeout,
            headers={
                "User-Agent": self._user_agent
            }
        )

        self._semaphore = SemaphoreManager(
            max_tasks=max_concurrent,
        )

        rate_delay = 1 / requests_per_second

        effective_delay = max(
            rate_delay,
            min_delay,
        )

        self._rate_limiter = RateLimiter(
            requests_per_second=1 / effective_delay,
            per_domain=True,
        )

        self._parser = HTMLParser()

        self._visited_urls = set()
        self._failed_urls = {}
        self._processed_urls = {}
        self._url_depths = {}
        self._blocked_urls: set[str] = set()

        # Один RobotsParser на один домен.
        self._robots_parsers: dict[
            str,
            RobotsParser
        ] = {}

        # Отдельный limiter для Crawl-delay каждого домена.
        self._robots_limiters: dict[
            str,
            tuple[float, RateLimiter]
        ] = {}

        # Статистика запросов.
        self._request_timestamps = deque()
        self._request_delays: list[float] = []
        self._last_request_started: float | None = None

    async def _get_robots_parser(
        self,
        url: str,
    ) -> RobotsParser:
        domain = urlparse(url).netloc

        parser = self._robots_parsers.get(domain)

        if parser is None:
            parser = RobotsParser(
                self._session
            )

            self._robots_parsers[domain] = parser

        await parser.fetch_robots(url)

        return parser

    def _get_robots_limiter(
        self,
        domain: str,
        crawl_delay: float,
    ) -> RateLimiter | None:
        if crawl_delay <= 0:
            return None

        cached = self._robots_limiters.get(domain)

        if cached is not None:
            cached_delay, limiter = cached

            if cached_delay == crawl_delay:
                return limiter

        limiter = RateLimiter(
            requests_per_second=1 / crawl_delay,
            per_domain=False,
        )

        self._robots_limiters[domain] = (
            crawl_delay,
            limiter,
        )

        return limiter

    async def _wait_before_request(
        self,
        url: str,
    ) -> None:
        domain = urlparse(url).netloc

        robots_delay = 0.0

        if self._respect_robots:
            robots = await self._get_robots_parser(
                url
            )

            if not robots.can_fetch(
                url,
                user_agent=self._user_agent,
            ):
                crawler_logger.warning(
                    "URL заблокирован robots.txt: %s",
                    url,
                )

                raise RobotsBlockedError(url)

            robots_delay = robots.get_crawl_delay(
                user_agent=self._user_agent,
            )

        # Jitter.
        # Отдельного параметра jitter в ТЗ нет,
        # поэтому используем случайную задержку
        # в диапазоне 0..min_delay.
        if self._min_delay > 0:
            await asyncio.sleep(
                random.uniform(
                    0,
                    self._min_delay,
                )
            )

        # Основное ограничение:
        # requests_per_second + min_delay.
        await self._rate_limiter.acquire(
            domain
        )

        # Дополнительно соблюдаем Crawl-delay,
        # если robots.txt его задал.
        if robots_delay > 0:
            robots_limiter = (
                self._get_robots_limiter(
                    domain,
                    robots_delay,
                )
            )

            await robots_limiter.acquire()

    def _record_request_start(
        self,
    ) -> None:
        now = perf_counter()

        if self._last_request_started is not None:
            self._request_delays.append(
                now - self._last_request_started
            )

        self._last_request_started = now

        self._request_timestamps.append(now)

        # Для "текущих req/sec" оставляем
        # только запросы за последнюю секунду.
        while (
            self._request_timestamps
            and now - self._request_timestamps[0] > 1
        ):
            self._request_timestamps.popleft()

    def _get_request_stats(
        self,
    ) -> dict:
        average_delay = 0.0

        if self._request_delays:
            average_delay = (
                sum(self._request_delays)
                / len(self._request_delays)
            )

        return {
            "requests_per_second": len(
                self._request_timestamps
            ),
            "average_delay": average_delay,
            "robots_blocked": len(
                self._blocked_urls
            ),
        }

    async def fetch_url(self, url: str) -> str:
        crawler_logger.info("Начало загрузки: %s", url)

        for attempt in range(self.MAX_ATTEMPTS):
            await self._wait_before_request(url)
            await self._semaphore.acquire(url)

            try:
                self._record_request_start()

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
                    "HTTP-ошибка | URL: %s | "
                    "статус: %s | попытка: %s",
                    url,
                    error.status,
                    attempt + 1,
                )

                retryable = (
                    error.status == 429
                    or error.status >= 500
                )

                if (
                    not retryable
                    or attempt == self.MAX_ATTEMPTS - 1
                ):
                    return ""

            except TimeoutError as error:
                crawler_logger.error(
                    "Таймаут | URL: %s | "
                    "тип: %s | попытка: %s",
                    url,
                    type(error).__name__,
                    attempt + 1,
                )

                if attempt == self.MAX_ATTEMPTS - 1:
                    return ""

            except aiohttp.ClientError as error:
                crawler_logger.error(
                    "Сетевая ошибка | URL: %s | "
                    "тип: %s | сообщение: %s | "
                    "попытка: %s",
                    url,
                    type(error).__name__,
                    error,
                    attempt + 1,
                )

                if attempt == self.MAX_ATTEMPTS - 1:
                    return ""

            finally:
                self._semaphore.release(url)

            backoff = 2 ** attempt

            crawler_logger.info(
                "Backoff %.2f сек | URL: %s",
                backoff,
                url,
            )

            await asyncio.sleep(backoff)

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
        self._queue = CrawlerQueue()

        self._visited_urls.clear()
        self._failed_urls.clear()
        self._processed_urls.clear()
        self._url_depths.clear()
        self._request_timestamps.clear()
        self._request_delays.clear()
        self._blocked_urls.clear()
        self._last_request_started = None

        include_patterns = include_patterns or []
        exclude_patterns = exclude_patterns or []

        allowed_domains = {
            urlparse(url).netloc
            for url in start_urls
        }

        for url in start_urls:
            # поправить 2 параметр
            url = normalize_url(
                value=url,
                base_url=url,
            )

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
            await self._queue.join()
        finally:
            for worker in workers:
                worker.cancel()

            await asyncio.gather(*workers, return_exceptions=True)

        return dict(self._processed_urls)

    async def _worker(
        self,
        *,
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

                        self._visited_urls.add(next_url)
                        self._url_depths[next_url] = next_depth
                        self._queue.add_url(next_url, priority=next_depth)
            except RobotsBlockedError:
                self._blocked_urls.add(url)
                self._queue.mark_processed(url)

            except Exception as error:
                self._failed_urls[url] = str(error)
                self._queue.mark_failed(url, error=str(error))
            else:
                self._processed_urls[url] = parsed_page
                self._queue.mark_processed(url)
            finally:
                self._log_progress(
                    started
                )

    def _log_progress(
        self,
        started: float,
    ) -> None:
        processed = len(
            self._processed_urls
        )

        failed = len(
            self._failed_urls
        )

        blocked = len(
            self._blocked_urls
        )

        elapsed = (
            perf_counter() - started
        )

        queue_stats = (
            self._queue.get_stats()
        )

        request_stats = (
            self._get_request_stats()
        )

        crawler_logger.info(
            "Прогресс | "
            "обработано: %s | "
            "очередь: %s | "
            "ошибок: %s | "
            "robots blocked: %s | "
            "скорость: %.2f req/sec | "
            "средняя задержка: %.2f сек | "
            "время: %.2f сек",
            processed,
            queue_stats["queued"],
            failed,
            blocked,
            request_stats[
                "requests_per_second"
            ],
            request_stats[
                "average_delay"
            ],
            elapsed,
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
