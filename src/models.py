import asyncio
import random
import re
from collections import deque
from datetime import datetime
from time import perf_counter
from urllib.parse import urlparse

import aiohttp

from .exception import (
    NetworkError,
    ParseError,
    PermanentError,
    RobotsBlockedError,
    TransientError,
)
from .limiter import RateLimiter
from .parser import HTMLParser
from .queue import CrawlerQueue
from .retry_strategy import RetryStrategy
from .robots import RobotsParser
from .semaphore import SemaphoreManager
from .sitemap import SitemapParser
from .stats import CrawlerStats
from .storage import DataStorage
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
        max_concurrent_per_domain: int = 3,
        max_depth: int = 2,
        requests_per_second: float = 2.0,
        respect_robots: bool = False,
        min_delay: float = 1.0,
        user_agent: str = "*",
        connect_timeout: float = 5.0,
        read_timeout: float = 10.0,
        total_timeout: float = 30.0,
        retry_strategy: RetryStrategy | None = None,
        storage: DataStorage | None = None
    ) -> None:
        if max_concurrent <= 0:
            raise ValueError(
                "max_concurrent must be greater than 0"
            )

        if max_concurrent_per_domain <= 0:
            raise ValueError(
                "max_concurrent_per_domain must be greater than 0"
            )

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
        self._connect_timeout = connect_timeout
        self._read_timeout = read_timeout
        self._total_timeout = total_timeout
        self._storage = storage

        self._session = aiohttp.ClientSession(
            timeout=timeout,
            headers={
                "User-Agent": self._user_agent
            }
        )

        self._semaphore = SemaphoreManager(
            max_tasks=max_concurrent,
            max_tasks_per_domain=max_concurrent_per_domain,
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
        self._stats = CrawlerStats()

        self._robots_parser = RobotsParser(
            self._session,
            before_request=self._wait_before_robots_request,
        )

        # Отдельный limiter для Crawl-delay каждого домена.
        self._robots_limiters: dict[
            str,
            tuple[float, RateLimiter]
        ] = {}

        # Статистика запросов.
        self._request_timestamps = deque()
        self._request_delays: list[float] = []
        self._last_request_started: float | None = None
        self._response_info: dict[str, dict] = {}
        self._active_tasks = 0

        self._retry_strategy = retry_strategy or RetryStrategy(
            max_retries=3,
            backoff_factor=2.0,
            retry_on=[
                TransientError,
                NetworkError,
            ],
        )

    async def _get_robots_parser(
        self,
        url: str,
    ) -> RobotsParser:
        await self._robots_parser.fetch_robots(url)

        return self._robots_parser

    def _get_timeout(
            self,
            multiplier: float = 1.0
    ) -> aiohttp.ClientTimeout:
        return aiohttp.ClientTimeout(
            total=self._total_timeout * multiplier,
            connect=self._connect_timeout * multiplier,
            sock_read=self._read_timeout * multiplier
        )

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
                domain=domain,
            )

        await self._wait_for_rate_limit(url)

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

    async def _wait_before_robots_request(
        self,
        robots_url: str,
    ) -> None:
        await self._wait_for_rate_limit(robots_url)
        self._record_request_start()

    async def _wait_before_sitemap_request(
        self,
        sitemap_url: str,
    ) -> None:
        await self._wait_before_request(sitemap_url)
        self._record_request_start()

    async def _wait_for_rate_limit(
        self,
        url: str,
    ) -> None:
        domain = urlparse(url).netloc

        # Jitter.
        # Отдельного параметра jitter в ТЗ нет,
        # поэтому использую случайную задержку
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

    def get_request_stats(self) -> dict:
        return self._get_request_stats()

    async def fetch_url(
        self,
        url: str,
        timeout_multiplier: float = 1.0,
    ) -> str:
        crawler_logger.info("Начало загрузки: %s", url)

        await self._wait_before_request(url)
        await self._semaphore.acquire(url)

        try:
            self._record_request_start()

            timeout = self._get_timeout(multiplier=timeout_multiplier)

            async with self._session.get(
                url,
                timeout=timeout
            ) as response:
                if response.status in {429, 500, 503}:
                    raise TransientError(
                        f"HTTP {response.status}",
                        status=response.status
                    )

                if 400 <= response.status < 500:
                    raise PermanentError(
                        f"HTTP {response.status}",
                        status=response.status
                    )

                if response.status >= 500:
                    raise TransientError(
                        f"HTTP {response.status}",
                        status=response.status
                    )

                content = await response.text()

                crawler_logger.info(
                    "Успешно загружено: %s | HTTP %s | символов: %s",
                    url,
                    response.status,
                    len(content),
                )

                self._response_info[url] = {
                    "status_code": response.status,
                    "content_type": response.content_type,
                }

                return content

        except (TransientError, PermanentError) as error:
            crawler_logger.error(
                "HTTP-ошибка | URL: %s | тип: %s | статус: %s",
                url,
                type(error).__name__,
                error.status,
            )

            raise

        except TimeoutError as error:
            crawler_logger.error(
                "Таймаут | URL: %s | "
                "тип: %s",
                url,
                type(error).__name__,
            )

            raise TransientError(
                f"Timeout: {url}"
            ) from error

        except aiohttp.ClientError as error:
            crawler_logger.error(
                "Сетевая ошибка | URL: %s | "
                "тип: %s | сообщение: %s | ",
                url,
                type(error).__name__,
                error,
            )

            raise NetworkError(
                f"Network error: {url}: {error}"
            ) from error

        finally:
            self._semaphore.release(url)

    async def fetch_urls_sequentially(self, urls: list[str]) -> dict[str, str]:
        results = {}

        for url in urls:
            results[url] = await self.fetch_url(url)

        return results

    async def fetch_urls(self, urls: list[str]) -> dict[str, str]:
        responses = await asyncio.gather(
            *(self.fetch_url(url) for url in urls),
            return_exceptions=True,
        )

        results = {}

        for url, response in zip(
            urls,
            responses,
            strict=True,
        ):
            if isinstance(response, Exception):
                crawler_logger.error(
                    "Не удалось загрузить %s | %s",
                    url,
                    response,
                )

                continue

            results[url] = response

        return results

    async def close(self) -> None:
        await self._session.close()

        if self._storage:
            await self._storage.close()

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
        sitemap_urls: list[str] | None = None
    ) -> dict:
        started = perf_counter()

        self._stats = CrawlerStats()
        self._stats.start()
        self._retry_strategy.reset_stats()

        self._queue = CrawlerQueue()

        self._visited_urls.clear()
        self._failed_urls.clear()
        self._processed_urls.clear()
        self._url_depths.clear()
        self._request_timestamps.clear()
        self._request_delays.clear()
        self._blocked_urls.clear()
        self._last_request_started = None
        self._active_tasks = 0

        include_patterns = include_patterns or []
        exclude_patterns = exclude_patterns or []

        domain_sources = start_urls or (sitemap_urls or [])
        allowed_domains = {
            urlparse(normalized_url).netloc
            for raw_url in domain_sources
            if (
                normalized_url := normalize_url(
                    value=raw_url,
                    base_url=raw_url,
                )
            ) is not None
        }
        seed_urls = list(start_urls)

        if sitemap_urls:
            sitemap_parser = SitemapParser(
                self._session,
                before_request=self._wait_before_sitemap_request,
            )

            for sitemap_url in sitemap_urls:
                try:
                    sitemap_pages = await sitemap_parser.fetch_sitemap(
                        sitemap_url
                    )
                except Exception as error:
                    crawler_logger.warning(
                        "Не удалось загрузить sitemap | URL: %s | "
                        "тип: %s | ошибка: %r",
                        sitemap_url,
                        type(error).__name__,
                        error,
                    )

                    continue

                seed_urls.extend(
                    sitemap_pages
                )

        for url in seed_urls:
            if len(self._visited_urls) >= max_pages:
                break

            normalized_url = normalize_url(
                value=url,
                base_url=url,
            )

            if normalized_url is None:
                continue

            if normalized_url in self._visited_urls:
                continue

            if not self._is_allowed_url(
                normalized_url,
                allowed_domains=allowed_domains,
                same_domain_only=same_domain_only,
                include_patterns=include_patterns,
                exclude_patterns=exclude_patterns,
            ):
                continue

            self._visited_urls.add(
                normalized_url
            )

            self._url_depths[
                normalized_url
            ] = 0

            self._queue.add_url(
                normalized_url,
                priority=0,
            )

        if not self._visited_urls:
            self._sync_retry_stats()
            self._stats.finish()
            return {}

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

            self._sync_retry_stats()
            self._stats.finish()

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

            self._active_tasks += 1

            attempt = 0

            # Обертка для увеличения timeout при повторных попытках
            async def fetch_with_timeout(
                cur_url=url
            ):
                nonlocal attempt

                attempt += 1

                return await self.fetch_url(
                    cur_url,
                    timeout_multiplier=attempt,
                )

            try:
                html = await self._retry_strategy.execute_with_retry(
                    fetch_with_timeout
                )

                try:
                    parsed_page = await self._parser.parse_html(
                        html,
                        url,
                    )
                except Exception as error:
                    raise ParseError(
                        f"Parse error: {url}"
                    ) from error

                try:
                    if self._storage:
                        await self._storage.save({
                            "url": url,
                            "title": parsed_page["title"],
                            "text": parsed_page["text"],
                            "links": parsed_page["links"],
                            "metadata": parsed_page["metadata"],
                            "crawled_at": datetime.now(),
                            "status_code": self._response_info[url]["status_code"],
                            "content_type": self._response_info[url]["content_type"],
                        })
                except Exception as error:
                    crawler_logger.error(
                        "Ошибка сохранения | URL: %s | "
                        "тип: %s | сообщение: %s",
                        url,
                        type(error).__name__,
                        error,
                    )

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
                self._failed_urls[url] = {
                    "type": type(error).__name__,
                    "message": str(error),
                }

                status_code = getattr(
                    error,
                    "status",
                    None,
                )

                self._stats.record_failure(
                    url=url,
                    status_code=status_code,
                    error_type=(
                        None
                        if isinstance(
                            error,
                            (
                                TransientError,
                                NetworkError,
                                PermanentError,
                            ),
                        )
                        else type(error).__name__
                    ),
                    permanent=isinstance(error, PermanentError),
                )

                self._queue.mark_failed(url, error=str(error))
            else:
                self._processed_urls[url] = parsed_page

                self._stats.record_success(
                    url=url,
                    status_code=self._response_info[url][
                        "status_code"
                    ],
                )

                self._queue.mark_processed(url)
            finally:
                self._active_tasks -= 1

                self._log_progress(
                    started,
                    max_pages
                )

    def get_stats(self) -> dict:
        self._sync_retry_stats()
        return self._stats.get_stats()

    def _sync_retry_stats(self) -> None:
        self._stats.set_retry_stats(
            self._retry_strategy.get_stats()
        )

    def export_to_json(
        self,
        filename: str,
    ) -> None:
        self._sync_retry_stats()
        self._stats.export_to_json(
            filename
        )

    def export_to_html_report(
        self,
        filename: str
    ) -> None:
        self._sync_retry_stats()
        self._stats.export_to_html_report(filename)

    def _log_progress(
        self,
        started: float,
        max_pages: int,
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

        completed = (
            processed
            + failed
            + blocked
        )

        elapsed = (
            perf_counter()
            - started
        )

        pages_per_second = (
            completed / elapsed
            if elapsed > 0
            else 0.0
        )

        queue_stats = (
            self._queue.get_stats()
        )

        request_stats = (
            self._get_request_stats()
        )

        retries_stats = (
            self._retry_strategy.get_stats()
        )

        known_remaining = (
            queue_stats["queued"]
            + self._active_tasks
        )

        is_finished = (
            queue_stats["queued"] == 0
            and self._active_tasks == 0
        )

        if is_finished:
            progress = 100.0
            eta = 0.0
        else:
            progress = min(
                completed / max_pages * 100,
                100.0,
            )

            eta = (
                known_remaining / pages_per_second
                if pages_per_second > 0
                else None
            )

        crawler_logger.info(
            "Прогресс | "
            "%.1f%% | "
            "обработано: %s | "
            "лимит: %s | "
            "успешно: %s | "
            "ошибок: %s | "
            "robots blocked: %s | "
            "очередь: %s | "
            "active: %s | "
            "скорость обработки: %.2f pages/sec | "
            "скорость запросов: %.2f req/sec | "
            "средняя задержка: %.2f сек | "
            "ETA текущей очереди: %s | "
            "время: %.2f сек | "
            "retries: %s | "
            "retries success: %s | "
            "retries failed: %s",
            progress,
            completed,
            max_pages,
            processed,
            failed,
            blocked,
            queue_stats["queued"],
            self._active_tasks,
            pages_per_second,
            request_stats["requests_per_second"],
            request_stats["average_delay"],
            (
                f"{eta:.2f} сек"
                if eta is not None
                else "N/A"
            ),
            elapsed,
            retries_stats["total_retries"],
            retries_stats["successful_after_retry"],
            retries_stats["failed_after_retries"],
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
