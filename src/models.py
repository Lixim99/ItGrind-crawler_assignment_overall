import asyncio

import aiohttp

from .parser import HTMLParser
from .utils import crawler_logger


class AsyncCrawler:
    def __init__(
        self,
        *,
        max_concurrent: int = 10
    ) -> None:
        timeout = aiohttp.ClientTimeout(
            total=30,
            connect=5,
            sock_read=10,
        )

        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._max_concurrent = max_concurrent
        self._session = aiohttp.ClientSession(timeout=timeout)
        self._parser = HTMLParser()

    async def fetch_url(self, url: str) -> str:
        crawler_logger.info("Начало загрузки: %s", url)

        try:
            async with (
                self._semaphore,
                self._session.get(url) as response,
            ):
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
                "HTTP-ошибка | URL: %s | статус: %s",
                url,
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

    async def fetch_and_parse(self, url: str) -> dict[str, str]:
        page_html = await self.fetch_url(url)

        return await self._parser.parse_html(page_html, url)
