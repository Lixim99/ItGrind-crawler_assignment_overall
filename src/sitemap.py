import xml.etree.ElementTree as ET
from collections.abc import Awaitable, Callable

import aiohttp

from .utils import crawler_logger


class SitemapParser:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        before_request: Callable[[str], Awaitable[None]] | None = None,
    ) -> None:
        self._session = session
        self._before_request = before_request
        self._visited_sitemaps: set[str] = set()

    @staticmethod
    def _local_name(tag: str) -> str:
        return tag.split("}")[-1]

    @classmethod
    def _get_locations(
        cls,
        root: ET.Element,
        item_name: str,
    ) -> list[str]:
        locations = []

        for item in root:
            if cls._local_name(item.tag) != item_name:
                continue

            for child in item:
                if cls._local_name(child.tag) != "loc":
                    continue

                if child.text and child.text.strip():
                    locations.append(child.text.strip())

                break

        return locations

    async def fetch_sitemap(
        self,
        sitemap_url: str
    ) -> list[str]:
        if sitemap_url in self._visited_sitemaps:
            return []

        self._visited_sitemaps.add(sitemap_url)

        try:
            if self._before_request is not None:
                await self._before_request(sitemap_url)

            async with self._session.get(sitemap_url) as response:
                response.raise_for_status()

                xml = await response.text()

            root = ET.fromstring(xml)
        except (
            aiohttp.ClientError,
            TimeoutError,
            ET.ParseError,
        ) as error:
            crawler_logger.warning(
                "Не удалось обработать sitemap | URL: %s | "
                "тип: %s | ошибка: %r",
                sitemap_url,
                type(error).__name__,
                error,
            )

            return []

        root_type = self._local_name(
            root.tag
        )

        if root_type == "urlset":
            return self._get_locations(
                root,
                "url",
            )

        if root_type == "sitemapindex":
            result = []

            for child_sitemap in self._get_locations(
                root,
                "sitemap",
            ):
                urls = await self.fetch_sitemap(
                    child_sitemap
                )

                result.extend(urls)

            return result

        crawler_logger.warning(
            "Неизвестный формат sitemap | URL: %s | root: %s",
            sitemap_url,
            root_type,
        )

        return []
