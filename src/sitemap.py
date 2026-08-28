import xml.etree.ElementTree as ET

import aiohttp

from .utils import crawler_logger


class SitemapParser:
    def __init__(
        self,
        session: aiohttp.ClientSession
    ):
        self._session = session
        self._visited_sitemaps: set[str] = set()

    @staticmethod
    def _local_name(tag: str) -> str:
        return tag.split("}")[-1]

    async def fetch_sitemap(
        self,
        sitemap_url: str
    ) -> list[str]:
        if sitemap_url in self._visited_sitemaps:
            return []

        self._visited_sitemaps.add(sitemap_url)

        try:
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
            urls = []

            for elem in root.iter():
                if self._local_name(elem.tag) != "loc":
                    continue

                if elem.text:
                    urls.append(
                        elem.text.strip()
                    )

            return urls

        if root_type == "sitemapindex":
            result = []

            for elem in root.iter():
                if self._local_name(elem.tag) != "loc":
                    continue

                if not elem.text:
                    continue

                child_sitemap = elem.text.strip()

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
