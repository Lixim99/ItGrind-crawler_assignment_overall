import asyncio

from bs4 import BeautifulSoup

from .utils import crawler_logger, normalize_url


class HTMLParser:
    async def parse_html(
        self,
        html: str,
        url: str
    ) -> dict:
        return await asyncio.to_thread(
            self._parse_html_sync,
            html,
            url
        )

    def _parse_html_sync(
        self,
        html: str,
        url: str
    ) -> dict:
        crawler_logger.info("Начало парсинга URL: %s", url)

        result = {
            "url": url,
            "title": "",
            "links": [],
            "metadata": {},
            "text": "",
            "images": [],
            "headings": [],
            "tables": [],
            "lists": [],
            "text_length": 0,
            "links_count": 0,
            "images_count": 0,
        }

        if not html or not html.strip():
            crawler_logger.warning(
                "Парсинг пустой страницы | URL: %s",
                url
            )

            return result

        soup = BeautifulSoup(html, "lxml")

        metadata = self._extract_safely(
            "metadata",
            url,
            {},
            lambda: self.extract_metadata(soup),
        )

        links = self._extract_safely(
            "links",
            url,
            [],
            lambda: self.extract_links(soup, url),
        )

        text = self._extract_safely(
            "text",
            url,
            "",
            lambda: self.extract_text(soup),
        )

        images = self._extract_safely(
            "images",
            url,
            [],
            lambda: self.extract_images(soup, url),
        )

        headings = self._extract_safely(
            "headings",
            url,
            [],
            lambda: self.extract_headings(soup),
        )

        tables = self._extract_safely(
            "tables",
            url,
            [],
            lambda: self.extract_tables(soup),
        )

        lists = self._extract_safely(
            "lists",
            url,
            [],
            lambda: self.extract_lists(soup),
        )

        result.update({
            "title": metadata.get("title", ""),
            "text_length": len(text),
            "links_count": len(links),
            "links": links,
            "text": text,
            "metadata": metadata,
            "images": images,
            "headings": headings,
            "images_count": len(images),
            "tables": tables,
            "lists": lists,
        })

        return result

    @staticmethod
    def _extract_safely(
        name: str,
        url: str,
        default,
        extractor,
    ) -> any:
        try:
            return extractor()
        except Exception as error:
            crawler_logger.warning(
                "Ошибка парсинга %s | URL: %s | тип: %s",
                name,
                url,
                type(error).__name__,
            )

            return default

    @staticmethod
    def _normalize_url(value: str, base_url: str) -> str | None:
        return normalize_url(value, base_url)

    def extract_links(
        self,
        soup: BeautifulSoup,
        base_url: str
    ) -> list[str]:
        links = []
        seen = set()

        for link in soup.find_all("a"):
            href = link.get("href", "")

            absolut_url = self._normalize_url(href, base_url)

            if absolut_url and absolut_url not in seen:
                seen.add(absolut_url)
                links.append(absolut_url)

        return links

    def extract_text(
        self,
        soup: BeautifulSoup,
        selector: str | None = None
    ) -> str:
        element = soup.select_one(selector) if selector else soup

        if element is None:
            return ""

        return element.get_text(" ", strip=True)

    def extract_metadata(self, soup: BeautifulSoup) -> dict:
        metadata = {}

        if soup.title and soup.title.string:
            metadata["title"] = soup.title.string.strip()

        for meta in soup.find_all("meta"):
            name = meta.get("name") or meta.get("property")
            content = meta.get("content")

            if not name or not content:
                continue

            name = name.casefold()

            if name in {
                "description",
                "keywords",
                "og:title",
                "og:url",
                "og:image",
                "referrer",

            }:
                metadata[name] = content.strip()

        return metadata

    def extract_images(
            self,
            soup: BeautifulSoup,
            base_url: str
    ) -> list[dict[str, str]]:
        images = []

        for img in soup.find_all("img"):
            src = img.get("src", "")

            if not src:
                continue

            absolute_src = self._normalize_url(src, base_url)

            if absolute_src:
                images.append(
                    {
                        "src": absolute_src,
                        "alt": str(img.get("alt", "")).strip(),
                    }
                )

        return images

    def extract_headings(
        self,
        soup: BeautifulSoup
    ) -> list[dict[str, str]]:
        headings = []

        for heading in soup.find_all({"h1", "h2", "h3"}):
            headings.append(
                {
                    "level": heading.name,
                    "text": str(heading.get_text(" ")).strip()
                }
            )

        return headings

    def extract_tables(
        self,
        soup: BeautifulSoup,
    ) -> list[dict[str, str]]:
        tables = []

        for table in soup.find_all("table"):
            rows = []

            for row in table.find_all("tr"):
                cells = [
                    str(cell.get_text("")).strip()
                    for cell in row.find_all(["th", "td"])
                ]

                if cells:
                    rows.append(cells)

            tables.append(rows)

        return tables

    def extract_lists(
        self,
        soup: BeautifulSoup
    ) -> list[dict[str, object]]:
        lists = []

        for list_tag in soup.find_all(["ul", "ol"]):
            lists.append(
                {
                    "type": list_tag.name,
                    "items": [
                        str(item.get_text(" ")).strip()
                        for item in list_tag.find_all("li")
                    ]
                }
            )

        return lists
