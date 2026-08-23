from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import aiohttp


class RobotsParser:
    def __init__(
        self,
        session: aiohttp.ClientSession,
    ) -> None:
        self._session = session
        self._parser = RobotFileParser()

        self._domain: str | None = None
        self._loaded = False

    async def fetch_robots(
        self,
        base_url: str,
    ) -> dict:
        parsed_url = urlparse(base_url)

        domain = parsed_url.netloc

        robots_url = (
            f"{parsed_url.scheme}://"
            f"{domain}/robots.txt"
        )

        if self._loaded:
            return {
                "domain": domain,
                "cached": True,
            }

        self._domain = domain

        try:
            async with self._session.get(
                robots_url
            ) as response:

                if response.status == 200:
                    content = await response.text()

                    self._parser.parse(
                        content.splitlines()
                    )

                elif 400 <= response.status < 500:
                    # robots.txt недоступен —
                    # разрешаем обход.
                    self._parser.parse([
                        "User-agent: *",
                        "Allow: /",
                    ])

                else:
                    # 5xx — временная проблема сервера.
                    # Для вежливого crawler запрещаем обход.
                    self._parser.parse([
                        "User-agent: *",
                        "Disallow: /",
                    ])

        except aiohttp.ClientError:
            self._parser.parse([
                "User-agent: *",
                "Disallow: /",
            ])

        self._loaded = True

        return {
            "domain": domain,
            "cached": False,
        }

    def can_fetch(
        self,
        url: str,
        user_agent: str = "*",
    ) -> bool:
        if not self._loaded:
            return False

        return self._parser.can_fetch(
            user_agent,
            url,
        )

    def get_crawl_delay(
        self,
        user_agent: str = "*",
    ) -> float:
        if not self._loaded:
            return 0.0

        delay = self._parser.crawl_delay(
            user_agent
        )

        return float(delay or 0)
