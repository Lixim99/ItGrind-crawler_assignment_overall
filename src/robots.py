import asyncio
from collections.abc import Awaitable, Callable
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import aiohttp


class RobotsParser:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        before_request: Callable[[str], Awaitable[None]] | None = None,
    ) -> None:
        self._session = session
        self._before_request = before_request
        self._parsers: dict[str, RobotFileParser] = {}
        self._load_locks: dict[str, asyncio.Lock] = {}

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

        if domain in self._parsers:
            return {
                "domain": domain,
                "cached": True,
            }

        load_lock = self._load_locks.setdefault(
            domain,
            asyncio.Lock(),
        )

        async with load_lock:
            if domain in self._parsers:
                return {
                    "domain": domain,
                    "cached": True,
                }

            parser = RobotFileParser()

            if self._before_request is not None:
                await self._before_request(robots_url)

            try:
                async with self._session.get(
                    robots_url
                ) as response:

                    if response.status == 200:
                        content = await response.text()

                        parser.parse(
                            content.splitlines()
                        )

                    elif 400 <= response.status < 500:
                        # robots.txt недоступен —
                        # разрешаем обход.
                        parser.parse([
                            "User-agent: *",
                            "Allow: /",
                        ])

                    else:
                        # 5xx — временная проблема сервера.
                        # Для crawler запрещаем обход.
                        parser.parse([
                            "User-agent: *",
                            "Disallow: /",
                        ])

            except (
                aiohttp.ClientError,
                TimeoutError,
            ):
                parser.parse([
                    "User-agent: *",
                    "Disallow: /",
                ])

            self._parsers[domain] = parser

            return {
                "domain": domain,
                "cached": False,
            }

    def can_fetch(
        self,
        url: str,
        user_agent: str = "*",
    ) -> bool:
        domain = urlparse(url).netloc
        parser = self._parsers.get(domain)

        if parser is None:
            return False

        return parser.can_fetch(
            user_agent,
            url,
        )

    def get_crawl_delay(
        self,
        user_agent: str = "*",
        domain: str | None = None,
    ) -> float:
        if domain is None:
            if len(self._parsers) != 1:
                return 0.0

            parser = next(iter(self._parsers.values()))
        else:
            parsed_domain = urlparse(domain).netloc
            domain_key = parsed_domain or domain
            parser = self._parsers.get(domain_key)

        if parser is None:
            return 0.0

        delay = parser.crawl_delay(
            user_agent
        )

        return float(delay or 0)
