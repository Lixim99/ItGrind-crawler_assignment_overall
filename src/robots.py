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
        self._crawl_delays: dict[
            str,
            dict[str, float | None],
        ] = {}
        self._load_locks: dict[str, asyncio.Lock] = {}

    @staticmethod
    def _user_agent_token(user_agent: str) -> str:
        value = user_agent.strip()

        if not value:
            return "*"

        product = value.split(maxsplit=1)[0]

        return product.split("/", 1)[0].casefold() or "*"

    @classmethod
    def _parse_crawl_delays(
        cls,
        lines: list[str],
    ) -> dict[str, float | None]:
        delays: dict[str, float | None] = {}
        agents: list[str] = []
        delay: float | None = None
        has_directives = False

        def save_group() -> None:
            for agent in agents:
                delays.setdefault(agent, delay)

        for raw_line in lines:
            line = raw_line.split("#", 1)[0].strip()

            if not line:
                if agents and has_directives:
                    save_group()
                    agents = []
                    delay = None
                    has_directives = False

                continue

            name, separator, value = line.partition(":")

            if not separator:
                continue

            directive = name.strip().casefold()
            value = value.strip()

            if directive == "user-agent":
                if agents and has_directives:
                    save_group()
                    agents = []
                    delay = None
                    has_directives = False

                agents.append(cls._user_agent_token(value))
                continue

            if not agents:
                continue

            has_directives = True

            if directive != "crawl-delay":
                continue

            try:
                parsed_delay = float(value)
            except ValueError:
                continue

            if parsed_delay >= 0:
                delay = parsed_delay

        if agents:
            save_group()

        return delays

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
            crawl_delays: dict[str, float | None] = {}

            if self._before_request is not None:
                await self._before_request(robots_url)

            try:
                async with self._session.get(
                    robots_url
                ) as response:

                    if response.status == 200:
                        content = await response.text()
                        lines = content.splitlines()

                        parser.parse(lines)
                        crawl_delays = self._parse_crawl_delays(
                            lines
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
            self._crawl_delays[domain] = crawl_delays

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
            self._user_agent_token(user_agent),
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

            domain_key = next(iter(self._parsers))
        else:
            parsed_domain = urlparse(domain).netloc
            domain_key = parsed_domain or domain

        if domain_key not in self._parsers:
            return 0.0

        delays = self._crawl_delays.get(domain_key, {})
        product = self._user_agent_token(user_agent)

        if product in delays:
            delay = delays[product]
        else:
            delay = delays.get("*")

        return float(delay or 0)
