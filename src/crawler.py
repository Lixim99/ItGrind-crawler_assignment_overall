from .config import create_storage, load_config
from .models import AsyncCrawler


class AdvancedCrawler:
    def __init__(
        self,
        crawler: AsyncCrawler,
        crawl_config: dict,
    ):
        self._crawler = crawler
        self._crawl_config = crawl_config

    @classmethod
    def from_config_data(
        cls,
        config: dict,
    ) -> "AdvancedCrawler":
        storage = create_storage(
            config["storage"]
        )

        crawler = AsyncCrawler(
            **config["crawler"],
            storage=storage,
        )

        return cls(
            crawler=crawler,
            crawl_config=config["crawl"],
        )

    @classmethod
    def from_config(
        cls,
        filename: str,
    ) -> "AdvancedCrawler":
        config = load_config(
            filename
        )

        return cls.from_config_data(
            config
        )

    async def crawl(self) -> dict:
        return await self._crawler.crawl(
            **self._crawl_config
        )

    def get_stats(self) -> dict:
        return self._crawler.get_stats()

    def export_to_json(
        self,
        filename: str,
    ) -> None:
        self._crawler.export_to_json(
            filename
        )

    def export_to_html_report(
        self,
        filename: str,
    ) -> None:
        self._crawler.export_to_html_report(
            filename
        )

    async def close(self) -> None:
        await self._crawler.close()
