import logging
from urllib.parse import urldefrag, urljoin, urlparse


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


crawler_logger = logging.getLogger("async_crawler")


def normalize_url(value: str, base_url: str) -> str | None:
    value = value.strip()

    if not value or value.startswith("#"):
        return None

    absolute_url = urljoin(base_url, value)
    absolute_url, _ = urldefrag(absolute_url)
    parsed = urlparse(absolute_url)

    if parsed.scheme not in {"http", "https"}:
        return None

    if not parsed.netloc:
        return

    return absolute_url
