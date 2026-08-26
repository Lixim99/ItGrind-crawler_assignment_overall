import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from urllib.parse import urldefrag, urljoin, urlparse


def setup_logging(
    log_file: str = "logs/crawler.log",
    level: int = logging.INFO,
) -> None:
    log_path = Path(log_file)

    log_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    console_handler = logging.StreamHandler()

    console_handler.setLevel(
        level
    )

    console_handler.setFormatter(
        formatter
    )

    file_handler = RotatingFileHandler(
        filename=log_path,
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )

    file_handler.setLevel(
        level
    )

    file_handler.setFormatter(
        formatter
    )

    logger = logging.getLogger(
        "crawler"
    )

    logger.setLevel(
        level
    )

    logger.handlers.clear()

    logger.addHandler(
        console_handler
    )

    logger.addHandler(
        file_handler
    )


crawler_logger = logging.getLogger("crawler")


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

    if not parsed.path:
        parsed = parsed._replace(path="/")

    return parsed.geturl()
