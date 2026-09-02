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

    scheme = parsed.scheme.lower()

    if scheme not in {"http", "https"}:
        return None

    hostname = parsed.hostname

    if hostname is None:
        return None

    try:
        port = parsed.port
    except ValueError:
        return None

    host = hostname.lower()

    if ":" in host:
        host = f"[{host}]"

    is_default_port = (
        scheme == "http" and port == 80
    ) or (
        scheme == "https" and port == 443
    )

    if port is not None and not is_default_port:
        host = f"{host}:{port}"

    user_info = ""

    if "@" in parsed.netloc:
        user_info = f"{parsed.netloc.rsplit('@', 1)[0]}@"

    parsed = parsed._replace(
        scheme=scheme,
        netloc=f"{user_info}{host}",
        path=parsed.path or "/",
    )

    return parsed.geturl()
