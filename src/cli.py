import argparse


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Async web crawler"
    )

    parser.add_argument(
        "--urls",
        nargs="+",
        help="Start URLs",
    )

    parser.add_argument(
        "--max-pages",
        type=int,
        help="Maximum number of pages",
    )

    parser.add_argument(
        "--max-depth",
        type=int,
        help="Maximum crawl depth",
    )

    parser.add_argument(
        "--output",
        type=str,
        help="Crawled pages JSON file",
    )

    parser.add_argument(
        "--config",
        type=str,
        default="config.json",
        help="Configuration file",
    )

    parser.add_argument(
        "--respect-robots",
        action="store_true",
        default=None,
        help="Respect robots.txt",
    )

    parser.add_argument(
        "--rate-limit",
        type=float,
        help="Requests per second",
    )

    parser.add_argument(
        "--html-report",
        type=str,
        help="HTML statistics report file",
    )

    parser.add_argument(
        "--demo-day",
        type=int,
        choices=range(1, 8),
        help="Run the demonstration for day 1-7",
    )

    return parser


def parse_args() -> argparse.Namespace:
    parser = create_parser()

    return parser.parse_args()


def apply_cli_overrides(
    config: dict,
    args: argparse.Namespace,
) -> dict:
    if args.urls is not None:
        config["crawl"]["start_urls"] = (
            args.urls
        )

    if args.max_pages is not None:
        config["crawl"]["max_pages"] = (
            args.max_pages
        )

    if args.max_depth is not None:
        config["crawler"]["max_depth"] = (
            args.max_depth
        )

    if args.respect_robots is not None:
        config["crawler"]["respect_robots"] = (
            args.respect_robots
        )

    if args.rate_limit is not None:
        config["crawler"][
            "requests_per_second"
        ] = args.rate_limit

    return config
