import asyncio
from collections import Counter

from .exception import TransientError
from .utils import crawler_logger


class RetryStrategy:
    def __init__(
        self,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        retry_on: list | None = None,
        max_retries_by_type: dict[type[Exception], int] | None = None,
        backoff_factors_by_type: dict[type[Exception], float] | None = None,
    ):
        if max_retries < 0:
            raise ValueError("max_retries cannot be negative")

        if backoff_factor <= 0:
            raise ValueError("backoff_factor must be greater than zero")

        self._max_retries = max_retries
        self._backoff_factor = backoff_factor
        self._retry_on = retry_on or []
        self._max_retries_by_type = max_retries_by_type or {}
        self._backoff_factors_by_type = backoff_factors_by_type or {}

        if any(value < 0 for value in self._max_retries_by_type.values()):
            raise ValueError("retry limits cannot be negative")

        if any(value <= 0 for value in self._backoff_factors_by_type.values()):
            raise ValueError("backoff factors must be greater than zero")

        self.reset_stats()

    @staticmethod
    def _get_value_for_error(
        error: Exception,
        values: dict[type[Exception], int | float],
        default: int | float,
    ) -> int | float:
        for error_type, value in values.items():
            if isinstance(error, error_type):
                return value

        return default

    @staticmethod
    def _find_url(
        args: tuple,
        kwargs: dict,
    ) -> str | None:
        keyword_url = kwargs.get("url")

        if isinstance(keyword_url, str):
            return keyword_url

        if args and isinstance(args[0], str):
            return args[0]

        return None

    def reset_stats(self) -> None:
        self._total_retries = 0
        self._failed_after_retries = 0
        self._successful_after_retry = 0
        self._total_retry_delay = 0.0
        self._errors_by_type = Counter()

    async def execute_with_retry(
        self,
        coro,
        *args,
        retry_url: str | None = None,
        **kwargs
    ):
        retry_url = retry_url or self._find_url(args, kwargs)
        retries_by_type = Counter()
        attempt = 1

        while True:
            try:
                result = await coro(*args, **kwargs)

                if attempt > 1:
                    self._successful_after_retry += 1
                    crawler_logger.info(
                        "Retry result | URL: %s | outcome: success | "
                        "attempts: %s",
                        retry_url or "N/A",
                        attempt,
                    )

                return result
            except Exception as error:
                error_type = type(error)
                error_name = error_type.__name__
                self._errors_by_type[error_name] += 1

                if not isinstance(
                    error,
                    tuple(self._retry_on),
                ):
                    crawler_logger.error(
                        "Retry result | URL: %s | outcome: failure | "
                        "attempts: %s | error: %s | retryable: false",
                        retry_url or "N/A",
                        attempt,
                        error_name,
                    )
                    raise

                max_retries = int(self._get_value_for_error(
                    error,
                    self._max_retries_by_type,
                    self._max_retries,
                ))
                retries_for_type = retries_by_type[error_type]

                if retries_for_type >= max_retries:
                    self._failed_after_retries += 1
                    crawler_logger.error(
                        "Retry result | URL: %s | outcome: failure | "
                        "attempts: %s | error: %s | retries exhausted: %s",
                        retry_url or "N/A",
                        attempt,
                        error_name,
                        max_retries,
                    )
                    raise

                retries_by_type[error_type] += 1
                self._total_retries += 1

                backoff_factor = float(self._get_value_for_error(
                    error,
                    self._backoff_factors_by_type,
                    self._backoff_factor,
                ))
                backoff = backoff_factor ** retries_for_type

                if (
                    isinstance(error, TransientError)
                    and error.status == 429
                ):
                    backoff *= 2

                self._total_retry_delay += backoff

                crawler_logger.warning(
                    "Retry | URL: %s | attempt: %s | retry: %s/%s | "
                    "error: %s | delay: %.2f sec",
                    retry_url or "N/A",
                    attempt,
                    retries_for_type + 1,
                    max_retries,
                    error_name,
                    backoff,
                )

                await asyncio.sleep(backoff)
                attempt += 1

    def get_stats(self) -> dict:
        average_retry_delay = (
            self._total_retry_delay / self._total_retries
            if self._total_retries > 0
            else 0.0
        )

        return {
            "total_retries": self._total_retries,
            "successful_after_retry": self._successful_after_retry,
            "failed_after_retries": self._failed_after_retries,
            "average_retry_delay": average_retry_delay,
            "errors_by_type": dict(self._errors_by_type),
        }
