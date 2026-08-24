import asyncio

from .exception import TransientError
from .utils import crawler_logger


class RetryStrategy:
    # Настраиваемые типы ошибок для повтора
    # Экспоненциальный backoff между попытками
    def __init__(
        self,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        retry_on: list | None = None
    ):
        self._max_retries = max_retries
        self._backoff_factor = backoff_factor
        self._retry_on = retry_on or []

        self._total_retries = 0
        self._failed_after_retries = 0
        self._successful_after_retry = 0

    async def execute_with_retry(
        self,
        coro,
        *args,
        **kwargs
    ):
        for attempt in range(self._max_retries + 1):
            try:
                result = await coro(*args, **kwargs)

                if attempt > 0:
                    self._successful_after_retry += 1

                return result
            except Exception as error:
                if not isinstance(
                    error,
                    tuple(self._retry_on),
                ):
                    raise

                if attempt == self._max_retries:
                    self._failed_after_retries += 1
                    raise

                self._total_retries += 1

                backoff = self._backoff_factor ** attempt

                if (
                    isinstance(error, TransientError)
                    and error.status == 429
                ):
                    backoff *= 2

                crawler_logger.warning(
                    "Retry | attempt: %s/%s | error: %s | delay: %.2f sec",
                    attempt + 1,
                    self._max_retries,
                    type(error).__name__,
                    backoff,
                )

                await asyncio.sleep(backoff)

    def get_stats(self) -> dict:
        return {
            "total_retries": self._total_retries,
            "successful_after_retry": self._successful_after_retry,
            "failed_after_retries": self._failed_after_retries,
        }
