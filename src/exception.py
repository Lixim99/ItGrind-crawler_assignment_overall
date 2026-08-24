class RobotsBlockedError(Exception):
    def __init__(self, message: str):
        super().__init__(f"Заблокирован роботами {message}")


class TransientError(Exception):
    def __init__(
        self,
        message: str,
        status: int | None = None,
    ):
        super().__init__(message)
        self.status = status


class PermanentError(Exception):
    def __init__(
        self,
        message: str,
        status: int | None = None,
    ):
        super().__init__(message)
        self.status = status


class NetworkError(Exception):
    ...


class ParseError(Exception):
    ...
