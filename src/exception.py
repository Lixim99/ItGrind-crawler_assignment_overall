class RobotsBlockedError(Exception):
    def __init__(self, message: str):
        super().__init__(f"Заблокирован роботами {message}")
