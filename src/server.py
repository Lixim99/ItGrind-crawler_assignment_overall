import asyncio
import os

from aiohttp import web


async def slow_handler(request):
    await asyncio.sleep(0.05)

    return web.Response(
        text="OK"
    )

app = web.Application()

app.router.add_get(
    "/slow",
    slow_handler,
)


def main() -> None:
    host = os.getenv(
        "SERVER_HOST",
        os.getenv("SERVIER_HOST", "127.0.0.1"),
    )

    web.run_app(
        app,
        host=host,
        port=int(os.getenv("SERVER_PORT", "8080")),
        access_log=None,
    )


if __name__ == "__main__":
    main()
