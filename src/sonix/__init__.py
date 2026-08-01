import asyncio
import functools

from sonix.server.protocol import HTTPProtocol
from sonix.types import Receive, Scope, Send

HOST = "127.0.0.1"
PORT = 8000

_HELLO_BODY = b"Hello, world!"


async def _hello_world_app(scope: Scope, receive: Receive, send: Send) -> None:
    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [
                (b"content-type", b"text/plain; charset=utf-8"),
                (b"content-length", str(len(_HELLO_BODY)).encode()),
            ],
        }
    )
    await send({"type": "http.response.body", "body": _HELLO_BODY, "more_body": False})


async def _run_server(host: str, port: int) -> None:
    loop = asyncio.get_running_loop()
    server = await loop.create_server(
        functools.partial(HTTPProtocol, app=_hello_world_app), host, port
    )
    async with server:
        print(f"Sonix serving on http://{host}:{port} (Ctrl+C to stop)")
        await server.serve_forever()


def main() -> None:
    try:
        asyncio.run(_run_server(HOST, PORT))
    except KeyboardInterrupt:
        print("\nShutting down.")
