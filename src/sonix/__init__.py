import asyncio
import functools

from sonix.app.applications import Sonix
from sonix.app.requests import Request
from sonix.app.responses import HTMLResponse, JSONResponse, PlainTextResponse, Response
from sonix.server.protocol import HTTPProtocol

__all__ = [
    "HTMLResponse",
    "JSONResponse",
    "PlainTextResponse",
    "Request",
    "Response",
    "Sonix",
]

HOST = "127.0.0.1"
PORT = 8000


def _build_demo_app() -> Sonix:
    app = Sonix()

    @app.get("/")
    def hello_world(request: Request) -> dict:
        return {"message": "Hello, world!"}

    @app.get("/items/{item_id:int}")
    def get_item(request: Request) -> dict:
        return {"item_id": request.path_params["item_id"]}

    return app


async def _run_server(host: str, port: int) -> None:
    loop = asyncio.get_running_loop()
    app = _build_demo_app()
    server = await loop.create_server(
        functools.partial(HTTPProtocol, app=app), host, port
    )
    async with server:
        print(f"Sonix serving on http://{host}:{port} (Ctrl+C to stop)")
        await server.serve_forever()


def main() -> None:
    try:
        asyncio.run(_run_server(HOST, PORT))
    except KeyboardInterrupt:
        print("\nShutting down.")
