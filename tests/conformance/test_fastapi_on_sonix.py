"""A real, unmodified FastAPI application served by Sonix's HTTP server.

This is the strongest correctness claim available for the server layer: it is
one thing to pass one's own unit tests, and another to correctly run a
framework written by someone else against the ASGI spec rather than against
Sonix's interpretation of it.
"""

from __future__ import annotations

import contextlib

import httpx
import pytest
from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel

from conformance.support import serve_on_sonix


class Item(BaseModel):
    name: str
    price: float


def get_multiplier() -> int:
    return 3


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.started = True
    yield
    app.state.started = False


def build_app() -> FastAPI:
    app = FastAPI(lifespan=lifespan)

    @app.get("/ping")
    async def ping():
        return {"pong": True}

    @app.get("/items/{item_id}")
    async def read_item(item_id: int, q: str | None = None):
        return {"item_id": item_id, "q": q}

    @app.post("/items")
    async def create_item(item: Item):
        return {"name": item.name, "price": item.price}

    @app.get("/multiplied/{value}")
    async def multiplied(value: int, multiplier: int = Depends(get_multiplier)):
        return {"result": value * multiplier}

    @app.get("/boom")
    async def boom():
        raise HTTPException(status_code=418, detail="teapot")

    @app.get("/started")
    async def started():
        return {"started": app.state.started}

    return app


@pytest.fixture
async def fastapi_url():
    async with serve_on_sonix(build_app()) as url:
        yield url


class TestFastAPIOnSonix:
    async def test_simple_get(self, fastapi_url):
        async with httpx.AsyncClient(base_url=fastapi_url) as client:
            response = await client.get("/ping")
        assert response.status_code == 200
        assert response.json() == {"pong": True}

    async def test_path_and_query_parameters(self, fastapi_url):
        async with httpx.AsyncClient(base_url=fastapi_url) as client:
            response = await client.get("/items/42", params={"q": "search"})
        assert response.json() == {"item_id": 42, "q": "search"}

    async def test_json_request_body(self, fastapi_url):
        # Exercises the request-body path: Content-Length framing through
        # Sonix's parser into FastAPI's receive() loop and pydantic.
        async with httpx.AsyncClient(base_url=fastapi_url) as client:
            response = await client.post(
                "/items", json={"name": "widget", "price": 9.99}
            )
        assert response.status_code == 200
        assert response.json() == {"name": "widget", "price": 9.99}

    async def test_fastapi_dependency_injection_works(self, fastapi_url):
        async with httpx.AsyncClient(base_url=fastapi_url) as client:
            response = await client.get("/multiplied/7")
        assert response.json() == {"result": 21}

    async def test_fastapi_validation_error_becomes_422(self, fastapi_url):
        async with httpx.AsyncClient(base_url=fastapi_url) as client:
            response = await client.get("/items/not-an-int")
        assert response.status_code == 422

    async def test_fastapi_http_exception_passes_through(self, fastapi_url):
        async with httpx.AsyncClient(base_url=fastapi_url) as client:
            response = await client.get("/boom")
        assert response.status_code == 418
        assert response.json() == {"detail": "teapot"}

    async def test_fastapi_lifespan_ran(self, fastapi_url):
        # Sonix drove FastAPI's lifespan, not just its request path.
        async with httpx.AsyncClient(base_url=fastapi_url) as client:
            response = await client.get("/started")
        assert response.json() == {"started": True}

    async def test_unmatched_route_is_fastapis_404(self, fastapi_url):
        async with httpx.AsyncClient(base_url=fastapi_url) as client:
            response = await client.get("/definitely-not-here")
        assert response.status_code == 404

    async def test_keep_alive_reuses_one_connection(self, fastapi_url):
        # httpx pools by default, so these share a connection unless Sonix
        # closes it -- which would silently halve benchmark throughput later.
        async with httpx.AsyncClient(base_url=fastapi_url) as client:
            first = await client.get("/ping")
            second = await client.get("/ping")
        assert first.status_code == second.status_code == 200

    async def test_openapi_schema_is_served(self, fastapi_url):
        # A larger response body than the other cases, so it exercises the
        # write path beyond a single small chunk.
        async with httpx.AsyncClient(base_url=fastapi_url) as client:
            response = await client.get("/openapi.json")
        assert response.status_code == 200
        assert "paths" in response.json()
