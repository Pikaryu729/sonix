"""An unmodified Starlette application served by Sonix's HTTP server.

Complements the FastAPI case with the things Starlette exposes directly and
FastAPI hides: a genuinely streaming response body (several
http.response.body messages), background tasks that run after the response is
sent, and explicit control over request-body reading.
"""

from __future__ import annotations

import asyncio
import contextlib

import httpx
import pytest
from starlette.applications import Starlette
from starlette.background import BackgroundTask
from starlette.responses import JSONResponse, PlainTextResponse, StreamingResponse
from starlette.routing import Route

from conformance.support import serve_on_sonix

BACKGROUND_RAN = asyncio.Event()


async def ping(request):
    return PlainTextResponse("pong")


async def echo(request):
    body = await request.json()
    return JSONResponse({"echoed": body})


async def stream(request):
    async def chunks():
        for index in range(5):
            yield f"chunk-{index};".encode()

    # No Content-Length: Sonix must frame this correctly on its own.
    return StreamingResponse(chunks(), media_type="text/plain")


async def with_background(request):
    async def afterwards():
        BACKGROUND_RAN.set()

    return PlainTextResponse("accepted", background=BackgroundTask(afterwards))


async def large(request):
    # Comfortably past a single TCP segment, so the write path has to handle
    # a body that does not go out in one call.
    return PlainTextResponse("x" * 200_000)


def build_app() -> Starlette:
    return Starlette(
        routes=[
            Route("/ping", ping),
            Route("/echo", echo, methods=["POST"]),
            Route("/stream", stream),
            Route("/background", with_background),
            Route("/large", large),
        ]
    )


@pytest.fixture
async def starlette_url():
    BACKGROUND_RAN.clear()
    async with serve_on_sonix(build_app()) as url:
        yield url


class TestStarletteOnSonix:
    async def test_simple_get(self, starlette_url):
        async with httpx.AsyncClient(base_url=starlette_url) as client:
            response = await client.get("/ping")
        assert response.status_code == 200
        assert response.text == "pong"

    async def test_request_body_round_trip(self, starlette_url):
        async with httpx.AsyncClient(base_url=starlette_url) as client:
            response = await client.post("/echo", json={"a": 1})
        assert response.json() == {"echoed": {"a": 1}}

    async def test_streaming_response_arrives_intact(self, starlette_url):
        # Sonix has to frame a response with no Content-Length, and preserve
        # every one of the several http.response.body messages.
        async with httpx.AsyncClient(base_url=starlette_url) as client:
            response = await client.get("/stream")
        assert response.status_code == 200
        assert response.text == "".join(f"chunk-{i};" for i in range(5))

    async def test_streaming_response_can_be_consumed_incrementally(
        self, starlette_url
    ):
        async with (
            httpx.AsyncClient(base_url=starlette_url) as client,
            client.stream("GET", "/stream") as response,
        ):
            received = [chunk async for chunk in response.aiter_bytes()]
        assert b"".join(received) == b"".join(f"chunk-{i};".encode() for i in range(5))

    async def test_background_task_runs_after_the_response(self, starlette_url):
        async with httpx.AsyncClient(base_url=starlette_url) as client:
            response = await client.get("/background")
        assert response.text == "accepted"
        await asyncio.wait_for(BACKGROUND_RAN.wait(), timeout=5)

    async def test_large_response_body(self, starlette_url):
        async with httpx.AsyncClient(base_url=starlette_url) as client:
            response = await client.get("/large")
        assert len(response.text) == 200_000

    async def test_starlette_404(self, starlette_url):
        async with httpx.AsyncClient(base_url=starlette_url) as client:
            response = await client.get("/nope")
        assert response.status_code == 404

    async def test_starlette_405_carries_allow(self, starlette_url):
        async with httpx.AsyncClient(base_url=starlette_url) as client:
            response = await client.get("/echo")
        assert response.status_code == 405
        assert "allow" in {k.lower() for k in response.headers}

    async def test_concurrent_requests_are_all_answered(self, starlette_url):
        async with httpx.AsyncClient(base_url=starlette_url) as client:
            responses = await asyncio.gather(*(client.get("/ping") for _ in range(20)))
        assert [r.text for r in responses] == ["pong"] * 20

    async def test_client_disconnect_mid_stream_does_not_kill_the_server(
        self, starlette_url
    ):
        # Abandon a streaming response part-way, then prove the server is
        # still healthy on a fresh connection.
        with contextlib.suppress(Exception):
            async with httpx.AsyncClient(base_url=starlette_url) as client:
                async with client.stream("GET", "/large") as response:
                    await response.aiter_bytes().__anext__()

        async with httpx.AsyncClient(base_url=starlette_url) as client:
            assert (await client.get("/ping")).text == "pong"
