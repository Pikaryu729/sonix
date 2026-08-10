"""A Sonix application run by uvicorn, in a real subprocess.

The other half of the two-layer claim. `sonix/app/**` never imports
`sonix.server` -- tests/test_layering.py enforces that statically -- but only
this test shows what the rule actually buys: the application layer runs on
somebody else's server.

Until lifespan support landed, this test was impossible. Sonix.__call__
delegated to the router unconditionally, so uvicorn's lifespan scope reached
Router.__call__ and died on scope["path"] with a KeyError before a single
request could be served.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys

import httpx
import pytest
import websockets

from conformance.support import UVICORN_CMD, free_port, wait_until_serving

TARGET = "conformance.sonix_target:app"

# tests/ is what makes `conformance.sonix_target` importable. The subprocess
# does not inherit pytest's pythonpath setting, so it is passed explicitly
# rather than relying on the working directory.
TESTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

STARTUP_TIMEOUT = 60.0


@pytest.fixture
async def uvicorn_url():
    port = free_port()
    env = {**os.environ, "PYTHONPATH": TESTS_DIR}
    # ASYNC220: Popen spawns and returns; it does not wait. The blocking wait
    # is in teardown below, where there is nothing else for the loop to do.
    process = subprocess.Popen(  # noqa: ASYNC220
        [
            *UVICORN_CMD,
            TARGET,
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        async with asyncio.timeout(STARTUP_TIMEOUT):
            await wait_until_serving(port, process)
        yield f"http://127.0.0.1:{port}"
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:  # pragma: no cover - defensive
            process.kill()
            process.wait(timeout=10)


class TestSonixUnderUvicorn:
    async def test_simple_get(self, uvicorn_url):
        async with httpx.AsyncClient(base_url=uvicorn_url) as client:
            response = await client.get("/ping")
        assert response.status_code == 200
        assert response.text == "pong"

    async def test_typed_path_parameter_is_coerced(self, uvicorn_url):
        async with httpx.AsyncClient(base_url=uvicorn_url) as client:
            response = await client.get("/items/42")
        assert response.json() == {"item_id": 42}

    async def test_non_matching_path_parameter_is_404(self, uvicorn_url):
        # The int converter's regex is what rejects this, not a coercion
        # failure after the fact -- behavior must be identical off-Sonix.
        async with httpx.AsyncClient(base_url=uvicorn_url) as client:
            response = await client.get("/items/not-an-int")
        assert response.status_code == 404

    async def test_query_parameters(self, uvicorn_url):
        async with httpx.AsyncClient(base_url=uvicorn_url) as client:
            response = await client.get("/query", params={"q": "hello"})
        assert response.json() == {"q": "hello"}

    async def test_request_body(self, uvicorn_url):
        async with httpx.AsyncClient(base_url=uvicorn_url) as client:
            response = await client.post("/echo", json={"a": [1, 2]})
        assert response.json() == {"echoed": {"a": [1, 2]}}

    async def test_uvicorn_drives_sonix_lifespan(self, uvicorn_url):
        # The regression guard for the KeyError this whole test was blocked
        # by: if lifespan were broken, uvicorn would never have started.
        async with httpx.AsyncClient(base_url=uvicorn_url) as client:
            response = await client.get("/state")
        assert response.json() == {"greeting": "from lifespan"}

    async def test_http_exception_becomes_its_status(self, uvicorn_url):
        async with httpx.AsyncClient(base_url=uvicorn_url) as client:
            response = await client.get("/teapot")
        assert response.status_code == 418

    async def test_405_carries_allow(self, uvicorn_url):
        async with httpx.AsyncClient(base_url=uvicorn_url) as client:
            response = await client.post("/ping")
        assert response.status_code == 405
        assert response.headers["allow"] == "GET"

    async def test_unhandled_exception_becomes_500(self, uvicorn_url):
        async with httpx.AsyncClient(base_url=uvicorn_url) as client:
            response = await client.get("/boom")
        assert response.status_code == 500

    async def test_server_survives_a_handler_failure(self, uvicorn_url):
        async with httpx.AsyncClient(base_url=uvicorn_url) as client:
            await client.get("/boom")
            assert (await client.get("/ping")).text == "pong"

    async def test_concurrent_requests(self, uvicorn_url):
        async with httpx.AsyncClient(base_url=uvicorn_url) as client:
            responses = await asyncio.gather(*(client.get("/ping") for _ in range(20)))
        assert all(r.text == "pong" for r in responses)


def test_uvicorn_module_is_runnable():
    # Guards the fixture: a bad command line would otherwise surface as an
    # opaque startup timeout.
    result = subprocess.run(
        [*UVICORN_CMD, "--version"],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert sys.executable


class TestSonixWebSocketsUnderUvicorn:
    """The app layer speaking ASGI websockets on somebody else's server.

    Sonix's own bridge is not involved at all here: uvicorn does the
    handshake and the framing, and a @app.websocket handler runs unchanged.
    """

    @staticmethod
    def ws_url(base_url: str, path: str) -> str:
        return base_url.replace("http://", "ws://", 1) + path

    async def test_echo(self, uvicorn_url):
        async with websockets.connect(self.ws_url(uvicorn_url, "/ws/echo")) as client:
            await client.send("hello")
            assert await asyncio.wait_for(client.recv(), 5) == "hello"

    async def test_path_and_query_injection(self, uvicorn_url):
        url = self.ws_url(uvicorn_url, "/ws/rooms/7?token=abc")
        async with websockets.connect(url) as client:
            payload = await asyncio.wait_for(client.recv(), 5)
        assert json.loads(payload) == {"room": 7, "token": "abc"}

    async def test_lifespan_state_reaches_a_websocket_handler(self, uvicorn_url):
        async with websockets.connect(self.ws_url(uvicorn_url, "/ws/state")) as client:
            payload = await asyncio.wait_for(client.recv(), 5)
        assert json.loads(payload) == {"greeting": "from lifespan"}

    async def test_close_before_accept_is_refused(self, uvicorn_url):
        with pytest.raises(websockets.exceptions.InvalidStatus):
            await websockets.connect(self.ws_url(uvicorn_url, "/ws/deny"))

    async def test_unmatched_websocket_path_is_refused(self, uvicorn_url):
        with pytest.raises(websockets.exceptions.InvalidStatus):
            await websockets.connect(self.ws_url(uvicorn_url, "/ws/nope"))

    async def test_an_http_request_to_a_websocket_path_is_404(self, uvicorn_url):
        # Not a 405 with an empty Allow header.
        async with httpx.AsyncClient(base_url=uvicorn_url) as client:
            assert (await client.get("/ws/echo")).status_code == 404
