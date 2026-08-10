"""An unmodified Starlette WebSocket application served by Sonix's server.

The strongest available check on the frame codec and the ASGI websocket
bridge: an independently written application, driven by an independently
written client (the `websockets` package), agreeing with our implementation
on the wire. Our own tests can only show the codec agrees with itself.

Kept apart from test_starlette_on_sonix.py so the HTTP module stays about
HTTP, and because this one needs a websocket client the others do not.
"""

from __future__ import annotations

import asyncio
import json

import pytest
import websockets
from starlette.applications import Starlette
from starlette.routing import WebSocketRoute
from starlette.websockets import WebSocket, WebSocketDisconnect
from websockets.typing import Subprotocol

from conformance.support import serve_on_sonix

TIMEOUT = 5


async def echo(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            await websocket.send_text(await websocket.receive_text())
    except WebSocketDisconnect:
        return


async def echo_json(websocket: WebSocket):
    await websocket.accept()
    await websocket.send_json({"echoed": await websocket.receive_json()})


async def echo_bytes(websocket: WebSocket):
    await websocket.accept()
    await websocket.send_bytes(await websocket.receive_bytes())


async def with_params(websocket: WebSocket):
    await websocket.accept()
    await websocket.send_json(
        {
            "room": websocket.path_params["room"],
            "token": websocket.query_params.get("token"),
        }
    )


async def negotiate(websocket: WebSocket):
    await websocket.accept(subprotocol=websocket.scope["subprotocols"][0])
    await websocket.send_text("negotiated")


async def deny(websocket: WebSocket):
    await websocket.close(code=1008)


async def boom(websocket: WebSocket):
    await websocket.accept()
    raise RuntimeError("deliberate failure")


async def server_closes(websocket: WebSocket):
    await websocket.accept()
    await websocket.close(code=1001, reason="going away")


app = Starlette(
    routes=[
        WebSocketRoute("/echo", echo),
        WebSocketRoute("/echo-json", echo_json),
        WebSocketRoute("/echo-bytes", echo_bytes),
        WebSocketRoute("/rooms/{room:int}", with_params),
        WebSocketRoute("/negotiate", negotiate),
        WebSocketRoute("/deny", deny),
        WebSocketRoute("/boom", boom),
        WebSocketRoute("/server-closes", server_closes),
    ]
)


def ws_url(base_url: str, path: str) -> str:
    return base_url.replace("http://", "ws://", 1) + path


class TestStarletteWebSocketsOnSonix:
    async def test_text_echo(self):
        async with serve_on_sonix(app) as base_url:
            url = ws_url(base_url, "/echo")
            async with websockets.connect(url) as client:
                await client.send("hello from starlette")
                received = await asyncio.wait_for(client.recv(), TIMEOUT)
        assert received == "hello from starlette"

    async def test_binary_echo(self):
        async with serve_on_sonix(app) as base_url:
            url = ws_url(base_url, "/echo-bytes")
            async with websockets.connect(url) as client:
                await client.send(b"\x00\xff\xfe")
                received = await asyncio.wait_for(client.recv(), TIMEOUT)
        assert received == b"\x00\xff\xfe"

    async def test_json_echo(self):
        async with serve_on_sonix(app) as base_url:
            url = ws_url(base_url, "/echo-json")
            async with websockets.connect(url) as client:
                await client.send('{"a": 1}')
                received = await asyncio.wait_for(client.recv(), TIMEOUT)
        assert json.loads(received) == {"echoed": {"a": 1}}

    async def test_path_and_query_params(self):
        async with serve_on_sonix(app) as base_url:
            url = ws_url(base_url, "/rooms/7?token=abc")
            async with websockets.connect(url) as client:
                received = await asyncio.wait_for(client.recv(), TIMEOUT)
        assert json.loads(received) == {"room": 7, "token": "abc"}

    async def test_subprotocol_negotiation(self):
        offered = [Subprotocol("chat"), Subprotocol("superchat")]
        async with serve_on_sonix(app) as base_url:
            url = ws_url(base_url, "/negotiate")
            async with websockets.connect(url, subprotocols=offered) as client:
                assert client.subprotocol == "chat"
                received = await asyncio.wait_for(client.recv(), TIMEOUT)
        assert received == "negotiated"

    async def test_a_message_larger_than_one_frame_length_form(self):
        payload = "x" * 70000
        async with serve_on_sonix(app) as base_url:
            url = ws_url(base_url, "/echo")
            async with websockets.connect(url) as client:
                await client.send(payload)
                received = await asyncio.wait_for(client.recv(), TIMEOUT)
        assert received == payload

    async def test_close_before_accept_is_a_403(self):
        async with serve_on_sonix(app) as base_url:
            with pytest.raises(websockets.exceptions.InvalidStatus) as excinfo:
                await websockets.connect(ws_url(base_url, "/deny"))
        assert excinfo.value.response.status_code == 403

    async def test_unmatched_path_is_refused(self):
        async with serve_on_sonix(app) as base_url:
            with pytest.raises(websockets.exceptions.InvalidStatus):
                await websockets.connect(ws_url(base_url, "/nope"))

    async def test_raising_handler_closes_the_connection(self):
        async with serve_on_sonix(app) as base_url:
            url = ws_url(base_url, "/boom")
            async with websockets.connect(url) as client:
                with pytest.raises(websockets.exceptions.ConnectionClosed):
                    await asyncio.wait_for(client.recv(), TIMEOUT)

    async def test_server_initiated_close_carries_its_code(self):
        async with serve_on_sonix(app) as base_url:
            url = ws_url(base_url, "/server-closes")
            async with websockets.connect(url) as client:
                with pytest.raises(websockets.exceptions.ConnectionClosedOK) as info:
                    await asyncio.wait_for(client.recv(), TIMEOUT)
        close = info.value.rcvd
        assert close is not None
        assert close.code == 1001
        assert close.reason == "going away"

    async def test_client_initiated_close_completes_the_handshake(self):
        async with serve_on_sonix(app) as base_url:
            url = ws_url(base_url, "/echo")
            async with websockets.connect(url) as client:
                await client.send("still here")
                await asyncio.wait_for(client.recv(), TIMEOUT)
                await client.close(code=1000, reason="done")
            assert client.close_code == 1000

    async def test_client_pings_are_answered(self):
        # The `websockets` client's keepalive relies on this; a server that
        # ignored pings would have its connections dropped by any real client.
        async with serve_on_sonix(app) as base_url:
            url = ws_url(base_url, "/echo")
            async with websockets.connect(url) as client:
                pong_waiter = await client.ping(b"nonce")
                await asyncio.wait_for(pong_waiter, TIMEOUT)

    async def test_many_concurrent_connections(self):
        async with serve_on_sonix(app) as base_url:
            url = ws_url(base_url, "/echo")
            clients = await asyncio.gather(
                *(websockets.connect(url) for _ in range(10))
            )
            try:
                await asyncio.gather(
                    *(client.send(f"m{i}") for i, client in enumerate(clients))
                )
                received = await asyncio.wait_for(
                    asyncio.gather(*(client.recv() for client in clients)), TIMEOUT
                )
                assert received == [f"m{i}" for i in range(10)]
            finally:
                await asyncio.gather(*(client.close() for client in clients))
