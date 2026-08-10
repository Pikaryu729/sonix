"""WebSockets end to end: a real socket, both layers, no fakes.

At the top level rather than under tests/server/ or tests/app/ because it is
the first module that deliberately exercises *both* -- an @app.websocket
handler, through the router and the exception layer, through the frame codec,
onto a TCP connection and back. Everything else in the suite stays on one
side of the line on purpose.

The client is tests/wsclient.py, built on the server's own codec. Every wait
is bounded, so a hung handler fails the test instead of the run.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import struct

import pytest
from wsclient import HandshakeRejected, WSTestClient

from sonix import Sonix
from sonix.app.exceptions import WebSocketException
from sonix.app.websockets import WebSocket, WebSocketDisconnect
from sonix.server.server import Config, Server
from sonix.server.websockets import Opcode, encode_close_frame, encode_frame

TIMEOUT = 5


@contextlib.asynccontextmanager
async def serving(app, **config_kwargs):
    server = Server(Config(app, host="127.0.0.1", port=0, **config_kwargs))
    await server.startup()
    try:
        host, port = server.sockets[0].getsockname()[:2]
        yield host, port
    finally:
        await asyncio.wait_for(server.shutdown(), timeout=TIMEOUT)


def echo_app(**config_kwargs) -> Sonix:
    app = Sonix(**config_kwargs)

    @app.websocket("/ws")
    async def echo(websocket: WebSocket):
        await websocket.accept()
        try:
            while True:
                message = await websocket.receive()
                if message["type"] == "websocket.disconnect":
                    return
                if message.get("text") is not None:
                    await websocket.send_text(message["text"])
                else:
                    await websocket.send_bytes(message["bytes"])
        except WebSocketDisconnect:
            return

    return app


class TestRoundTrip:
    async def test_text(self):
        async with (
            serving(echo_app()) as (host, port),
            await WSTestClient.connect(host, port) as client,
        ):
            await client.send_text("héllo")
            assert await asyncio.wait_for(client.receive_text(), TIMEOUT) == "héllo"

    async def test_binary(self):
        async with (
            serving(echo_app()) as (host, port),
            await WSTestClient.connect(host, port) as client,
        ):
            await client.send_bytes(b"\x00\xff\xfe")
            assert await asyncio.wait_for(client.receive_bytes(), TIMEOUT) == (
                b"\x00\xff\xfe"
            )

    async def test_json(self):
        app = Sonix()

        @app.websocket("/ws")
        async def handler(websocket: WebSocket):
            await websocket.accept()
            await websocket.send_json({"echo": await websocket.receive_json()})

        async with (
            serving(app) as (host, port),
            await WSTestClient.connect(host, port) as client,
        ):
            await client.send_json({"a": 1})
            assert await asyncio.wait_for(client.receive_json(), TIMEOUT) == {
                "echo": {"a": 1}
            }

    async def test_many_messages_in_sequence(self):
        async with (
            serving(echo_app()) as (host, port),
            await WSTestClient.connect(host, port) as client,
        ):
            for index in range(50):
                await client.send_text(str(index))
                assert await asyncio.wait_for(client.receive_text(), TIMEOUT) == (
                    str(index)
                )

    async def test_large_message(self):
        payload = b"x" * (256 * 1024)
        async with (
            serving(echo_app()) as (host, port),
            await WSTestClient.connect(host, port) as client,
        ):
            await client.send_bytes(payload)
            assert await asyncio.wait_for(client.receive_bytes(), TIMEOUT) == (payload)

    async def test_client_fragmented_message_is_reassembled(self):
        async with (
            serving(echo_app()) as (host, port),
            await WSTestClient.connect(host, port) as client,
        ):
            await client.send_frame(Opcode.TEXT, b"he", fin=False)
            await client.send_frame(Opcode.CONTINUATION, b"llo")
            assert await asyncio.wait_for(client.receive_text(), TIMEOUT) == "hello"

    async def test_ping_is_answered_without_disturbing_the_stream(self):
        async with (
            serving(echo_app()) as (host, port),
            await WSTestClient.connect(host, port) as client,
        ):
            await client.send_frame(Opcode.PING, b"nonce")
            await client.send_text("after")
            # receive_text skips ping/pong, exactly as an app never sees it.
            assert await asyncio.wait_for(client.receive_text(), TIMEOUT) == "after"


class TestRoutingAndInjection:
    async def test_path_and_query_parameters_reach_the_handler(self):
        app = Sonix()

        @app.websocket("/rooms/{room:int}")
        async def handler(websocket: WebSocket, room: int, tag: list[str]):
            await websocket.accept()
            await websocket.send_json({"room": room, "tag": tag})

        async with (
            serving(app) as (host, port),
            await WSTestClient.connect(host, port, "/rooms/7?tag=a&tag=b") as client,
        ):
            assert await asyncio.wait_for(client.receive_json(), TIMEOUT) == {
                "room": 7,
                "tag": ["a", "b"],
            }

    async def test_subprotocol_negotiation(self):
        app = Sonix()

        @app.websocket("/ws")
        async def handler(websocket: WebSocket):
            await websocket.accept(subprotocol=websocket.subprotocols[0])
            await websocket.send_text("ok")

        async with (
            serving(app) as (host, port),
            await WSTestClient.connect(
                host, port, subprotocols=["chat", "superchat"]
            ) as client,
        ):
            assert await asyncio.wait_for(client.receive_text(), TIMEOUT) == "ok"

    async def test_lifespan_state_reaches_a_websocket_handler(self):
        sentinel = object()

        @contextlib.asynccontextmanager
        async def lifespan(app):
            yield {"db": sentinel}

        app = Sonix(lifespan=lifespan)
        seen = []

        @app.websocket("/ws")
        async def handler(websocket: WebSocket):
            seen.append(websocket.state["db"])
            await websocket.accept()
            await websocket.close()

        async with (
            serving(app) as (host, port),
            await WSTestClient.connect(host, port) as client,
        ):
            await asyncio.wait_for(client.receive_close(), TIMEOUT)
        assert seen == [sentinel]

    async def test_http_and_websocket_routes_coexist_on_one_path(self):
        app = Sonix()

        @app.get("/thing")
        async def http_handler(request):
            return "over http"

        @app.websocket("/thing")
        async def ws_handler(websocket: WebSocket):
            await websocket.accept()
            await websocket.send_text("over websocket")

        async with serving(app) as (host, port):
            reader, writer = await asyncio.open_connection(host, port)
            writer.write(b"GET /thing HTTP/1.1\r\nHost: e.com\r\n\r\n")
            await writer.drain()
            data = await asyncio.wait_for(reader.read(4096), TIMEOUT)
            writer.close()
            await writer.wait_closed()
            assert data.endswith(b"over http")

            async with await WSTestClient.connect(host, port, "/thing") as client:
                assert await asyncio.wait_for(client.receive_text(), TIMEOUT) == (
                    "over websocket"
                )


class TestRefusals:
    async def test_unmatched_path_is_a_403(self):
        # Not a 404: before accept, the only refusal a websocket understands
        # is a close, and ASGI renders a pre-accept close as HTTP 403.
        async with serving(echo_app()) as (host, port):
            with pytest.raises(HandshakeRejected) as excinfo:
                await WSTestClient.connect(host, port, "/nope")
            assert excinfo.value.status == 403

    async def test_handler_closing_before_accept_is_a_403(self):
        app = Sonix()

        @app.websocket("/ws")
        async def handler(websocket: WebSocket):
            await websocket.close(code=1008, reason="no")

        async with serving(app) as (host, port):
            with pytest.raises(HandshakeRejected) as excinfo:
                await WSTestClient.connect(host, port)
            assert excinfo.value.status == 403

    async def test_websocket_exception_after_accept_closes_with_its_code(self):
        app = Sonix()

        @app.websocket("/ws")
        async def handler(websocket: WebSocket):
            await websocket.accept()
            raise WebSocketException(4001, "not allowed")

        async with (
            serving(app) as (host, port),
            await WSTestClient.connect(host, port) as client,
        ):
            close = await asyncio.wait_for(client.receive_close(), TIMEOUT)
            assert close.code == 4001
            assert close.reason == "not allowed"

    async def test_a_raising_handler_closes_with_1011(self, capsys):
        app = Sonix()

        @app.websocket("/ws")
        async def handler(websocket: WebSocket):
            await websocket.accept()
            raise RuntimeError("deliberate failure")

        async with (
            serving(app) as (host, port),
            await WSTestClient.connect(host, port) as client,
        ):
            close = await asyncio.wait_for(client.receive_close(), TIMEOUT)
            assert close.code == 1011
        assert "deliberate failure" in capsys.readouterr().err

    async def test_a_bad_query_parameter_closes_with_1008(self):
        # Dependency injection's 422 has nowhere to go on a websocket, so the
        # exception layer turns it into a close carrying the detail.
        app = Sonix()

        @app.websocket("/ws")
        async def handler(websocket: WebSocket, limit: int):
            await websocket.accept()

        async with serving(app) as (host, port):
            with pytest.raises(HandshakeRejected) as excinfo:
                await WSTestClient.connect(host, port, "/ws?limit=abc")
            assert excinfo.value.status == 403

    async def test_oversized_message_closes_with_1009(self):
        async with (
            serving(echo_app(), websocket_max_message_size=1024) as (
                host,
                port,
            ),
            await WSTestClient.connect(host, port) as client,
        ):
            await client.send_bytes(b"x" * 4096)
            close = await asyncio.wait_for(client.receive_close(), TIMEOUT)
            assert close.code == 1009

    async def test_unmasked_client_frame_closes_with_1002(self):
        async with (
            serving(echo_app()) as (host, port),
            await WSTestClient.connect(host, port) as client,
        ):
            # RFC 6455 requires clients to mask; send_raw bypasses that.
            await client.send_raw(encode_frame(Opcode.TEXT, b"unmasked"))
            close = await asyncio.wait_for(client.receive_close(), TIMEOUT)
            assert close.code == 1002

    async def test_invalid_utf8_in_a_text_frame_closes_with_1007(self):
        async with (
            serving(echo_app()) as (host, port),
            await WSTestClient.connect(host, port) as client,
        ):
            await client.send_frame(Opcode.TEXT, b"\xff\xfe")
            close = await asyncio.wait_for(client.receive_close(), TIMEOUT)
            assert close.code == 1007


class TestClosing:
    async def test_client_initiated_close_is_echoed(self):
        async with (
            serving(echo_app()) as (host, port),
            await WSTestClient.connect(host, port) as client,
        ):
            await client.close(1001, "bye")
            close = await asyncio.wait_for(client.receive_close(), TIMEOUT)
            assert close.code == 1001

    async def test_server_initiated_close(self):
        app = Sonix()

        @app.websocket("/ws")
        async def handler(websocket: WebSocket):
            await websocket.accept()
            await websocket.close(1001, "later")

        async with (
            serving(app) as (host, port),
            await WSTestClient.connect(host, port) as client,
        ):
            close = await asyncio.wait_for(client.receive_close(), TIMEOUT)
            assert close.code == 1001
            assert close.reason == "later"

    async def test_handler_returning_ends_the_session(self):
        app = Sonix()

        @app.websocket("/ws")
        async def handler(websocket: WebSocket):
            await websocket.accept()
            await websocket.send_text("one")

        async with (
            serving(app) as (host, port),
            await WSTestClient.connect(host, port) as client,
        ):
            assert await asyncio.wait_for(client.receive_text(), TIMEOUT) == "one"
            close = await asyncio.wait_for(client.receive_close(), TIMEOUT)
            assert close.code == 1000

    async def test_a_message_sent_with_a_violation_is_echoed_before_the_close(self):
        # The real-socket proof for the seven Autobahn NON-STRICT cases, and
        # the closest local proxy for them since the suite needs Docker. Both
        # frames go out in a single write, so they reach the server in one
        # data_received: a valid masked TEXT, then an unmasked one the codec
        # must reject with 1002.
        async with (
            serving(echo_app()) as (host, port),
            await WSTestClient.connect(host, port) as client,
        ):
            await client.send_raw(
                encode_frame(Opcode.TEXT, b"last", mask=os.urandom(4))
                + encode_frame(Opcode.TEXT, b"bad")
            )
            assert await asyncio.wait_for(client.receive_text(), TIMEOUT) == "last"
            close = await asyncio.wait_for(client.receive_close(), TIMEOUT)
            assert close.code == 1002

    async def test_a_message_sent_with_a_close_is_echoed_before_the_echo_close(self):
        # The ordinary-traffic version: a client saying one last thing and
        # then closing, in one write.
        async with (
            serving(echo_app()) as (host, port),
            await WSTestClient.connect(host, port) as client,
        ):
            await client.send_raw(
                encode_frame(Opcode.TEXT, b"bye", mask=os.urandom(4))
                + encode_close_frame(1000, "done", mask=os.urandom(4))
            )
            assert await asyncio.wait_for(client.receive_text(), TIMEOUT) == "bye"
            close = await asyncio.wait_for(client.receive_close(), TIMEOUT)
            assert close.code == 1000

    async def test_handler_sees_the_client_going_away(self):
        app = Sonix()
        codes: list[int] = []

        @app.websocket("/ws")
        async def handler(websocket: WebSocket):
            await websocket.accept()
            try:
                await websocket.receive_text()
            except WebSocketDisconnect as exc:
                codes.append(exc.code)

        async with serving(app) as (host, port):
            client = await WSTestClient.connect(host, port)
            await client.disconnect()
            for _ in range(50):
                if codes:
                    break
                await asyncio.sleep(0.02)
        assert codes == [1006]

    async def test_client_close_code_reaches_the_handler(self):
        app = Sonix()
        codes: list[int] = []

        @app.websocket("/ws")
        async def handler(websocket: WebSocket):
            await websocket.accept()
            try:
                await websocket.receive_text()
            except WebSocketDisconnect as exc:
                codes.append(exc.code)

        async with (
            serving(app) as (host, port),
            await WSTestClient.connect(host, port) as client,
        ):
            await client.close(1001, "bye")
            await asyncio.wait_for(client.receive_close(), TIMEOUT)
        assert codes == [1001]

    async def test_iter_text_ends_cleanly_at_disconnect(self):
        app = Sonix()
        collected: list[str] = []

        @app.websocket("/ws")
        async def handler(websocket: WebSocket):
            await websocket.accept()
            collected.extend([message async for message in websocket.iter_text()])

        async with (
            serving(app) as (host, port),
            await WSTestClient.connect(host, port) as client,
        ):
            await client.send_text("a")
            await client.send_text("b")
            await client.close()
            await asyncio.wait_for(client.receive_close(), TIMEOUT)
        for _ in range(50):
            if collected == ["a", "b"]:
                break
            await asyncio.sleep(0.02)
        assert collected == ["a", "b"]


class TestConcurrencyAndShutdown:
    async def test_twenty_concurrent_clients(self):
        async with serving(echo_app()) as (host, port):
            clients = await asyncio.gather(
                *(WSTestClient.connect(host, port) for _ in range(20))
            )
            try:
                await asyncio.gather(
                    *(client.send_text(f"m{i}") for i, client in enumerate(clients))
                )
                received = await asyncio.wait_for(
                    asyncio.gather(*(client.receive_text() for client in clients)),
                    TIMEOUT,
                )
                assert received == [f"m{i}" for i in range(20)]
            finally:
                await asyncio.gather(*(client.disconnect() for client in clients))

    async def test_shutdown_closes_live_websockets_with_1001(self):
        server = Server(Config(echo_app(), host="127.0.0.1", port=0))
        await server.startup()
        host, port = server.sockets[0].getsockname()[:2]
        async with await WSTestClient.connect(host, port) as client:
            await client.send_text("alive")
            assert await asyncio.wait_for(client.receive_text(), TIMEOUT) == "alive"
            await asyncio.wait_for(server.shutdown(), timeout=TIMEOUT)
            close = await asyncio.wait_for(client.receive_close(), TIMEOUT)
            assert close.code == 1001


class TestPipeliningAcrossTheUpgrade:
    async def test_a_request_pipelined_ahead_of_the_handshake_is_answered_first(self):
        app = echo_app()

        @app.get("/first")
        async def first(request):
            return "first"

        async with serving(app) as (host, port):
            reader, writer = await asyncio.open_connection(host, port)
            handshake = (
                b"GET /ws HTTP/1.1\r\n"
                b"Host: e.com\r\n"
                b"Upgrade: websocket\r\n"
                b"Connection: Upgrade\r\n"
                b"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                b"Sec-WebSocket-Version: 13\r\n\r\n"
            )
            writer.write(b"GET /first HTTP/1.1\r\nHost: e.com\r\n\r\n" + handshake)
            await writer.drain()
            data = b""
            while b"101 Switching Protocols" not in data:
                chunk = await asyncio.wait_for(reader.read(4096), TIMEOUT)
                assert chunk, "connection closed before the upgrade"
                data += chunk
            writer.close()
            await writer.wait_closed()
            assert data.index(b"200 OK") < data.index(b"101 Switching Protocols")

    async def test_a_request_smuggled_behind_the_handshake_is_never_run(self):
        # The parser's UPGRADED state is what makes this true: the smuggled
        # request stays inert bytes rather than becoming a dispatched request
        # on a connection the client believes is a tunnel.
        app = echo_app()
        admin_calls: list[int] = []

        @app.get("/admin")
        async def admin(request):
            admin_calls.append(1)
            return "secret"

        async with serving(app) as (host, port):
            reader, writer = await asyncio.open_connection(host, port)
            handshake = (
                b"GET /ws HTTP/1.1\r\n"
                b"Host: e.com\r\n"
                b"Upgrade: websocket\r\n"
                b"Connection: Upgrade\r\n"
                b"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                b"Sec-WebSocket-Version: 13\r\n\r\n"
            )
            writer.write(handshake + b"GET /admin HTTP/1.1\r\nHost: e.com\r\n\r\n")
            await writer.drain()
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), TIMEOUT)
            assert head.startswith(b"HTTP/1.1 101 ")
            # The smuggled bytes are fed to the frame parser, which rejects
            # them as unmasked -- a close, not a second response.
            rest = await asyncio.wait_for(reader.read(4096), TIMEOUT)
            writer.close()
            await writer.wait_closed()
            assert b"secret" not in rest
            assert not admin_calls
            assert rest[0] == 0x88  # a close frame
            assert struct.unpack_from("!H", rest, 2)[0] == 1002
