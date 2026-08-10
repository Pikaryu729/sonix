"""The app-layer WebSocket, against a hand-built scope and fake receive/send.

No socket, no frame, no server -- which is the point. If any of these tests
needed a byte on a wire, the two-layer split would not be real.
"""

import pytest
from helpers import make_receive, make_send, make_ws_scope

from sonix.app.websockets import (
    WebSocket,
    WebSocketDisconnect,
    WebSocketState,
)


def make_websocket(incoming):
    send, sent = make_send()
    ws = WebSocket(make_ws_scope(), make_receive(incoming), send)
    return ws, sent


CONNECT = {"type": "websocket.connect"}


def text(value):
    return {"type": "websocket.receive", "text": value}


def data(value):
    return {"type": "websocket.receive", "bytes": value}


def disconnect(code=1000, reason=""):
    return {"type": "websocket.disconnect", "code": code, "reason": reason}


class TestHandshake:
    async def test_accept_consumes_connect_first(self):
        # ASGI requires the app to consume websocket.connect before
        # answering it. Doing that inside accept() is why this class exists.
        ws, sent = make_websocket([CONNECT])
        await ws.accept()
        assert sent == [{"type": "websocket.accept", "subprotocol": None}]
        assert ws.client_state is WebSocketState.CONNECTED
        assert ws.application_state is WebSocketState.CONNECTED

    async def test_accept_with_subprotocol_and_headers(self):
        ws, sent = make_websocket([CONNECT])
        await ws.accept(subprotocol="chat", headers=[(b"x-trace", b"abc")])
        assert sent == [
            {
                "type": "websocket.accept",
                "subprotocol": "chat",
                "headers": [(b"x-trace", b"abc")],
            }
        ]

    async def test_offered_subprotocols_come_from_the_scope(self):
        send, _ = make_send()
        ws = WebSocket(
            make_ws_scope(subprotocols=["chat", "superchat"]),
            make_receive([CONNECT]),
            send,
        )
        assert ws.subprotocols == ["chat", "superchat"]

    async def test_subprotocols_default_to_empty(self):
        send, _ = make_send()
        scope = make_ws_scope()
        del scope["subprotocols"]
        assert WebSocket(scope, make_receive([]), send).subprotocols == []

    async def test_close_before_accept_is_a_denial(self):
        ws, sent = make_websocket([CONNECT])
        await ws.close(code=1008, reason="nope")
        assert sent == [{"type": "websocket.close", "code": 1008, "reason": "nope"}]


class TestReceiving:
    async def test_receive_text(self):
        ws, _ = make_websocket([CONNECT, text("hello")])
        await ws.accept()
        assert await ws.receive_text() == "hello"

    async def test_receive_bytes(self):
        ws, _ = make_websocket([CONNECT, data(b"\x00\xff")])
        await ws.accept()
        assert await ws.receive_bytes() == b"\x00\xff"

    async def test_receive_json(self):
        ws, _ = make_websocket([CONNECT, text('{"a": 1}')])
        await ws.accept()
        assert await ws.receive_json() == {"a": 1}

    async def test_disconnect_raises_with_the_code(self):
        ws, _ = make_websocket([CONNECT, disconnect(1001, "bye")])
        await ws.accept()
        with pytest.raises(WebSocketDisconnect) as excinfo:
            await ws.receive_text()
        assert excinfo.value.code == 1001
        assert excinfo.value.reason == "bye"

    async def test_wrong_message_kind_is_a_bug_not_a_disconnect(self):
        ws, _ = make_websocket([CONNECT, data(b"binary")])
        await ws.accept()
        with pytest.raises(RuntimeError, match="expected a text message"):
            await ws.receive_text()

    async def test_receive_before_accept_is_refused(self):
        ws, _ = make_websocket([CONNECT, text("hello")])
        with pytest.raises(RuntimeError, match="accept\\(\\) must be awaited"):
            await ws.receive_text()

    async def test_receive_after_disconnect_is_refused(self):
        ws, _ = make_websocket([CONNECT, disconnect()])
        await ws.accept()
        with pytest.raises(WebSocketDisconnect):
            await ws.receive_text()
        with pytest.raises(RuntimeError, match="already disconnected"):
            await ws.receive()

    async def test_raw_receive_surfaces_the_disconnect_message(self):
        ws, _ = make_websocket([CONNECT, disconnect(1006)])
        await ws.accept()
        assert await ws.receive() == disconnect(1006)


class TestIteration:
    async def test_iter_text_stops_at_disconnect(self):
        ws, _ = make_websocket([CONNECT, text("a"), text("b"), disconnect()])
        await ws.accept()
        assert [message async for message in ws.iter_text()] == ["a", "b"]

    async def test_iter_bytes_stops_at_disconnect(self):
        ws, _ = make_websocket([CONNECT, data(b"a"), disconnect()])
        await ws.accept()
        assert [message async for message in ws.iter_bytes()] == [b"a"]


class TestSending:
    async def test_send_text_bytes_and_json(self):
        ws, sent = make_websocket([CONNECT])
        await ws.accept()
        await ws.send_text("hi")
        await ws.send_bytes(b"\x01")
        await ws.send_json({"a": 1})
        assert sent[1:] == [
            {"type": "websocket.send", "text": "hi"},
            {"type": "websocket.send", "bytes": b"\x01"},
            {"type": "websocket.send", "text": '{"a": 1}'},
        ]

    async def test_send_before_accept_is_refused(self):
        ws, _ = make_websocket([CONNECT])
        with pytest.raises(RuntimeError, match="accept\\(\\) must be awaited"):
            await ws.send_text("too early")


class TestClosing:
    async def test_close_sends_the_code_and_reason(self):
        ws, sent = make_websocket([CONNECT])
        await ws.accept()
        await ws.close(1001, "later")
        assert sent[-1] == {"type": "websocket.close", "code": 1001, "reason": "later"}

    async def test_close_is_idempotent(self):
        # `finally: await ws.close()` is the standard shape, and it must not
        # raise because the peer closed first.
        ws, sent = make_websocket([CONNECT])
        await ws.accept()
        await ws.close()
        await ws.close()
        assert len([m for m in sent if m["type"] == "websocket.close"]) == 1

    async def test_close_after_a_peer_disconnect_is_a_no_op(self):
        ws, sent = make_websocket([CONNECT, disconnect(1006)])
        await ws.accept()
        with pytest.raises(WebSocketDisconnect):
            await ws.receive_text()
        await ws.close()
        assert not [m for m in sent if m["type"] == "websocket.close"]


class TestScopeSurface:
    async def test_inherits_the_connection_surface_from_httpconnection(self):
        # Path and query parameters must be reachable the same way they are
        # on a Request, since that is what makes DI work here unchanged.
        send, _ = make_send()
        scope = make_ws_scope(
            path="/rooms/7",
            query_string=b"token=abc&tag=x&tag=y",
            path_params={"room": 7},
        )
        ws = WebSocket(scope, make_receive([]), send)
        assert ws.path == "/rooms/7"
        assert ws.query_params["token"] == "abc"
        assert ws.query_params.get_list("tag") == ["x", "y"]
        assert ws.path_params == {"room": 7}
        assert ws.headers["host"] == "example.com"
        assert ws.client == ("127.0.0.1", 54321)
        assert ws.state == {}

    async def test_has_no_method(self):
        send, _ = make_send()
        ws = WebSocket(make_ws_scope(), make_receive([]), send)
        assert not hasattr(ws, "method")
