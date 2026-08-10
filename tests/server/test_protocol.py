import asyncio
import struct

import pytest

from sonix.server.parser import RequestHead
from sonix.server.protocol import HTTPProtocol, build_scope
from sonix.server.websockets import (
    BinaryMessage,
    CloseReceived,
    FrameParser,
    Opcode,
    Pong,
    TextMessage,
    encode_frame,
)
from sonix.types import Message, Receive, Scope, Send


class FakeTransport(asyncio.Transport):
    def __init__(self, peername=("127.0.0.1", 54321), sockname=("127.0.0.1", 8000)):
        self.written = bytearray()
        self.closed = False
        self.paused = False
        self._peername = peername
        self._sockname = sockname

    def write(self, data: bytes | bytearray | memoryview) -> None:
        self.written.extend(data)

    def close(self) -> None:
        self.closed = True

    def is_closing(self) -> bool:
        return self.closed

    def get_extra_info(self, name, default=None):
        return {"peername": self._peername, "sockname": self._sockname}.get(
            name, default
        )

    def pause_reading(self) -> None:
        self.paused = True

    def resume_reading(self) -> None:
        self.paused = False


def make_protocol(app, **kwargs) -> tuple[HTTPProtocol, FakeTransport]:
    protocol = HTTPProtocol(app, **kwargs)
    transport = FakeTransport()
    protocol.connection_made(transport)
    return protocol, transport


async def fixed_response_app(scope: Scope, receive: Receive, send: Send) -> None:
    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [
                (b"content-type", b"text/plain"),
                (b"content-length", b"2"),
            ],
        }
    )
    await send({"type": "http.response.body", "body": b"ok", "more_body": False})


class TestScopeBuilding:
    def test_scope_shape(self):
        head = RequestHead(
            method="GET",
            target="/items/42?limit=10",
            path="/items/42",
            query_string=b"limit=10",
            http_version="1.1",
            headers=[(b"host", b"example.com")],
        )
        scope = build_scope(head, client=("1.2.3.4", 111), server=("127.0.0.1", 8000))
        assert scope == {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": "/items/42",
            "raw_path": b"/items/42",
            "query_string": b"limit=10",
            "root_path": "",
            "headers": [(b"host", b"example.com")],
            "client": ("1.2.3.4", 111),
            "server": ("127.0.0.1", 8000),
        }


class TestBasicRequestResponse:
    async def test_simple_get(self):
        protocol, transport = make_protocol(fixed_response_app)
        protocol.data_received(b"GET / HTTP/1.1\r\nHost: e.com\r\n\r\n")
        await asyncio.sleep(0.05)
        assert bytes(transport.written).startswith(b"HTTP/1.1 200 OK\r\n")
        assert transport.written.endswith(b"ok")

    async def test_status_line_echoes_request_http_version(self):
        protocol, transport = make_protocol(fixed_response_app)
        protocol.data_received(
            b"GET / HTTP/1.0\r\nHost: e.com\r\nConnection: keep-alive\r\n\r\n"
        )
        await asyncio.sleep(0.05)
        assert bytes(transport.written).startswith(b"HTTP/1.0 200 OK\r\n")


class TestKeepAlive:
    async def test_two_requests_over_one_connection_stay_open(self):
        protocol, transport = make_protocol(fixed_response_app)
        protocol.data_received(b"GET / HTTP/1.1\r\nHost: e.com\r\n\r\n")
        await asyncio.sleep(0.05)
        assert not transport.closed
        protocol.data_received(b"GET / HTTP/1.1\r\nHost: e.com\r\n\r\n")
        await asyncio.sleep(0.05)
        assert not transport.closed
        assert transport.written.count(b"HTTP/1.1 200 OK") == 2

    async def test_receive_queues_do_not_grow_across_sequential_requests(self):
        protocol, transport = make_protocol(fixed_response_app)
        for _ in range(20):
            protocol.data_received(b"GET / HTTP/1.1\r\nHost: e.com\r\n\r\n")
            await asyncio.sleep(0.01)
        assert not transport.closed
        assert len(protocol._receive_queues) == 0


class TestConnectionClose:
    async def test_connection_close_header_closes_after_response(self):
        protocol, transport = make_protocol(fixed_response_app)
        protocol.data_received(
            b"GET / HTTP/1.1\r\nHost: e.com\r\nConnection: close\r\n\r\n"
        )
        await asyncio.sleep(0.05)
        assert transport.closed
        assert b"Connection: close" in transport.written


class TestPipelining:
    async def test_responses_written_in_request_order_not_completion_order(self):
        gate = asyncio.Event()

        async def slow_first_app(scope: Scope, receive: Receive, send: Send) -> None:
            if scope["path"] == "/first":
                await gate.wait()
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [(b"content-length", b"5")],
                }
            )
            body = b"first" if scope["path"] == "/first" else b"secnd"
            await send({"type": "http.response.body", "body": body, "more_body": False})

        protocol, transport = make_protocol(slow_first_app)
        data = (
            b"GET /first HTTP/1.1\r\nHost: e.com\r\n\r\n"
            b"GET /second HTTP/1.1\r\nHost: e.com\r\n\r\n"
        )
        protocol.data_received(data)
        # Let /second's task run ahead and block on the turnstile.
        await asyncio.sleep(0.05)
        assert b"secnd" not in transport.written
        gate.set()
        await asyncio.sleep(0.05)
        assert transport.written.index(b"first") < transport.written.index(b"secnd")


class TestMalformedRequest:
    async def test_malformed_request_line_returns_400_and_closes(self):
        protocol, transport = make_protocol(fixed_response_app)
        protocol.data_received(b"GET/HTTP/1.1\r\nHost: e.com\r\n\r\n")
        await asyncio.sleep(0.05)
        assert bytes(transport.written).startswith(b"HTTP/1.1 400")
        assert transport.closed

        written_before = bytes(transport.written)
        protocol.data_received(b"more garbage")
        assert bytes(transport.written) == written_before

    async def test_good_request_then_malformed_still_gets_a_response(self):
        protocol, transport = make_protocol(fixed_response_app)
        data = (
            b"GET / HTTP/1.1\r\nHost: e.com\r\n\r\nGET/HTTP/1.1\r\nHost: e.com\r\n\r\n"
        )
        protocol.data_received(data)
        await asyncio.sleep(0.05)
        assert b"HTTP/1.1 200 OK" in transport.written
        assert b"HTTP/1.1 400" in transport.written
        assert transport.closed

    async def test_head_completes_then_body_fails_does_not_deadlock(self):
        # A pipelined request whose head parses fine but whose body framing
        # is malformed leaves a RequestHeadComplete in partial_events with
        # no matching RequestComplete. Dispatching that dangling head would
        # spawn an app task blocked forever in receive() (no more body
        # events will ever arrive for it), which would stall the write
        # turnstile and hang the connection -- verify this no longer
        # happens.
        drain_calls: list[str] = []

        async def draining_app(scope: Scope, receive: Receive, send: Send) -> None:
            drain_calls.append(scope["path"])
            while True:
                msg = await receive()
                if not msg.get("more_body", False):
                    break
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [(b"content-length", b"2")],
                }
            )
            await send(
                {"type": "http.response.body", "body": b"ok", "more_body": False}
            )

        protocol, transport = make_protocol(draining_app)
        data = (
            b"GET /a HTTP/1.1\r\nHost: e.com\r\n\r\n"
            b"POST /b HTTP/1.1\r\nHost: e.com\r\nTransfer-Encoding: chunked\r\n\r\n"
            b"ZZZ\r\n"
        )
        protocol.data_received(data)
        await asyncio.wait_for(asyncio.sleep(0.1), timeout=1)
        assert drain_calls == ["/a"]
        assert b"HTTP/1.1 200 OK" in transport.written
        assert b"HTTP/1.1 400" in transport.written
        assert transport.closed


class TestRequestTooLarge:
    async def test_oversized_headers_return_413(self):
        protocol, transport = make_protocol(fixed_response_app, max_header_size=16)
        protocol.data_received(b"GET / HTTP/1.1\r\nHost: " + b"a" * 100 + b"\r\n\r\n")
        await asyncio.sleep(0.05)
        assert bytes(transport.written).startswith(b"HTTP/1.1 413")
        assert transport.closed


class TestSlowLoris:
    async def test_idle_connection_times_out(self):
        _protocol, transport = make_protocol(fixed_response_app, head_timeout=0.05)
        await asyncio.sleep(0.15)
        assert transport.closed
        assert bytes(transport.written).startswith(b"HTTP/1.1 408")

    async def test_completed_head_disarms_timeout(self):
        protocol, transport = make_protocol(fixed_response_app, head_timeout=0.05)
        protocol.data_received(b"GET / HTTP/1.1\r\nHost: e.com\r\n\r\n")
        await asyncio.sleep(0.15)
        assert b"HTTP/1.1 408" not in transport.written


class TestDisconnect:
    async def test_connection_lost_delivers_disconnect_to_pending_receive(self):
        received: list[Message] = []
        got_first = asyncio.Event()
        got_second = asyncio.Event()

        async def slow_receive_app(scope: Scope, receive: Receive, send: Send) -> None:
            msg = await receive()
            received.append(msg)
            got_first.set()
            msg = await receive()
            received.append(msg)
            got_second.set()

        protocol, _transport = make_protocol(slow_receive_app)
        protocol.data_received(
            b"POST / HTTP/1.1\r\nHost: e.com\r\nContent-Length: 0\r\n\r\n"
        )
        await asyncio.wait_for(got_first.wait(), timeout=1)
        protocol.connection_lost(None)
        await asyncio.wait_for(got_second.wait(), timeout=1)
        assert received[-1] == {"type": "http.disconnect"}


class TestBodyStreaming:
    async def test_body_arrives_progressively_across_data_received_calls(self):
        messages: list[Message] = []
        done = asyncio.Event()

        async def recording_app(scope: Scope, receive: Receive, send: Send) -> None:
            while True:
                msg = await receive()
                messages.append(msg)
                if not msg.get("more_body", False):
                    break
            done.set()

        protocol, _transport = make_protocol(recording_app)
        protocol.data_received(
            b"POST / HTTP/1.1\r\nHost: e.com\r\nContent-Length: 6\r\n\r\n"
        )
        await asyncio.sleep(0.01)
        protocol.data_received(b"ab")
        await asyncio.sleep(0.01)
        protocol.data_received(b"cd")
        await asyncio.sleep(0.01)
        protocol.data_received(b"ef")
        await asyncio.wait_for(done.wait(), timeout=1)

        assert len(messages) >= 3
        assert b"".join(m["body"] for m in messages) == b"abcdef"
        assert messages[-1]["more_body"] is False


class TestSendBridgeErrors:
    async def test_body_before_start_raises_and_closes(self):
        async def broken_app(scope: Scope, receive: Receive, send: Send) -> None:
            await send(
                {"type": "http.response.body", "body": b"oops", "more_body": False}
            )

        protocol, transport = make_protocol(broken_app)
        protocol.data_received(b"GET / HTTP/1.1\r\nHost: e.com\r\n\r\n")
        await asyncio.sleep(0.05)
        assert transport.closed

    async def test_header_with_crlf_injection_raises_and_closes(self):
        async def injecting_app(scope: Scope, receive: Receive, send: Send) -> None:
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [(b"x-evil", b"value\r\nSet-Cookie: pwned=1")],
                }
            )

        protocol, transport = make_protocol(injecting_app)
        protocol.data_received(b"GET / HTTP/1.1\r\nHost: e.com\r\n\r\n")
        await asyncio.sleep(0.05)
        assert transport.closed
        assert b"Set-Cookie" not in transport.written

    async def test_content_length_and_transfer_encoding_together_raises(self):
        async def conflicting_app(scope: Scope, receive: Receive, send: Send) -> None:
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [
                        (b"content-length", b"2"),
                        (b"transfer-encoding", b"chunked"),
                    ],
                }
            )

        protocol, transport = make_protocol(conflicting_app)
        protocol.data_received(b"GET / HTTP/1.1\r\nHost: e.com\r\n\r\n")
        await asyncio.sleep(0.05)
        assert transport.closed
        assert transport.written == b""


class TestBackpressure:
    async def test_large_body_pauses_and_resumes_reading(self):
        release = asyncio.Event()
        done = asyncio.Event()

        async def draining_app(scope: Scope, receive: Receive, send: Send) -> None:
            await release.wait()
            while True:
                msg = await receive()
                if not msg.get("more_body", False):
                    break
            done.set()

        protocol, transport = make_protocol(
            draining_app, body_pause_watermark=2, body_resume_watermark=0
        )
        content_length = 5
        protocol.data_received(
            f"POST / HTTP/1.1\r\nHost: e.com\r\n"
            f"Content-Length: {content_length}\r\n\r\n".encode()
        )
        for byte in b"abcde":
            protocol.data_received(bytes([byte]))
        await asyncio.sleep(0.01)
        assert transport.paused
        release.set()
        await asyncio.wait_for(done.wait(), timeout=1)
        assert not transport.paused


@pytest.fixture
async def real_server():
    loop = asyncio.get_running_loop()
    server = await loop.create_server(
        lambda: HTTPProtocol(fixed_response_app), "127.0.0.1", 0
    )
    async with server:
        yield server.sockets[0].getsockname()


class TestRealSocketRoundTrip:
    async def test_real_socket_get_and_keep_alive(self, real_server):
        host, port = real_server
        reader, writer = await asyncio.open_connection(host, port)
        try:
            writer.write(b"GET / HTTP/1.1\r\nHost: e.com\r\n\r\n")
            await writer.drain()
            response = await asyncio.wait_for(reader.read(4096), timeout=1)
            assert response.startswith(b"HTTP/1.1 200 OK\r\n")
            assert response.endswith(b"ok")

            writer.write(b"GET / HTTP/1.1\r\nHost: e.com\r\n\r\n")
            await writer.drain()
            response2 = await asyncio.wait_for(reader.read(4096), timeout=1)
            assert response2.startswith(b"HTTP/1.1 200 OK\r\n")
        finally:
            writer.close()
            await writer.wait_closed()


HANDSHAKE = (
    b"GET /ws HTTP/1.1\r\n"
    b"Host: e.com\r\n"
    b"Upgrade: websocket\r\n"
    b"Connection: Upgrade\r\n"
    b"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
    b"Sec-WebSocket-Version: 13\r\n"
    b"\r\n"
)
ACCEPT = b"s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
CLIENT_MASK = b"\x37\xfa\x21\x3d"


def client_frame(opcode: Opcode, payload: bytes = b"", *, fin: bool = True) -> bytes:
    return encode_frame(opcode, payload, fin=fin, mask=CLIENT_MASK)


def server_frames(transport: FakeTransport) -> list:
    """Decode everything the server wrote after the 101 response."""
    written = bytes(transport.written)
    head_end = written.index(b"\r\n\r\n") + 4
    return FrameParser(require_mask=False).feed_data(written[head_end:])


async def echo_ws_app(scope: Scope, receive: Receive, send: Send) -> None:
    assert scope["type"] == "websocket"
    await receive()  # websocket.connect
    await send({"type": "websocket.accept"})
    while True:
        message = await receive()
        if message["type"] == "websocket.disconnect":
            return
        if "text" in message and message["text"] is not None:
            await send({"type": "websocket.send", "text": message["text"]})
        else:
            await send({"type": "websocket.send", "bytes": message["bytes"]})


def make_ws_app(*, subprotocol=None, headers=None, deny=False):
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await receive()
        if deny:
            await send({"type": "websocket.close", "code": 1008})
            return
        accept: Message = {"type": "websocket.accept"}
        if subprotocol is not None:
            accept["subprotocol"] = subprotocol
        if headers is not None:
            accept["headers"] = headers
        await send(accept)
        await receive()

    return app


class TestWebSocketHandshake:
    async def test_accept_writes_101_with_the_rfc_accept_key(self):
        protocol, transport = make_protocol(echo_ws_app)
        protocol.data_received(HANDSHAKE)
        await asyncio.sleep(0.05)
        written = bytes(transport.written)
        assert written.startswith(b"HTTP/1.1 101 Switching Protocols\r\n")
        assert b"Sec-WebSocket-Accept: " + ACCEPT + b"\r\n" in written
        assert b"Upgrade: websocket\r\n" in written
        # protocol.py's HTTP encoder would have appended one of these, and it
        # is wrong on a 101.
        assert b"keep-alive" not in written.lower()

    async def test_scope_is_a_websocket_scope(self):
        seen = {}

        async def app(scope, receive, send):
            seen.update(scope)
            await receive()
            await send({"type": "websocket.accept"})
            await receive()

        protocol, _ = make_protocol(app)
        protocol.data_received(HANDSHAKE)
        await asyncio.sleep(0.05)
        assert seen["type"] == "websocket"
        assert seen["scheme"] == "ws"
        assert seen["path"] == "/ws"
        assert seen["subprotocols"] == []
        assert "method" not in seen

    async def test_subprotocols_reach_the_scope_and_the_choice_is_echoed(self):
        request = HANDSHAKE.replace(
            b"Sec-WebSocket-Version: 13\r\n",
            b"Sec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: chat, superchat\r\n",
        )
        protocol, transport = make_protocol(make_ws_app(subprotocol="chat"))
        protocol.data_received(request)
        await asyncio.sleep(0.05)
        assert b"Sec-WebSocket-Protocol: chat\r\n" in bytes(transport.written)

    async def test_extra_accept_headers_are_written(self):
        protocol, transport = make_protocol(make_ws_app(headers=[(b"x-trace", b"abc")]))
        protocol.data_received(HANDSHAKE)
        await asyncio.sleep(0.05)
        assert b"x-trace: abc\r\n" in bytes(transport.written)

    async def test_close_before_accept_is_an_http_403(self):
        protocol, transport = make_protocol(make_ws_app(deny=True))
        protocol.data_received(HANDSHAKE)
        await asyncio.sleep(0.05)
        assert bytes(transport.written).startswith(b"HTTP/1.1 403 Forbidden\r\n")
        assert transport.closed is True

    async def test_missing_version_is_426_advertising_13(self):
        request = HANDSHAKE.replace(b"Sec-WebSocket-Version: 13\r\n", b"")
        protocol, transport = make_protocol(echo_ws_app)
        protocol.data_received(request)
        await asyncio.sleep(0.05)
        written = bytes(transport.written)
        assert written.startswith(b"HTTP/1.1 426 ")
        assert b"sec-websocket-version: 13\r\n" in written

    async def test_bad_key_is_400(self):
        request = HANDSHAKE.replace(
            b"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n",
            b"Sec-WebSocket-Key: nope\r\n",
        )
        protocol, transport = make_protocol(echo_ws_app)
        protocol.data_received(request)
        await asyncio.sleep(0.05)
        assert bytes(transport.written).startswith(b"HTTP/1.1 400 ")

    async def test_unimplemented_upgrade_is_501(self):
        request = (
            b"GET / HTTP/1.1\r\nHost: e.com\r\n"
            b"Upgrade: h2c\r\nConnection: Upgrade\r\n\r\n"
        )
        protocol, transport = make_protocol(
            fixed_response_app, upgrade_protocols=frozenset({"websocket", "h2c"})
        )
        protocol.data_received(request)
        await asyncio.sleep(0.05)
        assert bytes(transport.written).startswith(b"HTTP/1.1 501 Not Implemented\r\n")

    async def test_upgrade_with_a_body_is_a_400(self):
        request = HANDSHAKE.replace(
            b"Sec-WebSocket-Version: 13\r\n",
            b"Sec-WebSocket-Version: 13\r\nContent-Length: 5\r\n",
        )
        protocol, transport = make_protocol(echo_ws_app)
        protocol.data_received(request + b"hello")
        await asyncio.sleep(0.05)
        assert bytes(transport.written).startswith(b"HTTP/1.1 400 ")

    async def test_pipelined_request_ahead_of_the_handshake_is_answered_first(self):
        # The write turnstile: a websocket task holds its own forever, but it
        # must still wait for responses queued ahead of it.
        protocol, transport = make_protocol(_http_then_ws_app)
        protocol.data_received(b"GET /a HTTP/1.1\r\nHost: e.com\r\n\r\n" + HANDSHAKE)
        await asyncio.sleep(0.05)
        written = bytes(transport.written)
        assert written.index(b"200 OK") < written.index(b"101 Switching Protocols")


async def _http_then_ws_app(scope: Scope, receive: Receive, send: Send) -> None:
    if scope["type"] == "http":
        await fixed_response_app(scope, receive, send)
        return
    await receive()
    await send({"type": "websocket.accept"})
    await receive()


class TestWebSocketMessages:
    async def test_text_round_trip(self):
        protocol, transport = make_protocol(echo_ws_app)
        protocol.data_received(HANDSHAKE)
        await asyncio.sleep(0.01)
        protocol.data_received(client_frame(Opcode.TEXT, "héllo".encode()))
        await asyncio.sleep(0.05)
        assert server_frames(transport) == [TextMessage("héllo")]

    async def test_binary_round_trip(self):
        protocol, transport = make_protocol(echo_ws_app)
        protocol.data_received(HANDSHAKE)
        await asyncio.sleep(0.01)
        protocol.data_received(client_frame(Opcode.BINARY, b"\x00\xff"))
        await asyncio.sleep(0.05)
        assert server_frames(transport) == [BinaryMessage(b"\x00\xff")]

    async def test_server_frames_are_unmasked(self):
        protocol, transport = make_protocol(echo_ws_app)
        protocol.data_received(HANDSHAKE)
        await asyncio.sleep(0.01)
        protocol.data_received(client_frame(Opcode.TEXT, b"hi"))
        await asyncio.sleep(0.05)
        written = bytes(transport.written)
        frame_start = written.index(b"\r\n\r\n") + 4
        assert written[frame_start + 1] & 0x80 == 0

    async def test_prologue_frames_are_not_lost(self):
        # Frames arriving in the same TCP segment as the handshake are already
        # inside the HTTP parser's buffer when the upgrade is detected.
        protocol, transport = make_protocol(echo_ws_app)
        protocol.data_received(HANDSHAKE + client_frame(Opcode.TEXT, b"early"))
        await asyncio.sleep(0.05)
        assert server_frames(transport) == [TextMessage("early")]

    async def test_fragmented_client_message_is_reassembled(self):
        protocol, transport = make_protocol(echo_ws_app)
        protocol.data_received(HANDSHAKE)
        await asyncio.sleep(0.01)
        protocol.data_received(
            client_frame(Opcode.TEXT, b"he", fin=False)
            + client_frame(Opcode.CONTINUATION, b"llo")
        )
        await asyncio.sleep(0.05)
        assert server_frames(transport) == [TextMessage("hello")]

    async def test_ping_is_answered_with_a_pong_the_app_never_sees(self):
        received = []

        async def app(scope, receive, send):
            await receive()
            await send({"type": "websocket.accept"})
            while True:
                message = await receive()
                received.append(message["type"])
                if message["type"] == "websocket.disconnect":
                    return

        protocol, transport = make_protocol(app)
        protocol.data_received(HANDSHAKE)
        await asyncio.sleep(0.01)
        protocol.data_received(client_frame(Opcode.PING, b"nonce"))
        await asyncio.sleep(0.05)
        assert server_frames(transport) == [Pong(b"nonce")]
        assert "websocket.receive" not in received


class TestWebSocketClose:
    async def test_client_close_is_echoed_and_the_app_sees_the_code(self):
        codes = []

        async def app(scope, receive, send):
            await receive()
            await send({"type": "websocket.accept"})
            message = await receive()
            codes.append(message["code"])

        protocol, transport = make_protocol(app)
        protocol.data_received(HANDSHAKE)
        await asyncio.sleep(0.01)
        protocol.data_received(
            client_frame(Opcode.CLOSE, struct.pack("!H", 1001) + b"bye")
        )
        await asyncio.sleep(0.05)
        assert codes == [1001]
        assert server_frames(transport) == [CloseReceived(1001, "bye")]
        assert transport.closed is True

    async def test_payloadless_client_close_reports_1005_and_echoes_empty(self):
        codes = []

        async def app(scope, receive, send):
            await receive()
            await send({"type": "websocket.accept"})
            codes.append((await receive())["code"])

        protocol, transport = make_protocol(app)
        protocol.data_received(HANDSHAKE)
        await asyncio.sleep(0.01)
        protocol.data_received(client_frame(Opcode.CLOSE, b""))
        await asyncio.sleep(0.05)
        assert codes == [1005]
        # 1005 is never sendable, so the echo carries no payload either.
        assert server_frames(transport) == [CloseReceived(1005, "")]

    async def test_app_initiated_close(self):
        async def app(scope, receive, send):
            await receive()
            await send({"type": "websocket.accept"})
            await send({"type": "websocket.close", "code": 1001, "reason": "later"})

        protocol, transport = make_protocol(app)
        protocol.data_received(HANDSHAKE)
        await asyncio.sleep(0.05)
        assert server_frames(transport) == [CloseReceived(1001, "later")]
        assert transport.closed is True

    async def test_close_after_close_is_a_no_op(self):
        # `finally: await ws.close()` is the commonest idiom there is and must
        # not explode because the peer went first.
        errors = []

        async def app(scope, receive, send):
            await receive()
            await send({"type": "websocket.accept"})
            try:
                await send({"type": "websocket.close"})
                await send({"type": "websocket.close"})
            except Exception as exc:
                errors.append(exc)

        protocol, _ = make_protocol(app)
        protocol.data_received(HANDSHAKE)
        await asyncio.sleep(0.05)
        assert errors == []

    async def test_handler_returning_without_closing_sends_1000(self):
        async def app(scope, receive, send):
            await receive()
            await send({"type": "websocket.accept"})

        protocol, transport = make_protocol(app)
        protocol.data_received(HANDSHAKE)
        await asyncio.sleep(0.05)
        assert server_frames(transport) == [CloseReceived(1000, "")]
        assert transport.closed is True

    async def test_codec_violation_closes_with_the_codec_code(self):
        codes = []

        async def app(scope, receive, send):
            await receive()
            await send({"type": "websocket.accept"})
            codes.append((await receive())["code"])

        protocol, transport = make_protocol(app)
        protocol.data_received(HANDSHAKE)
        await asyncio.sleep(0.01)
        protocol.data_received(encode_frame(Opcode.TEXT, b"unmasked"))
        await asyncio.sleep(0.05)
        assert codes == [1002]
        closes = server_frames(transport)
        assert [c.code for c in closes] == [1002]

    async def test_oversized_message_closes_with_1009(self):
        codes = []

        async def app(scope, receive, send):
            await receive()
            await send({"type": "websocket.accept"})
            codes.append((await receive())["code"])

        protocol, transport = make_protocol(app, websocket_max_message_size=10)
        protocol.data_received(HANDSHAKE)
        await asyncio.sleep(0.01)
        protocol.data_received(client_frame(Opcode.BINARY, b"x" * 50))
        await asyncio.sleep(0.05)
        assert codes == [1009]
        assert [c.code for c in server_frames(transport)] == [1009]

    async def test_transport_loss_reports_1006(self):
        codes = []

        async def app(scope, receive, send):
            await receive()
            await send({"type": "websocket.accept"})
            codes.append((await receive())["code"])

        protocol, _ = make_protocol(app)
        protocol.data_received(HANDSHAKE)
        await asyncio.sleep(0.01)
        protocol.connection_lost(None)
        await asyncio.sleep(0.05)
        assert codes == [1006]

    async def test_a_clean_close_is_not_overwritten_by_1006(self):
        codes = []

        async def app(scope, receive, send):
            await receive()
            await send({"type": "websocket.accept"})
            codes.append((await receive())["code"])
            # A second receive must repeat the disconnect rather than hang.
            codes.append((await receive())["code"])

        protocol, _ = make_protocol(app)
        protocol.data_received(HANDSHAKE)
        await asyncio.sleep(0.01)
        protocol.data_received(client_frame(Opcode.CLOSE, struct.pack("!H", 1000)))
        protocol.connection_lost(None)
        await asyncio.sleep(0.05)
        assert codes == [1000, 1000]


class TestWebSocketSendErrors:
    async def test_send_before_accept(self):
        errors = []

        async def app(scope, receive, send):
            await receive()
            try:
                await send({"type": "websocket.send", "text": "too early"})
            except RuntimeError as exc:
                errors.append(str(exc))

        protocol, _ = make_protocol(app)
        protocol.data_received(HANDSHAKE)
        await asyncio.sleep(0.05)
        assert errors
        assert "before websocket.accept" in errors[0]

    async def test_accept_twice(self):
        errors = []

        async def app(scope, receive, send):
            await receive()
            await send({"type": "websocket.accept"})
            try:
                await send({"type": "websocket.accept"})
            except RuntimeError as exc:
                errors.append(str(exc))

        protocol, _ = make_protocol(app)
        protocol.data_received(HANDSHAKE)
        await asyncio.sleep(0.05)
        assert errors
        assert "more than once" in errors[0]

    async def test_send_without_text_or_bytes(self):
        errors = []

        async def app(scope, receive, send):
            await receive()
            await send({"type": "websocket.accept"})
            try:
                await send({"type": "websocket.send"})
            except RuntimeError as exc:
                errors.append(str(exc))

        protocol, _ = make_protocol(app)
        protocol.data_received(HANDSHAKE)
        await asyncio.sleep(0.05)
        assert errors
        assert "'bytes' or 'text'" in errors[0]

    async def test_http_message_on_a_websocket(self):
        errors = []

        async def app(scope, receive, send):
            await receive()
            await send({"type": "websocket.accept"})
            try:
                await send({"type": "http.response.start", "status": 200})
            except RuntimeError as exc:
                errors.append(str(exc))

        protocol, _ = make_protocol(app)
        protocol.data_received(HANDSHAKE)
        await asyncio.sleep(0.05)
        assert errors
        assert "unexpected ASGI message type" in errors[0]


class TestWebSocketBackpressure:
    async def test_reading_is_paused_until_accept(self):
        started = asyncio.Event()

        async def app(scope, receive, send):
            await receive()
            started.set()
            await asyncio.sleep(0.2)

        protocol, transport = make_protocol(app)
        protocol.data_received(HANDSHAKE)
        await asyncio.wait_for(started.wait(), timeout=1)
        assert transport.paused is True

    async def test_reading_resumes_on_accept(self):
        protocol, transport = make_protocol(echo_ws_app)
        protocol.data_received(HANDSHAKE)
        await asyncio.sleep(0.05)
        assert transport.paused is False
