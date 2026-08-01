import asyncio

import pytest

from sonix.server.parser import RequestHead
from sonix.server.protocol import HTTPProtocol, build_scope
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
            f"POST / HTTP/1.1\r\nHost: e.com\r\nContent-Length: {content_length}\r\n\r\n".encode()
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
