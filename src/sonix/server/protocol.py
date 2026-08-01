"""The asyncio.Protocol <-> ASGI bridge.

This is the "uvicorn" layer: it turns bytes on a socket into
``(scope, receive, send)`` calls into an ASGI application, using
``server/parser.py`` for all framing decisions. It never independently
inspects headers to decide how a body is framed -- it only acts on the
events the parser emits, which is what prevents the same request being
parsed two different ways by two different pieces of code.
"""

from __future__ import annotations

import asyncio
import functools
import http
from typing import cast

from sonix.server.parser import (
    BodyChunk,
    Event,
    HTTP11Parser,
    HTTPParserError,
    MalformedRequest,
    RequestComplete,
    RequestHead,
    RequestHeadComplete,
    RequestTooLarge,
)
from sonix.types import ASGIApp, Message, Receive, Scope, Send

DEFAULT_HEAD_TIMEOUT = 10.0
DEFAULT_BODY_PAUSE_WATERMARK = 32
DEFAULT_BODY_RESUME_WATERMARK = 8

_PARSER_ERROR_STATUS: dict[type[HTTPParserError], int] = {
    MalformedRequest: 400,
    RequestTooLarge: 413,
}


# -- pure helpers, no event loop involved -----------------------------------


def build_scope(
    head: RequestHead,
    *,
    client: tuple[str, int] | None,
    server: tuple[str, int] | None,
) -> Scope:
    return {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": head.http_version,
        "method": head.method,
        "scheme": "http",
        "path": head.path,
        "raw_path": head.path.encode("ascii"),
        "query_string": head.query_string,
        "root_path": "",
        "headers": head.headers,
        "client": client,
        "server": server,
    }


def _reason_phrase(status: int) -> bytes:
    try:
        return http.HTTPStatus(status).phrase.encode("ascii")
    except ValueError:
        return b""


def _client_requested_close(head: RequestHead) -> bool:
    tokens: set[bytes] = set()
    for name, value in head.headers:
        if name == b"connection":
            tokens.update(t.strip().lower() for t in value.split(b","))
    if b"close" in tokens:
        return True
    if head.http_version == "1.0":
        return b"keep-alive" not in tokens
    return False


def _response_declares_length(headers: list[tuple[bytes, bytes]]) -> bool:
    return any(
        name.lower() in (b"content-length", b"transfer-encoding") for name, _ in headers
    )


def _decide_close(
    head: RequestHead, response_headers: list[tuple[bytes, bytes]]
) -> bool:
    if _client_requested_close(head):
        return True
    return not _response_declares_length(response_headers)


def _encode_response_head(
    status: int,
    headers: list[tuple[bytes, bytes]],
    *,
    close: bool,
    http_version: str = "1.1",
) -> bytes:
    lines = [
        b"HTTP/%s %d %s"
        % (http_version.encode("ascii"), status, _reason_phrase(status))
    ]
    has_connection = any(name.lower() == b"connection" for name, _ in headers)
    lines.extend(name + b": " + value for name, value in headers)
    if not has_connection:
        lines.append(b"Connection: close" if close else b"Connection: keep-alive")
    return b"\r\n".join(lines) + b"\r\n\r\n"


_FORBIDDEN_HEADER_BYTES = (b"\r", b"\n", b"\x00")


def _validate_response_headers(headers: list[tuple[bytes, bytes]]) -> None:
    for name, value in headers:
        if any(b in name for b in _FORBIDDEN_HEADER_BYTES):
            raise ValueError(f"invalid character in response header name: {name!r}")
        if any(b in value for b in _FORBIDDEN_HEADER_BYTES):
            raise ValueError(f"invalid character in response header value: {value!r}")
    has_length = any(name.lower() == b"content-length" for name, _ in headers)
    has_encoding = any(name.lower() == b"transfer-encoding" for name, _ in headers)
    if has_length and has_encoding:
        raise ValueError(
            "response must not set both Content-Length and Transfer-Encoding"
        )


def _set_event() -> asyncio.Event:
    event = asyncio.Event()
    event.set()
    return event


def _completed_request_events(events: list[Event]) -> list[Event]:
    """Drop a trailing request that never reached RequestComplete.

    A parser error's ``partial_events`` can end mid-request (a
    RequestHeadComplete, possibly followed by BodyChunks, with no matching
    RequestComplete because the body framing itself is what failed to
    parse). Dispatching that dangling head would spawn an ASGI app task
    whose receive() can never be satisfied -- no more BodyChunk/
    RequestComplete events will ever arrive for it -- so it would hang
    forever and, via the write turnstile, block every response behind it,
    including the failure response itself. Only replay events belonging to
    requests that fully completed.
    """
    last_complete = -1
    for i, event in enumerate(events):
        if isinstance(event, RequestComplete):
            last_complete = i
    return events[: last_complete + 1]


class HTTPProtocol(asyncio.Protocol):
    """One instance per TCP connection. State is never shared across connections."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        max_header_size: int = 8 * 1024,
        max_headers: int = 100,
        max_body_size: int = 16 * 1024 * 1024,
        head_timeout: float = DEFAULT_HEAD_TIMEOUT,
        body_pause_watermark: int = DEFAULT_BODY_PAUSE_WATERMARK,
        body_resume_watermark: int = DEFAULT_BODY_RESUME_WATERMARK,
    ) -> None:
        self.app = app
        self._max_header_size = max_header_size
        self._max_headers = max_headers
        self._max_body_size = max_body_size
        self._head_timeout = head_timeout
        self._body_pause_watermark = body_pause_watermark
        self._body_resume_watermark = body_resume_watermark
        self.transport: asyncio.Transport | None = None
        self.parser: HTTP11Parser | None = None

    # -- connection lifecycle ------------------------------------------------

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = cast(asyncio.Transport, transport)
        self.parser = HTTP11Parser(
            max_header_size=self._max_header_size,
            max_headers=self._max_headers,
            max_body_size=self._max_body_size,
        )
        self._loop = asyncio.get_running_loop()
        self._client = self.transport.get_extra_info("peername")
        self._server = self.transport.get_extra_info("sockname")

        self._receive_queues: list[asyncio.Queue[Message]] = []
        self._current_receive_queue: asyncio.Queue[Message] | None = None
        self._inflight_tasks: set[asyncio.Task[None]] = set()
        self._write_turnstile: asyncio.Event = _set_event()
        self._disconnected = False
        self._closed = False
        self._got_first_head = False
        self._reading_paused = False
        self._paused_writing = False
        self._drain_waiter: asyncio.Future[None] | None = None

        self._head_timeout_handle: asyncio.TimerHandle | None = self._loop.call_later(
            self._head_timeout, self._on_head_timeout
        )

    def _disarm_head_timeout(self) -> None:
        if self._head_timeout_handle is not None:
            self._head_timeout_handle.cancel()
            self._head_timeout_handle = None

    def _on_head_timeout(self) -> None:
        if (
            self._got_first_head
            or self.transport is None
            or self.transport.is_closing()
        ):
            return
        self._closed = True
        body = b"Request Timeout"
        head = _encode_response_head(
            408,
            [
                (b"content-length", str(len(body)).encode()),
                (b"content-type", b"text/plain; charset=utf-8"),
            ],
            close=True,
        )
        self.transport.write(head + body)
        self.transport.close()

    def eof_received(self) -> bool | None:
        if self.parser is not None:
            try:
                self.parser.feed_eof()
            except HTTPParserError:
                pass  # client went away mid-request; connection_lost does cleanup
        return False

    def connection_lost(self, exc: Exception | None) -> None:
        self._closed = True
        self._disarm_head_timeout()
        self._disconnected = True
        for queue in self._receive_queues:
            queue.put_nowait({"type": "http.disconnect"})
        # Deferred via call_soon (not called directly): a task blocked in
        # receive() has its queue.get() future resolved by put_nowait above,
        # but that only *schedules* the task's resumption -- it hasn't run
        # yet. Cancelling synchronously here would race that resumption
        # (Task.cancel() on a task whose current awaitable is already done
        # falls back to `_must_cancel`, discarding the just-delivered
        # message). Queuing the cancel via call_soon lets the task actually
        # observe the disconnect message first; if it then finishes on its
        # own, cancel() on an already-done task is a harmless no-op.
        for task in list(self._inflight_tasks):
            self._loop.call_soon(task.cancel)

    # -- write backpressure ---------------------------------------------------

    def pause_writing(self) -> None:
        self._paused_writing = True

    def resume_writing(self) -> None:
        self._paused_writing = False
        waiter = self._drain_waiter
        if waiter is not None and not waiter.done():
            waiter.set_result(None)

    async def _drain(self) -> None:
        if not self._paused_writing:
            return
        waiter = self._loop.create_future()
        self._drain_waiter = waiter
        try:
            await waiter
        finally:
            self._drain_waiter = None

    # -- incoming bytes ---------------------------------------------------------

    def data_received(self, data: bytes) -> None:
        if self._closed or self.parser is None:
            return
        try:
            events = self.parser.feed_data(data)
        except HTTPParserError as exc:
            for event in _completed_request_events(exc.partial_events):
                self._handle_event(event)
            self._fail(exc)
            return

        for event in events:
            self._handle_event(event)

    def _handle_event(self, event: Event) -> None:
        if isinstance(event, RequestHeadComplete):
            if not self._got_first_head:
                self._got_first_head = True
                self._disarm_head_timeout()
            self._begin_request(event.head)
        elif isinstance(event, BodyChunk):
            self._push_body_chunk(
                {
                    "type": "http.request",
                    "body": event.data,
                    "more_body": event.more_body,
                }
            )
        elif isinstance(event, RequestComplete):
            pass
        else:  # pragma: no cover - exhaustive over Event
            raise AssertionError(f"unhandled parser event: {event!r}")  # noqa: TRY004

    def _fail(self, exc: HTTPParserError) -> None:
        self._closed = True
        self._disarm_head_timeout()
        status = _PARSER_ERROR_STATUS.get(type(exc), 400)
        # Any good pipelined requests ahead of this one were just spawned as
        # tasks by _handle_event above but haven't run yet (create_task only
        # schedules them). Wait our turn on the same turnstile _make_send
        # uses, so their responses get written before this failure response
        # closes the connection -- otherwise the good responses would be
        # silently dropped against an already-closed transport.
        my_turn = self._write_turnstile
        task = self._loop.create_task(self._write_failure_response(status, my_turn))
        self._inflight_tasks.add(task)
        task.add_done_callback(self._inflight_tasks.discard)

    async def _write_failure_response(
        self, status: int, my_turn: asyncio.Event
    ) -> None:
        reason = _reason_phrase(status) or b"Bad Request"
        head = _encode_response_head(
            status,
            [
                (b"content-length", str(len(reason)).encode()),
                (b"content-type", b"text/plain; charset=utf-8"),
            ],
            close=True,
        )
        await my_turn.wait()
        if self.transport is not None and not self.transport.is_closing():
            self.transport.write(head + reason)
            self.transport.close()

    # -- per-request dispatch ----------------------------------------------------

    def _begin_request(self, head: RequestHead) -> None:
        queue: asyncio.Queue[Message] = asyncio.Queue()
        self._receive_queues.append(queue)
        self._current_receive_queue = queue

        scope = build_scope(head, client=self._client, server=self._server)
        receive = self._make_receive(queue)

        my_turn = self._write_turnstile
        my_done = asyncio.Event()
        self._write_turnstile = my_done
        send = self._make_send(head, my_turn, my_done)

        task = asyncio.ensure_future(self.app(scope, receive, send), loop=self._loop)
        self._inflight_tasks.add(task)
        task.add_done_callback(
            functools.partial(self._on_task_done, my_done=my_done, queue=queue)
        )

    def _on_task_done(
        self,
        task: asyncio.Task[None],
        *,
        my_done: asyncio.Event,
        queue: asyncio.Queue[Message] | None = None,
    ) -> None:
        self._inflight_tasks.discard(task)
        if queue is not None:
            # Nothing will ever call receive() on this queue again -- the
            # request's task is done -- so it doesn't need to keep receiving
            # a connection_lost() disconnect message, and doesn't need to be
            # retained for the life of the (possibly long, keep-alive)
            # connection.
            try:
                self._receive_queues.remove(queue)
            except ValueError:
                pass
        if not my_done.is_set():
            my_done.set()
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            if self.transport is not None and not self.transport.is_closing():
                self.transport.close()
            self._loop.call_exception_handler(
                {
                    "message": "unhandled exception in ASGI application",
                    "exception": exc,
                    "protocol": self,
                }
            )

    def _make_send(
        self, head: RequestHead, my_turn: asyncio.Event, my_done: asyncio.Event
    ) -> Send:
        started = False
        close = False

        async def send(message: Message) -> None:
            nonlocal started, close
            msg_type = message.get("type")
            if msg_type == "http.response.start":
                if started:
                    raise RuntimeError(
                        "http.response.start sent more than once for this request"
                    )
                started = True
                status = message["status"]
                headers = list(message.get("headers", []))
                _validate_response_headers(headers)
                close = _decide_close(head, headers)
                await my_turn.wait()
                if self.transport is not None and not self.transport.is_closing():
                    self.transport.write(
                        _encode_response_head(
                            status,
                            headers,
                            close=close,
                            http_version=head.http_version,
                        )
                    )
            elif msg_type == "http.response.body":
                if not started:
                    raise RuntimeError(
                        "http.response.body sent before http.response.start"
                    )
                body = message.get("body", b"")
                if (
                    body
                    and self.transport is not None
                    and not self.transport.is_closing()
                ):
                    self.transport.write(body)
                    await self._drain()
                if not message.get("more_body", False):
                    my_done.set()
                    if close and self.transport is not None:
                        self.transport.close()
            else:
                raise RuntimeError(f"unexpected ASGI message type: {msg_type!r}")

        return send

    def _make_receive(self, queue: asyncio.Queue[Message]) -> Receive:
        async def receive() -> Message:
            if not queue.empty():
                message = queue.get_nowait()
            elif self._disconnected:
                return {"type": "http.disconnect"}
            else:
                message = await queue.get()
            if (
                self._reading_paused
                and self.transport is not None
                and queue.qsize() <= self._body_resume_watermark
            ):
                self.transport.resume_reading()
                self._reading_paused = False
            return message

        return receive

    def _push_body_chunk(self, message: Message) -> None:
        queue = self._current_receive_queue
        if (
            queue is None
        ):  # pragma: no cover - parser always emits BodyChunk after RequestHeadComplete
            raise RuntimeError("received a body chunk with no active request")
        queue.put_nowait(message)
        if (
            not self._reading_paused
            and self.transport is not None
            and queue.qsize() >= self._body_pause_watermark
        ):
            self.transport.pause_reading()
            self._reading_paused = True
