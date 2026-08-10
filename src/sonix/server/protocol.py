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
import enum
import functools
import http
import os
import time
from email.utils import formatdate
from typing import cast

from sonix.server.parser import (
    DEFAULT_UPGRADE_PROTOCOLS,
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
from sonix.server.websockets import (
    DEFAULT_MAX_MESSAGE_SIZE,
    BinaryMessage,
    CloseCode,
    CloseReceived,
    FrameEvent,
    FrameParser,
    HandshakeError,
    Opcode,
    Ping,
    Pong,
    TextMessage,
    WebSocketProtocolError,
    encode_close_frame,
    encode_frame,
    encode_handshake_response,
    parse_subprotocols,
    validate_handshake,
)
from sonix.types import ASGIApp, Message, Receive, Scope, Send

DEFAULT_HEAD_TIMEOUT = 10.0
# Matches uvicorn's --timeout-keep-alive. Shorter than head_timeout on
# purpose: a connection that has already been used has proved nothing about
# whether the client intends to use it again.
DEFAULT_KEEP_ALIVE_TIMEOUT = 5.0
# asyncio's own transport defaults, restated rather than inherited: the point
# of the knob is that the policy is Sonix's to state. Note the unit differs
# from the watermarks below -- these count *bytes buffered in the transport*,
# those count *messages queued for the application*.
DEFAULT_WRITE_PAUSE_WATERMARK = 64 * 1024
DEFAULT_WRITE_RESUME_WATERMARK = 16 * 1024
# How many application tasks one connection may have in flight before the
# server stops reading from it. Pipelining lets a client dispatch a task per
# request with tens of bytes each, and nothing else bounds that: the body and
# websocket watermarks below bound one *request's* queue, not how many
# requests exist. Without this a single connection can allocate tasks, queues
# and scopes without limit.
DEFAULT_PIPELINE_PAUSE_WATERMARK = 32
DEFAULT_PIPELINE_RESUME_WATERMARK = 8
DEFAULT_BODY_PAUSE_WATERMARK = 32
DEFAULT_BODY_RESUME_WATERMARK = 8
DEFAULT_WS_PAUSE_WATERMARK = 32
DEFAULT_WS_RESUME_WATERMARK = 8
# A websocket connection can stay silent indefinitely and still be perfectly
# healthy, so a dead peer is invisible without an application-level liveness
# check. These match uvicorn's --ws-ping-interval/--ws-ping-timeout defaults.
DEFAULT_WS_PING_INTERVAL = 20.0
DEFAULT_WS_PING_TIMEOUT = 20.0
# How long a deferred close waits for the application task to finish. Bounded
# for the same reason the server's drain is: the point is to let a well-behaved
# handler answer one more message, not to let a hung one hold a socket open.
DEFAULT_WS_CLOSE_TIMEOUT = 5.0

DEFAULT_SERVER_HEADER = b"sonix"

# Formatting a date costs more than writing one, and every response inside the
# same second wants the identical string. Module-level rather than
# per-connection because the value is global: two connections in the same
# second must not disagree about what time it is.
_date_cache: tuple[int, bytes] = (0, b"")


def _formatted_date(now: float) -> bytes:
    """The current time as an RFC 9110 IMF-fixdate, memoized per second.

    email.utils.formatdate rather than time.strftime("%a, %d %b %Y ... GMT"):
    strftime's %a and %b are locale-dependent, so under a non-C LC_TIME that
    spelling emits a date no HTTP client is required to parse. formatdate
    carries its own English tables and cannot drift.

    Takes `now` rather than calling time.time() itself, so it stays a function
    of its argument and the memoization is testable without sleeping. No
    background refresh task (uvicorn's approach): one time.time() call and an
    int comparison per response costs less than owning a timer that has to be
    created, cancelled, and reasoned about across graceful shutdown.
    """
    global _date_cache
    second = int(now)
    cached_second, cached = _date_cache
    if second != cached_second:
        cached = formatdate(second, usegmt=True).encode("ascii")
        _date_cache = (second, cached)
    return cached


_PARSER_ERROR_STATUS: dict[type[HTTPParserError], int] = {
    MalformedRequest: 400,
    RequestTooLarge: 413,
}


class _TimerMode(enum.Enum):
    """Why the connection timer is currently armed.

    HEAD covers both "no bytes yet" and "a head is arriving one byte at a
    time" -- the same condition from the server's point of view, and the same
    answer. KEEP_ALIVE covers a connection sitting at a request boundary with
    an empty buffer.
    """

    HEAD = enum.auto()
    KEEP_ALIVE = enum.auto()


class _WSState(enum.Enum):
    """Where a connection is in the ASGI websocket handshake.

    ``None`` rather than a member of this enum means "still an ordinary HTTP
    connection" -- the upgrade is a mode switch, not a request kind.
    """

    CONNECTING = enum.auto()
    CONNECTED = enum.auto()
    # The close code is decided and the disconnect is already in the
    # application's queue, but the close frame is not on the wire yet. Inbound
    # frames are no longer decoded; outbound sends still go through. This
    # window exists for exactly one thing: letting the application answer
    # messages that arrived in the same batch as the event that ended the
    # session.
    CLOSING = enum.auto()
    CLOSED = enum.auto()


class ServerState:
    """Cross-connection state shared by every protocol instance of one server.

    Lives here rather than in server.py so that protocol.py doesn't have to
    import from the module that imports *it*. It holds only what graceful
    shutdown needs: the set of live connections, and a flag telling them a
    shutdown is in progress so they stop advertising keep-alive.

    This is the single exception to "no mutable state is shared across
    connections" -- and it is why connections register and deregister
    themselves rather than the server tracking them from the outside, which
    would leak entries for connections that died without notice.
    """

    __slots__ = ("connections", "should_exit")

    def __init__(self) -> None:
        self.connections: set[HTTPProtocol] = set()
        self.should_exit = False


# -- pure helpers, no event loop involved -----------------------------------


def build_scope(
    head: RequestHead,
    *,
    client: tuple[str, int] | None,
    server: tuple[str, int] | None,
    state: dict | None = None,
) -> Scope:
    scope: Scope
    if head.upgrade == "websocket":
        # No "method" key: a websocket scope has no HTTP method, which is why
        # anything reading scope["method"] unconditionally breaks here.
        # Reading Sec-WebSocket-Protocol is application metadata, not a
        # framing conclusion, so it does not trespass on the parser's rule.
        scope = {
            "type": "websocket",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": head.http_version,
            "scheme": "ws",
            "path": head.path,
            "raw_path": head.path.encode("ascii"),
            "query_string": head.query_string,
            "root_path": "",
            "headers": head.headers,
            "client": client,
            "server": server,
            "subprotocols": parse_subprotocols(head.headers),
        }
    else:
        scope = {
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
    # Per the ASGI spec each request gets a *shallow copy* of the server's
    # lifespan state: handlers may stash per-request values on it without
    # leaking them into the next request, while the shared objects the
    # lifespan opened (a connection, a pool) remain shared. Omitted entirely
    # when there is no lifespan state, so a scope built without a server is
    # byte-for-byte what it always was.
    if state is not None:
        scope["state"] = dict(state)
    return scope


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
    date: bytes | None = None,
    server: bytes | None = None,
) -> bytes:
    """Serialize a response head. Pure: the clock is read by the caller.

    ``date`` and ``server`` are pre-formatted values rather than flags
    precisely so this stays a function of its arguments, unit-testable with no
    event loop and no wall clock. Either None omits that header.

    An application-supplied header always wins, mirroring the existing
    Connection rule: an app that sets its own Date (a cache proxy replaying an
    origin response) or its own Server must not end up with two.
    """
    lines = [
        b"HTTP/%s %d %s"
        % (http_version.encode("ascii"), status, _reason_phrase(status))
    ]
    # One pass rather than three any() scans: this runs on every response, and
    # the benchmark is the reason these headers exist at all.
    has_connection = has_date = has_server = False
    for name, value in headers:
        lowered = name.lower()
        if lowered == b"connection":
            has_connection = True
        elif lowered == b"date":
            has_date = True
        elif lowered == b"server":
            has_server = True
        lines.append(name + b": " + value)
    if not has_connection:
        lines.append(b"Connection: close" if close else b"Connection: keep-alive")
    if date is not None and not has_date:
        lines.append(b"Date: " + date)
    if server is not None and not has_server:
        lines.append(b"Server: " + server)
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
        # float rather than float | None, unlike keep_alive_timeout below:
        # this is the slow-loris defense and is never optional. Idle reaping
        # is a resource-policy knob a deployment behind a connection-pooling
        # proxy may legitimately turn off.
        keep_alive_timeout: float | None = DEFAULT_KEEP_ALIVE_TIMEOUT,
        write_pause_watermark: int = DEFAULT_WRITE_PAUSE_WATERMARK,
        write_resume_watermark: int = DEFAULT_WRITE_RESUME_WATERMARK,
        pipeline_pause_watermark: int = DEFAULT_PIPELINE_PAUSE_WATERMARK,
        pipeline_resume_watermark: int = DEFAULT_PIPELINE_RESUME_WATERMARK,
        body_pause_watermark: int = DEFAULT_BODY_PAUSE_WATERMARK,
        body_resume_watermark: int = DEFAULT_BODY_RESUME_WATERMARK,
        websocket_max_message_size: int = DEFAULT_MAX_MESSAGE_SIZE,
        ws_pause_watermark: int = DEFAULT_WS_PAUSE_WATERMARK,
        ws_resume_watermark: int = DEFAULT_WS_RESUME_WATERMARK,
        ws_ping_interval: float | None = DEFAULT_WS_PING_INTERVAL,
        ws_ping_timeout: float | None = DEFAULT_WS_PING_TIMEOUT,
        ws_close_timeout: float | None = DEFAULT_WS_CLOSE_TIMEOUT,
        date_header: bool = True,
        server_header: bytes | None = DEFAULT_SERVER_HEADER,
        upgrade_protocols: frozenset[str] = DEFAULT_UPGRADE_PROTOCOLS,
        server_state: ServerState | None = None,
        state: dict | None = None,
    ) -> None:
        self.app = app
        self._max_header_size = max_header_size
        self._max_headers = max_headers
        self._max_body_size = max_body_size
        self._head_timeout = head_timeout
        self._keep_alive_timeout = keep_alive_timeout
        self._write_pause_watermark = write_pause_watermark
        self._write_resume_watermark = write_resume_watermark
        self._pipeline_pause_watermark = pipeline_pause_watermark
        self._pipeline_resume_watermark = pipeline_resume_watermark
        self._body_pause_watermark = body_pause_watermark
        self._body_resume_watermark = body_resume_watermark
        self._websocket_max_message_size = websocket_max_message_size
        self._ws_pause_watermark = ws_pause_watermark
        self._ws_resume_watermark = ws_resume_watermark
        self._ws_ping_interval = ws_ping_interval
        self._ws_ping_timeout = ws_ping_timeout
        self._ws_close_timeout = ws_close_timeout
        self._date_header = date_header
        self._server_header = server_header
        self._upgrade_protocols = upgrade_protocols
        self._server_state = server_state
        self._state = state
        self.transport: asyncio.Transport | None = None
        self.parser: HTTP11Parser | None = None

    @property
    def is_idle(self) -> bool:
        """True when no request is in flight, so closing now loses nothing."""
        return not self._inflight_tasks

    @property
    def is_websocket(self) -> bool:
        """True once this connection stopped being an HTTP connection.

        Note it is deliberately *not* folded into is_idle: a live websocket
        is honestly busy. Graceful shutdown closes these explicitly rather
        than draining them, since a session with nothing to say would
        otherwise hold the drain open to its full deadline.
        """
        return self._ws_state is not None

    # -- connection lifecycle ------------------------------------------------

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = cast(asyncio.Transport, transport)
        self.parser = HTTP11Parser(
            max_header_size=self._max_header_size,
            max_headers=self._max_headers,
            max_body_size=self._max_body_size,
            upgrade_protocols=self._upgrade_protocols,
        )
        # set_write_buffer_limits had never been called: pause_writing,
        # resume_writing and _drain were running on asyncio's defaults, which
        # is the difference between "we chose this" and "we inherited it". An
        # unbounded-in-practice write buffer is how one slow reader turns a
        # streaming response into server-side memory growth.
        try:
            # ValueError, not just NotImplementedError: asyncio rejects
            # low > high outright, and a misconfigured pair would otherwise
            # raise out of connection_made on *every* connection. A tuning
            # knob must not be able to take the server down.
            self.transport.set_write_buffer_limits(
                high=self._write_pause_watermark,
                low=self._write_resume_watermark,
            )
        except NotImplementedError, ValueError:
            # asyncio.Transport's base implementation raises, so a transport
            # with no write buffer at all -- a test double, or a foreign
            # transport -- lands here. A hardening knob must not be able to
            # break a connection that would otherwise work.
            pass
        self._loop = asyncio.get_running_loop()
        self._client = self.transport.get_extra_info("peername")
        self._server = self.transport.get_extra_info("sockname")

        self._receive_queues: list[asyncio.Queue[Message]] = []
        self._current_receive_queue: asyncio.Queue[Message] | None = None
        self._inflight_tasks: set[asyncio.Task[None]] = set()
        self._write_turnstile: asyncio.Event = _set_event()
        self._disconnected = False
        self._closed = False
        self._reading_paused = False
        self._paused_writing = False
        self._drain_waiters: list[asyncio.Future[None]] = []

        # None means "ordinary HTTP connection". Everything else here stays
        # unset until an upgrade head arrives.
        self._ws_state: _WSState | None = None
        self._ws_parser: FrameParser | None = None
        self._ws_queue: asyncio.Queue[Message] | None = None
        self._ws_key: bytes = b""
        self._ws_prologue: bytes = b""
        self._ws_close_sent = False
        self._ws_disconnect_message: Message | None = None
        self._ws_ping_handle: asyncio.TimerHandle | None = None
        # The payload of the ping we are waiting on a pong for, or None when
        # no ping is outstanding. Matching on the payload rather than a bare
        # flag means an unsolicited pong cannot clear a real deadline.
        self._ws_pending_ping: bytes | None = None
        self._ws_pending_close: tuple[int, str, bytes] | None = None
        self._ws_close_deadline: asyncio.TimerHandle | None = None

        self._timeout_handle: asyncio.TimerHandle | None = None
        self._timeout_mode: _TimerMode | None = None
        self._arm_timeout(_TimerMode.HEAD)

        if self._server_state is not None:
            self._server_state.connections.add(self)

    def _head_extras(self) -> tuple[bytes | None, bytes | None]:
        """The (date, server) values for one response.

        The single place in this module that reads the wall clock, so that
        _encode_response_head stays pure. Note time.time(), not loop.time():
        the loop's clock is monotonic and says nothing about the date.
        """
        date = _formatted_date(time.time()) if self._date_header else None
        return date, self._server_header

    # -- the connection timer ---------------------------------------------------

    def _arm_timeout(self, mode: _TimerMode) -> None:
        """(Re)arm the single connection timer in one of its two modes.

        One timer rather than two: at most one of "waiting for a head" and
        "idle between requests" is ever true, and two handles would mean two
        cancel sites to keep in step.
        """
        self._disarm_timeout()
        # Never on a websocket. Once _ws_state is set this has stopped being
        # an HTTP connection: its liveness is the ping timer's job, and hours
        # of silence are healthy rather than suspicious.
        if self._closed or self._ws_state is not None:
            return
        if self.transport is None or self.transport.is_closing():
            return
        delay = (
            self._head_timeout if mode is _TimerMode.HEAD else self._keep_alive_timeout
        )
        if delay is None:
            return
        self._timeout_mode = mode
        self._timeout_handle = self._loop.call_later(delay, self._on_timeout)

    def _disarm_timeout(self) -> None:
        if self._timeout_handle is not None:
            self._timeout_handle.cancel()
        self._timeout_handle = None
        self._timeout_mode = None

    def _arm_idle_timeout(self) -> None:
        """Arm the between-requests timer, picking the mode from the parser.

        The trap in this whole mechanism. A non-empty parser buffer after a
        response means the *next* request head is already partly here -- that
        is slow loris, not idleness, and it gets head_timeout and a 408.
        Reading it the other way round hands the attacker the more generous
        deadline, and writes a 408 into a connection whose client is merely
        thinking -- where that client will pair it with the request it is
        about to send.
        """
        parser = self.parser
        mid = parser is not None and parser.mid_request
        self._arm_timeout(_TimerMode.HEAD if mid else _TimerMode.KEEP_ALIVE)

    def _on_timeout(self) -> None:
        mode, self._timeout_mode = self._timeout_mode, None
        self._timeout_handle = None
        if (
            self.transport is None
            or self.transport.is_closing()
            or self._ws_state is not None
            # Not redundant with _arm_idle_if_free's own is_idle check. A
            # timer armed while the connection was idle can still be pending
            # when a new request starts, and the arming site cannot un-fire
            # it. This is the guard that actually stops a busy connection
            # being reaped.
            or not self.is_idle
        ):
            return
        self._closed = True
        if mode is not _TimerMode.HEAD:
            # An idle keep-alive connection is closed *silently*. A 408 would
            # arrive at a client with nothing outstanding, which will pair it
            # with whatever request it writes next -- possibly one already in
            # flight. Closing is the only unambiguous signal, and is what
            # uvicorn and nginx both do here.
            self.transport.close()
            return
        # Nothing has been written on this connection yet, so a 408 is
        # unambiguous, and RFC 9110 section 15.5.9 is explicit that it is the
        # right answer to a client that connected and then went quiet.
        body = b"Request Timeout"
        date, server = self._head_extras()
        head = _encode_response_head(
            408,
            [
                (b"content-length", str(len(body)).encode()),
                (b"content-type", b"text/plain; charset=utf-8"),
            ],
            close=True,
            date=date,
            server=server,
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
        self._disarm_timeout()
        self._disarm_ws_ping()
        self._disarm_ws_close_deadline()
        self._disconnected = True
        # Anyone blocked in _drain() is waiting on a resume_writing() that is
        # never coming. Resolve rather than cancel: the caller returns into
        # send(), whose is_closing() guards drop the write, and unwinds its own
        # way -- instead of waiting a full loop iteration for the deferred
        # task.cancel() below.
        self._wake_drain_waiters()
        if self._server_state is not None:
            self._server_state.connections.discard(self)
        for queue in self._receive_queues:
            if queue is self._ws_queue:
                continue
            queue.put_nowait({"type": "http.disconnect"})
        if self._ws_state is not None:
            # 1006: the connection went away without a close frame. Skipped
            # entirely if a real code was already delivered, so a clean 1000
            # is never overwritten by the abnormal-closure code that follows
            # every close.
            self._enqueue_ws_disconnect(CloseCode.ABNORMAL)
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
        self._wake_drain_waiters()

    def _wake_drain_waiters(self) -> None:
        waiters, self._drain_waiters = self._drain_waiters, []
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)

    async def _drain(self) -> None:
        """Block while the transport's write buffer is over its high mark.

        A list rather than the single slot this used to be. One slot is only
        correct while exactly one coroutine can be writing, and a websocket
        application with a reader task and a writer task breaks that: the
        second caller's future replaced the first's, and the first was then
        never resolved. Its only cure was the connection dying -- a permanent
        hang, not backpressure.
        """
        if not self._paused_writing or self._closed:
            return
        waiter = self._loop.create_future()
        self._drain_waiters.append(waiter)
        try:
            await waiter
        finally:
            # Cancellation is the other way out of here. Drop the future so a
            # cancelled task does not leave one retained for the life of a
            # long keep-alive connection.
            if waiter in self._drain_waiters:
                self._drain_waiters.remove(waiter)

    # -- incoming bytes ---------------------------------------------------------

    def data_received(self, data: bytes) -> None:
        if self._closed:
            return
        if self._ws_state is not None:
            # The upgrade is a connection *mode switch*, not a kind of
            # request: HTTP11Parser is never fed again, so no second request
            # head can be emitted for bytes the client believes are tunnelled.
            # It is also what makes the permanently-held write turnstile below
            # harmless -- no successor request can ever wait on it.
            self._ws_data_received(data)
            return
        if self.parser is None:
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

        # An idle-mode timer whose connection just received the first bytes of
        # a new request must switch to head mode: what is in the buffer now is
        # a partial head, and a partial head that never completes is slow
        # loris. This fires only on the idle -> mid-request transition -- once
        # the mode is HEAD the condition is False, so a one-byte-per-second
        # drip cannot keep pushing its own deadline out. If a head completed
        # in this call, _handle_event already disarmed and the mode is None,
        # so the check is inert.
        if (
            self._timeout_mode is _TimerMode.KEEP_ALIVE
            and self.parser is not None
            and self.parser.mid_request
        ):
            self._arm_timeout(_TimerMode.HEAD)

    def _handle_event(self, event: Event) -> None:
        if isinstance(event, RequestHeadComplete):
            # Unconditionally, on every head: a request is now in flight and
            # neither timeout applies until it finishes. The one-shot
            # _got_first_head latch this replaces is what left every
            # keep-alive connection after the first request unreapable.
            self._disarm_timeout()
            if event.head.upgrade == "websocket":
                self._begin_websocket(event.head)
                return
            if event.head.upgrade is not None:
                # The parser stopped framing HTTP for this connection, so no
                # body event will ever follow this head. Dispatching it as an
                # ordinary request would leave the app blocked in receive()
                # forever. Only websocket is implemented.
                self._reject_upgrade()
                return
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

    def _reject_upgrade(self) -> None:
        self._closed = True
        self._disarm_timeout()
        my_turn = self._write_turnstile
        task = self._loop.create_task(self._write_failure_response(501, my_turn))
        self._inflight_tasks.add(task)
        task.add_done_callback(self._inflight_tasks.discard)

    def _fail(self, exc: HTTPParserError) -> None:
        if self._ws_state is not None:  # pragma: no cover - data_received guards
            # Unreachable: once upgraded, data_received routes bytes to the
            # frame parser and never touches HTTP11Parser again. It is guarded
            # anyway because getting here would hang the connection forever --
            # after an upgrade self._write_turnstile is an Event nobody will
            # ever set, and the failure path below awaits it.
            self._ws_abort(CloseCode.PROTOCOL_ERROR, "http parsing after upgrade")
            return
        self._closed = True
        self._disarm_timeout()
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
        self,
        status: int,
        my_turn: asyncio.Event,
        extra_headers: list[tuple[bytes, bytes]] | None = None,
    ) -> None:
        reason = _reason_phrase(status) or b"Bad Request"
        date, server = self._head_extras()
        head = _encode_response_head(
            status,
            [
                (b"content-length", str(len(reason)).encode()),
                (b"content-type", b"text/plain; charset=utf-8"),
                *(extra_headers or []),
            ],
            close=True,
            date=date,
            server=server,
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

        scope = build_scope(
            head, client=self._client, server=self._server, state=self._state
        )
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
        # A deeply pipelined connection is allocating a task, a queue and a
        # scope per request from a couple of dozen bytes each. Stop reading
        # from it until the backlog drains.
        if len(self._inflight_tasks) >= self._pipeline_pause_watermark:
            self._pause_reading()

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
            self._arm_idle_if_free()
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
        self._arm_idle_if_free()

    def _arm_idle_if_free(self) -> None:
        """Start the between-requests timer once nothing is in flight.

        Only when the *last* task finishes: pipelining means several
        concurrent tasks, and arming while any of them is still running would
        reap a connection that is busy. A task that raised has already closed
        the transport, which _arm_timeout's is_closing() guard filters -- so
        no timer is ever armed on a corpse.

        Note the failure-response tasks spawned by _fail/_reject_upgrade/
        _fail_handshake do not route through here (they use a bare discard
        callback) and set _closed anyway, so they cannot re-arm either.
        """
        self._resume_reading_unless_backed_up(
            queue_clear=self._current_receive_queue is None
            or self._current_receive_queue.qsize() <= self._body_resume_watermark
        )
        if self.is_idle:
            self._arm_idle_timeout()

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
                # A shutdown started while this request was in flight: finish
                # it, but tell the client not to reuse the connection. Without
                # this the client sees keep-alive on a socket the server is
                # about to close and may pipeline a request into the void.
                if self._server_state is not None and self._server_state.should_exit:
                    close = True
                await my_turn.wait()
                if self.transport is not None and not self.transport.is_closing():
                    # Encoded here, inside the post-validation branch, so a
                    # head that _validate_response_headers rejects writes
                    # nothing at all -- including no Date.
                    date_value, server_value = self._head_extras()
                    self.transport.write(
                        _encode_response_head(
                            status,
                            headers,
                            close=close,
                            http_version=head.http_version,
                            date=date_value,
                            server=server_value,
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
            self._resume_reading_unless_backed_up(
                queue_clear=queue.qsize() <= self._body_resume_watermark
            )
            return message

        return receive

    def _push_body_chunk(self, message: Message) -> None:
        queue = self._current_receive_queue
        if (
            queue is None
        ):  # pragma: no cover - parser always emits BodyChunk after RequestHeadComplete
            raise RuntimeError("received a body chunk with no active request")
        self._push_to_queue(queue, message, self._body_pause_watermark)

    def _pause_reading(self) -> None:
        if not self._reading_paused and self.transport is not None:
            self.transport.pause_reading()
            self._reading_paused = True

    def _resume_reading_unless_backed_up(self, queue_clear: bool) -> None:
        """Resume reads only when *every* reason to have paused is gone.

        There are two independent reasons now -- a request's queue over its
        watermark, and too many application tasks in flight -- and one flag
        owning the transport's read state. Resuming on one reason while the
        other still holds is the double-resume bug in a new costume.
        """
        if not self._reading_paused or self.transport is None:
            return
        if not queue_clear:
            return
        if len(self._inflight_tasks) > self._pipeline_resume_watermark:
            return
        self.transport.resume_reading()
        self._reading_paused = False

    def _push_to_queue(
        self, queue: asyncio.Queue[Message], message: Message, watermark: int
    ) -> None:
        """Enqueue for the app, pausing reads if it is falling behind.

        Shared by the HTTP body path and the websocket message path so that
        one flag owns the transport's read state. Two flags both driving one
        transport is the classic double-resume bug, and after an upgrade the
        two paths are mutually exclusive anyway.
        """
        queue.put_nowait(message)
        if queue.qsize() >= watermark:
            self._pause_reading()

    # -- websockets --------------------------------------------------------------

    def _begin_websocket(self, head: RequestHead) -> None:
        assert self.parser is not None
        # Clients legitimately send their first frame in the same TCP segment
        # as the handshake, and those bytes are already inside the HTTP
        # parser's buffer. Without this they would be silently dropped, which
        # every send-immediately-after-connect client would hit.
        prologue = self.parser.take_buffer()
        try:
            key = validate_handshake(head.headers)
        except HandshakeError as exc:
            self._fail_handshake(exc)
            return

        self._ws_state = _WSState.CONNECTING
        self._ws_key = key
        self._ws_prologue = prologue
        self._ws_parser = FrameParser(max_message_size=self._websocket_max_message_size)

        queue: asyncio.Queue[Message] = asyncio.Queue()
        self._receive_queues.append(queue)
        self._ws_queue = queue
        # ASGI: the server opens the conversation, and the application answers
        # with accept or close.
        queue.put_nowait({"type": "websocket.connect"})

        # Hold off reading until the app has decided. Bounds what an
        # unauthorized client can push at us to a single TCP read.
        if self.transport is not None and not self._reading_paused:
            self.transport.pause_reading()
            self._reading_paused = True

        scope = build_scope(
            head, client=self._client, server=self._server, state=self._state
        )
        my_turn = self._write_turnstile
        # Installed but never set by the websocket path. Harmless precisely
        # because the parser is now terminal: no successor request exists to
        # wait on it. See data_received.
        my_done = asyncio.Event()
        self._write_turnstile = my_done
        send = self._make_ws_send(my_turn)
        receive = self._make_ws_receive(queue)

        task = asyncio.ensure_future(self.app(scope, receive, send), loop=self._loop)
        self._inflight_tasks.add(task)
        task.add_done_callback(
            functools.partial(self._on_ws_task_done, my_done=my_done)
        )

    def _fail_handshake(self, exc: HandshakeError) -> None:
        self._closed = True
        self._disarm_timeout()
        my_turn = self._write_turnstile
        task = self._loop.create_task(
            self._write_failure_response(exc.status, my_turn, exc.headers)
        )
        self._inflight_tasks.add(task)
        task.add_done_callback(self._inflight_tasks.discard)

    def _ws_write(self, data: bytes) -> None:
        if self.transport is not None and not self.transport.is_closing():
            self.transport.write(data)

    def _send_close_frame_once(self, frame: bytes) -> None:
        """Write a close frame, at most one per connection.

        Six paths can decide to close -- peer close, codec violation, ping
        timeout, app close, handler return, server shutdown -- and every one
        of them needs the same guard. Collapsing them here removes five
        places a future edit could forget it.
        """
        if self._ws_close_sent:
            return
        self._ws_close_sent = True
        self._ws_write(frame)

    # -- websocket keepalive -------------------------------------------------

    def _arm_ws_ping(self) -> None:
        """(Re)start the idle countdown to the next server-initiated ping."""
        self._disarm_ws_ping()
        if self._ws_ping_interval is None or self._ws_state is not _WSState.CONNECTED:
            return
        self._ws_ping_handle = self._loop.call_later(
            self._ws_ping_interval, self._on_ws_ping_due
        )

    def _disarm_ws_ping(self) -> None:
        if self._ws_ping_handle is not None:
            self._ws_ping_handle.cancel()
            self._ws_ping_handle = None

    def _on_ws_ping_due(self) -> None:
        self._ws_ping_handle = None
        if self._ws_state is not _WSState.CONNECTED:
            return
        if self._ws_pending_ping is not None:
            # The previous ping was never answered within a full interval,
            # so the peer is gone even though TCP has not noticed.
            self._ws_abort(CloseCode.INTERNAL_ERROR, "ping timeout")
            return
        payload = os.urandom(4)
        self._ws_pending_ping = payload
        self._ws_write(encode_frame(Opcode.PING, payload))
        # The next firing is the timeout for this ping when one is
        # configured, and otherwise just the next idle interval.
        delay = (
            self._ws_ping_timeout
            if self._ws_ping_timeout is not None
            else self._ws_ping_interval
        )
        if delay is not None:
            self._ws_ping_handle = self._loop.call_later(delay, self._on_ws_ping_due)

    def _ws_data_received(self, data: bytes) -> None:
        parser = self._ws_parser
        if parser is None or self._ws_state in (_WSState.CLOSING, _WSState.CLOSED):
            # CLOSING: the code is decided and nothing the peer says can
            # change it, so decoding on would only queue events for an
            # application that is already unwinding.
            return
        # Any inbound byte proves the peer is alive, so a busy connection
        # never spends anything on keepalive -- and an outstanding ping is
        # answered by definition, whatever arrived.
        self._ws_pending_ping = None
        self._arm_ws_ping()
        try:
            events = parser.feed_data(data)
        except WebSocketProtocolError as exc:
            for event in exc.partial_events:
                self._handle_frame_event(event)
            self._ws_abort(exc.code, exc.reason)
            return
        for event in events:
            self._handle_frame_event(event)

    def _handle_frame_event(self, event: FrameEvent) -> None:
        if isinstance(event, TextMessage):
            self._push_ws_message({"type": "websocket.receive", "text": event.data})
        elif isinstance(event, BinaryMessage):
            self._push_ws_message({"type": "websocket.receive", "bytes": event.data})
        elif isinstance(event, Ping):
            # ASGI defines no websocket.ping message, so there is nowhere to
            # deliver this. Answering with the identical payload is the only
            # conforming option, and the application never learns it happened.
            self._ws_write(encode_frame(Opcode.PONG, event.data))
        elif isinstance(event, Pong):
            if event.data == self._ws_pending_ping:
                self._ws_pending_ping = None
            # An unsolicited pong is legal and means nothing to us -- and,
            # because the payload must match, cannot clear a real deadline.
        elif isinstance(event, CloseReceived):
            self._ws_peer_closed(event)
        else:  # pragma: no cover - exhaustive over FrameEvent
            raise AssertionError(f"unhandled frame event: {event!r}")  # noqa: TRY004

    def _push_ws_message(self, message: Message) -> None:
        queue = self._ws_queue
        if queue is None:  # pragma: no cover - set before any frame can arrive
            return
        self._push_to_queue(queue, message, self._ws_pause_watermark)

    def _ws_peer_closed(self, event: CloseReceived) -> None:
        # 1005 is a "no status was present" sentinel and must never go on the
        # wire, so a payloadless close is echoed payloadless.
        echo = None if event.code == CloseCode.NO_STATUS else event.code
        frame = encode_close_frame(echo, event.reason)
        if self._ws_state is _WSState.CONNECTED:
            # Deferred for the same reason a violation is, and this path is
            # the more damaging of the two: a client sending a last message
            # and then closing in one segment is ordinary traffic, and until
            # now the application's echo raised RuntimeError into the handler
            # rather than merely being dropped.
            self._defer_ws_close(event.code, event.reason, frame)
            return
        self._send_close_frame_once(frame)
        self._finish_websocket(event.code, event.reason)

    def _ws_abort(self, code: int, reason: str) -> None:
        frame = encode_close_frame(code, reason)
        if self._ws_state is _WSState.CONNECTED:
            self._defer_ws_close(code, reason, frame)
            return
        # CONNECTING (no application has accepted, so there is nobody to give
        # a turn to) or already CLOSING/CLOSED: nothing to defer for.
        self._send_close_frame_once(frame)
        self._finish_websocket(code, reason)

    def _defer_ws_close(self, code: int, reason: str, frame: bytes) -> None:
        """Record a close, let the application task finish, write it after.

        The websocket counterpart of _fail(), and it exists for the same
        reason: a close discovered while decoding a batch must not shut the
        transport before the good messages ahead of it *in that same batch*
        have been answered. On the HTTP side the synchronization point is the
        write turnstile; here there is none -- a websocket task holds a
        turnstile it never releases -- so the synchronization point is the
        task itself, and the frame is written by _on_ws_task_done.

        The frame is encoded by the caller so the peer-echo's payloadless-1005
        rule stays in _ws_peer_closed where it already lives.
        """
        self._ws_state = _WSState.CLOSING
        self._ws_pending_close = (code, reason, frame)
        self._disarm_ws_ping()
        # Synchronously, exactly as before: the real code must be recorded
        # before any connection_lost can enqueue 1006, and it must sit in the
        # queue *behind* the good message so the application consumes that
        # message, answers it, and then ends on its own.
        self._enqueue_ws_disconnect(code, reason)
        if self._ws_close_timeout is not None:
            self._ws_close_deadline = self._loop.call_later(
                self._ws_close_timeout, self._on_ws_close_deadline
            )

    def _flush_ws_close(self) -> None:
        """Write the deferred close frame and finish the connection."""
        pending, self._ws_pending_close = self._ws_pending_close, None
        if pending is None:
            return
        code, reason, frame = pending
        self._disarm_ws_close_deadline()
        self._send_close_frame_once(frame)
        self._finish_websocket(code, reason)

    def _disarm_ws_close_deadline(self) -> None:
        if self._ws_close_deadline is not None:
            self._ws_close_deadline.cancel()
            self._ws_close_deadline = None

    def _on_ws_close_deadline(self) -> None:
        """A handler that never returns must not hold the close frame hostage.

        transport.close() inside _flush_ws_close then triggers
        connection_lost, which cancels the stuck task.
        """
        self._ws_close_deadline = None
        self._flush_ws_close()

    def _finish_websocket(self, code: int, reason: str = "") -> None:
        self._ws_state = _WSState.CLOSED
        self._disarm_ws_ping()
        self._disarm_ws_close_deadline()
        # Enqueued *before* closing the transport, so an application blocked
        # in receive() observes the real code rather than the 1006 that
        # connection_lost would otherwise deliver first.
        self._enqueue_ws_disconnect(code, reason)
        if self.transport is not None and not self.transport.is_closing():
            # close(), never abort(): abort discards the close frame that was
            # just written.
            self.transport.close()

    def _enqueue_ws_disconnect(self, code: int, reason: str = "") -> None:
        if self._ws_disconnect_message is not None:
            return
        message: Message = {
            "type": "websocket.disconnect",
            "code": int(code),
            "reason": reason,
        }
        self._ws_disconnect_message = message
        if self._ws_queue is not None:
            self._ws_queue.put_nowait(message)

    def shutdown_websocket(self) -> None:
        """Close a live websocket for graceful shutdown.

        Called by the server before it drains, because a websocket connection
        is never idle: waiting for one to finish on its own would burn the
        whole shutdown deadline and then hand the client a bare TCP close.
        """
        if self._ws_state is None or self._ws_state is _WSState.CLOSED:
            return
        if self._ws_state is _WSState.CLOSING:
            # Already closing with a code the peer is owed. Shutdown does not
            # get to relabel it 1001; it only stops waiting for the handler.
            self._flush_ws_close()
            return
        if self._ws_state is _WSState.CONNECTED:
            self._send_close_frame_once(
                encode_close_frame(CloseCode.GOING_AWAY, "server shutting down")
            )
        self._finish_websocket(CloseCode.GOING_AWAY, "server shutting down")

    def _on_ws_task_done(
        self, task: asyncio.Task[None], *, my_done: asyncio.Event
    ) -> None:
        self._inflight_tasks.discard(task)
        if self._ws_queue is not None:
            try:
                self._receive_queues.remove(self._ws_queue)
            except ValueError:
                pass
        if not my_done.is_set():
            my_done.set()
        if self._ws_state is _WSState.CLOSING:
            # The application has had its turn, and anything it wrote is
            # already on the wire ahead of this frame.
            self._flush_ws_close()
        elif self._ws_state is _WSState.CONNECTED:
            # A handler that simply returns has ended the session. Without
            # this the socket dangles for the life of the process.
            self._send_close_frame_once(encode_close_frame(CloseCode.NORMAL))
            self._finish_websocket(CloseCode.NORMAL)
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

    def _make_ws_send(self, my_turn: asyncio.Event) -> Send:
        async def send(message: Message) -> None:
            msg_type = message.get("type")
            state = self._ws_state
            if state is _WSState.CONNECTING:
                if msg_type == "websocket.accept":
                    await self._ws_accept(message, my_turn)
                elif msg_type == "websocket.close":
                    await self._ws_deny(my_turn)
                else:
                    raise RuntimeError(
                        f"unexpected ASGI message type {msg_type!r} before "
                        "websocket.accept"
                    )
            elif state is _WSState.CONNECTED:
                if msg_type == "websocket.send":
                    self._ws_send_message(message)
                    await self._drain()
                elif msg_type == "websocket.close":
                    self._ws_close(message)
                elif msg_type == "websocket.accept":
                    raise RuntimeError("websocket.accept sent more than once")
                else:
                    raise RuntimeError(f"unexpected ASGI message type: {msg_type!r}")
            elif state is _WSState.CLOSING:
                # The whole point of CLOSING: the connection is going away
                # with a code that is already decided, the application has not
                # returned yet, and its pending sends are exactly what this
                # window exists to let through.
                if msg_type == "websocket.send":
                    self._ws_send_message(message)
                    await self._drain()
                elif msg_type != "websocket.close":
                    raise RuntimeError(f"unexpected ASGI message type: {msg_type!r}")
                # A close from the application here is a no-op, not an
                # override. `finally: await ws.close()` is the standard
                # handler shape, and honouring it would put 1000 on the wire
                # where the peer is owed 1002 -- turning the seven Autobahn
                # non-strict cases into seven outright failures.
            elif msg_type != "websocket.close":
                raise RuntimeError(f"{msg_type!r} sent after the websocket was closed")
            # A close on an already-closed websocket is a no-op rather than an
            # error: `finally: await ws.close()` is the commonest idiom there
            # is, and it must not explode because the peer went first.

        return send

    async def _ws_accept(self, message: Message, my_turn: asyncio.Event) -> None:
        subprotocol = message.get("subprotocol")
        extra_headers = list(message.get("headers", []))
        _validate_response_headers(extra_headers)
        # A pipelined HTTP request ahead of the handshake must have its
        # response on the wire before the 101.
        await my_turn.wait()
        self._ws_state = _WSState.CONNECTED
        self._ws_write(
            encode_handshake_response(self._ws_key, subprotocol, extra_headers)
        )
        # Lift the pre-accept read bound first, so that if the prologue below
        # is itself enough to breach the queue watermark, the pause it takes
        # stands rather than being undone here.
        if self._reading_paused and self.transport is not None:
            self.transport.resume_reading()
            self._reading_paused = False
        self._arm_ws_ping()
        prologue, self._ws_prologue = self._ws_prologue, b""
        if prologue:
            self._ws_data_received(prologue)

    async def _ws_deny(self, my_turn: asyncio.Event) -> None:
        # ASGI: a close before accept is a *denied handshake*, which is an
        # ordinary HTTP response -- 101 never happens, so neither does a close
        # frame. It also means "no such websocket route" surfaces as 403
        # rather than 404; the app layer's only vocabulary for refusal here is
        # websocket.close.
        self._ws_state = _WSState.CLOSED
        self._enqueue_ws_disconnect(CloseCode.NORMAL)
        await my_turn.wait()
        date, server = self._head_extras()
        head = _encode_response_head(
            403,
            [
                (b"content-length", b"0"),
                (b"content-type", b"text/plain; charset=utf-8"),
            ],
            close=True,
            date=date,
            server=server,
        )
        self._ws_write(head)
        if self.transport is not None and not self.transport.is_closing():
            self.transport.close()

    def _ws_send_message(self, message: Message) -> None:
        data = message.get("bytes")
        if data is not None:
            self._ws_write(encode_frame(Opcode.BINARY, data))
            return
        text = message.get("text")
        if text is None:
            raise RuntimeError("websocket.send must carry either 'bytes' or 'text'")
        self._ws_write(encode_frame(Opcode.TEXT, text.encode("utf-8")))

    def _ws_close(self, message: Message) -> None:
        code = int(message.get("code", CloseCode.NORMAL))
        reason = message.get("reason") or ""
        self._send_close_frame_once(encode_close_frame(code, reason))
        self._finish_websocket(code, reason)

    def _make_ws_receive(self, queue: asyncio.Queue[Message]) -> Receive:
        async def receive() -> Message:
            if not queue.empty():
                message = queue.get_nowait()
            elif self._ws_disconnect_message is not None:
                # Repeat rather than hang: nothing more will ever arrive, and
                # the code the app already saw is the code it should keep
                # seeing.
                return self._ws_disconnect_message
            elif self._disconnected:
                return {
                    "type": "websocket.disconnect",
                    "code": int(CloseCode.ABNORMAL),
                    "reason": "",
                }
            else:
                message = await queue.get()
            if (
                self._reading_paused
                # Only once accepted: before that the pause is deliberate, and
                # resuming here would undo the pre-accept read bound.
                and self._ws_state is _WSState.CONNECTED
                and self.transport is not None
                and queue.qsize() <= self._ws_resume_watermark
            ):
                self.transport.resume_reading()
                self._reading_paused = False
            return message

        return receive
