"""Server lifecycle: binding a socket, serving, and shutting down cleanly.

``HTTPProtocol`` handles one connection; this module handles the *server* --
the listening socket, the set of live connections, signal handling, and the
graceful-shutdown sequence. It is the public entry point into the server
layer: ``sonix.run(app)`` is a thin wrapper over ``Server(Config(app)).run()``.

Splitting ``startup``/``serve``/``shutdown`` rather than exposing only a
blocking ``run()`` is deliberate: tests and benchmark harnesses need to bring
a server up, drive it, and take it down without ``serve_forever`` owning the
event loop.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import signal
import traceback
from dataclasses import dataclass, field
from typing import Any

from sonix.server.parser import DEFAULT_UPGRADE_PROTOCOLS
from sonix.server.protocol import (
    DEFAULT_BODY_PAUSE_WATERMARK,
    DEFAULT_BODY_RESUME_WATERMARK,
    DEFAULT_HEAD_TIMEOUT,
    DEFAULT_WS_PAUSE_WATERMARK,
    DEFAULT_WS_PING_INTERVAL,
    DEFAULT_WS_PING_TIMEOUT,
    DEFAULT_WS_RESUME_WATERMARK,
    HTTPProtocol,
    ServerState,
)
from sonix.server.websockets import DEFAULT_MAX_MESSAGE_SIZE
from sonix.types import ASGIApp, Message, Scope

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8000
DEFAULT_SHUTDOWN_TIMEOUT = 10.0


class LifespanFailure(RuntimeError):
    """The application reported that startup or shutdown failed."""


class LifespanRunner:
    """Drives the ASGI lifespan protocol on the server side.

    The application's lifespan call is long-lived -- it awaits receive()
    twice, once for startup and once for shutdown -- so it runs as a task fed
    by a queue rather than being awaited inline.

    The distinction this class exists to get right is between "this app does
    not speak lifespan" and "this app's startup genuinely failed". Conflating
    them is a real bug uvicorn shipped historically, and the consequence is
    severe in one direction: an app whose database connection fails at startup
    must crash the server loudly, not be silently downgraded to running
    without a lifespan. The rule here is that a *protocol message*
    (lifespan.startup.failed) means a genuine failure and always raises, while
    an *exception escaping the app* before any message means the app does not
    implement lifespan at all -- tolerated under "auto", fatal under "on".
    """

    def __init__(self, app: ASGIApp, *, mode: str = "auto") -> None:
        if mode not in ("auto", "on", "off"):
            raise ValueError(f"lifespan must be 'auto', 'on', or 'off', not {mode!r}")
        self.app = app
        self.mode = mode
        self.state: dict[str, Any] = {}
        self.supported = True
        self._acknowledged = False
        self._queue: asyncio.Queue[Message] = asyncio.Queue()
        self._startup_done = asyncio.Event()
        self._shutdown_done = asyncio.Event()
        self._failure: str | None = None
        self._task: asyncio.Task[None] | None = None

    async def _receive(self) -> Message:
        return await self._queue.get()

    async def _send(self, message: Message) -> None:
        message_type = message["type"]
        if message_type == "lifespan.startup.complete":
            self._acknowledged = True
            self._startup_done.set()
        elif message_type == "lifespan.startup.failed":
            self._acknowledged = True
            self._failure = str(message.get("message", ""))
            self._startup_done.set()
        elif message_type == "lifespan.shutdown.complete":
            self._shutdown_done.set()
        elif message_type == "lifespan.shutdown.failed":
            self._failure = str(message.get("message", ""))
            self._shutdown_done.set()
        else:
            raise RuntimeError(f"unexpected lifespan message {message_type!r}")

    async def _main(self) -> None:
        scope: Scope = {
            "type": "lifespan",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "state": self.state,
        }
        try:
            await self.app(scope, self._receive, self._send)
        except BaseException:
            # Escaped without sending anything -- the app does not implement
            # the lifespan protocol. Note this is only reached when no
            # lifespan.*.failed message arrived; a genuine failure is reported
            # through _failure and takes precedence in startup() below.
            self.supported = False
            if self.mode == "on":
                traceback.print_exc()
        finally:
            # An app that returned without ever acknowledging startup does not
            # implement lifespan either -- returning early on an unrecognized
            # scope type is the polite way to say so, and is more common than
            # raising. Detecting only the raising case would report such an
            # app as supported and then hang shutdown waiting for a reply it
            # is never going to send.
            if not self._acknowledged:
                self.supported = False
            # Unblock anyone waiting, so a non-conforming app cannot hang
            # startup or shutdown forever.
            self._startup_done.set()
            self._shutdown_done.set()

    async def startup(self) -> None:
        if self.mode == "off":
            return
        self._task = asyncio.ensure_future(self._main())
        self._queue.put_nowait({"type": "lifespan.startup"})
        await self._startup_done.wait()

        if self._failure is not None:
            raise LifespanFailure(f"application startup failed:\n{self._failure}")
        if not self.supported and self.mode == "on":
            raise LifespanFailure(
                "lifespan='on' but the application does not support the "
                "lifespan protocol"
            )

    async def shutdown(self) -> None:
        if self.mode == "off" or self._task is None:
            return
        if self.supported:
            self._queue.put_nowait({"type": "lifespan.shutdown"})
            await self._shutdown_done.wait()
        if not self._task.done():
            self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        if self._failure is not None:
            raise LifespanFailure(f"application shutdown failed:\n{self._failure}")


@dataclass(frozen=True, slots=True)
class Config:
    """Everything tunable about a running server.

    Every per-connection limit ``HTTPProtocol`` accepts is surfaced here.
    Before this existed the protocol was fully parameterized but nothing
    forwarded any of it, so the defaults were the only reachable values.
    """

    app: ASGIApp
    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    backlog: int = 2048
    reuse_port: bool = False

    # Per-connection limits, forwarded verbatim to HTTPProtocol.
    max_header_size: int = 8 * 1024
    max_headers: int = 100
    max_body_size: int = 16 * 1024 * 1024
    head_timeout: float = DEFAULT_HEAD_TIMEOUT
    body_pause_watermark: int = DEFAULT_BODY_PAUSE_WATERMARK
    body_resume_watermark: int = DEFAULT_BODY_RESUME_WATERMARK

    # WebSocket limits. websocket_max_message_size is the real DoS defense
    # for a websocket connection -- an over-limit message is refused with
    # close code 1009 rather than buffered without bound.
    websocket_max_message_size: int = DEFAULT_MAX_MESSAGE_SIZE
    ws_pause_watermark: int = DEFAULT_WS_PAUSE_WATERMARK
    ws_resume_watermark: int = DEFAULT_WS_RESUME_WATERMARK

    # Keepalive. A websocket may be silent for hours and perfectly healthy,
    # so liveness needs an explicit probe: ping after this much idle time,
    # and close the connection if no pong follows within the timeout. Either
    # set to None disables that half.
    ws_ping_interval: float | None = DEFAULT_WS_PING_INTERVAL
    ws_ping_timeout: float | None = DEFAULT_WS_PING_TIMEOUT

    # Which Upgrade offers actually switch the connection off HTTP. Empty
    # disables upgrades entirely, and any offer not listed is answered as an
    # ordinary HTTP request.
    upgrade_protocols: frozenset[str] = DEFAULT_UPGRADE_PROTOCOLS

    # How long shutdown waits for in-flight requests before forcing sockets
    # closed. A deadline rather than an unbounded wait: a hung handler must
    # not be able to prevent the process from exiting.
    shutdown_timeout: float = DEFAULT_SHUTDOWN_TIMEOUT

    # "auto" runs lifespan if the app supports it; "on" requires it; "off"
    # skips it entirely. See LifespanRunner for why "auto" must not swallow a
    # genuine startup failure.
    lifespan: str = "auto"

    def protocol_factory(
        self, state: ServerState, lifespan_state: dict[str, Any] | None = None
    ) -> functools.partial[HTTPProtocol]:
        return functools.partial(
            HTTPProtocol,
            self.app,
            max_header_size=self.max_header_size,
            max_headers=self.max_headers,
            max_body_size=self.max_body_size,
            head_timeout=self.head_timeout,
            body_pause_watermark=self.body_pause_watermark,
            body_resume_watermark=self.body_resume_watermark,
            websocket_max_message_size=self.websocket_max_message_size,
            ws_pause_watermark=self.ws_pause_watermark,
            ws_resume_watermark=self.ws_resume_watermark,
            ws_ping_interval=self.ws_ping_interval,
            ws_ping_timeout=self.ws_ping_timeout,
            upgrade_protocols=self.upgrade_protocols,
            server_state=state,
            state=lifespan_state,
        )


@dataclass(slots=True)
class Server:
    config: Config
    state: ServerState = field(default_factory=ServerState)
    _server: asyncio.Server | None = field(default=None, init=False)
    _lifespan: LifespanRunner | None = field(default=None, init=False)

    @property
    def sockets(self) -> list:
        """Bound sockets, available after startup(). Port 0 resolves here."""
        if self._server is None:
            return []
        return list(self._server.sockets)

    async def startup(self) -> None:
        # Lifespan first, then bind. If startup fails there should never have
        # been a listening socket for a client to connect to.
        self._lifespan = LifespanRunner(self.config.app, mode=self.config.lifespan)
        await self._lifespan.startup()

        loop = asyncio.get_running_loop()
        self._server = await loop.create_server(
            self.config.protocol_factory(self.state, self._lifespan.state),
            self.config.host,
            self.config.port,
            backlog=self.config.backlog,
            reuse_port=self.config.reuse_port,
        )

    async def serve_forever(self) -> None:
        if self._server is None:
            raise RuntimeError("startup() must be awaited before serve_forever()")
        # Cancellation is the normal way out of here -- run() cancels this on a
        # signal -- and it is deliberately allowed to propagate rather than
        # being caught: shutdown() is the caller's job, and swallowing the
        # CancelledError would hide a cancellation nobody asked for.
        await self._server.serve_forever()

    async def shutdown(self) -> None:
        """Stop accepting, drain in-flight requests, then close everything.

        Ordering matters. Closing the listening socket first means no new
        connection can arrive during the drain, so the wait is guaranteed to
        make progress rather than chasing a moving target.
        """
        server, self._server = self._server, None
        if server is not None:
            # close() stops accepting immediately. wait_closed() is deliberately
            # NOT awaited here: since 3.12 it also waits for every *existing*
            # connection to finish, so awaiting it before the drain below would
            # deadlock against connections this method has not yet closed.
            server.close()

        # Tells in-flight responses to advertise Connection: close, so clients
        # stop reusing sockets we are about to drop.
        self.state.should_exit = True

        # WebSockets first, and actively. A websocket connection is never
        # idle, so leaving it to the drain below would burn the whole deadline
        # and then hand the client a bare TCP close (1006) instead of a close
        # frame. Telling it to go away lets the handler observe a disconnect
        # and return within a tick, so the drain finishes immediately.
        for connection in list(self.state.connections):
            if connection.is_websocket:
                connection.shutdown_websocket()

        # Idle keep-alive connections are holding nothing; close them now
        # rather than waiting out the deadline for connections with no work.
        for connection in list(self.state.connections):
            if connection.is_idle and connection.transport is not None:
                connection.transport.close()

        await self._drain_busy_connections()

        for connection in list(self.state.connections):
            if connection.transport is not None:
                connection.transport.close()
        self.state.connections.clear()

        if server is not None:
            # Safe now: nothing is left for it to wait on.
            await server.wait_closed()

        # Last, so the application's teardown runs only once no request can
        # still be using the resources it is about to close.
        lifespan, self._lifespan = self._lifespan, None
        if lifespan is not None:
            await lifespan.shutdown()

    async def _drain_busy_connections(self) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.config.shutdown_timeout
        while any(not c.is_idle for c in self.state.connections):
            if loop.time() >= deadline:
                break
            # Polling rather than awaiting the in-flight tasks directly: the
            # task set is per-connection and mutates as requests complete, so
            # gathering a snapshot would miss anything started after it.
            await asyncio.sleep(0.01)

    async def run(self) -> None:
        """Serve until a signal arrives, then shut down gracefully."""
        await self.startup()
        serving = asyncio.ensure_future(self.serve_forever())

        loop = asyncio.get_running_loop()
        installed: list[signal.Signals] = []
        for signame in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(signame, serving.cancel)
            except NotImplementedError:
                # Windows, or a loop that doesn't support signal handlers.
                # KeyboardInterrupt still propagates out of run().
                continue
            installed.append(signame)

        try:
            await serving
        except asyncio.CancelledError:
            pass
        finally:
            for signame in installed:
                loop.remove_signal_handler(signame)
            await self.shutdown()


def run(app: ASGIApp, **kwargs: Any) -> None:
    """Serve ``app`` until interrupted. The one-liner entry point.

    Keyword arguments are Config fields, so ``run(app, port=9000)`` works
    without importing Config.
    """
    asyncio.run(Server(Config(app, **kwargs)).run())
