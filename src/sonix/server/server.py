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
import functools
import signal
from dataclasses import dataclass, field
from typing import Any

from sonix.server.protocol import (
    DEFAULT_BODY_PAUSE_WATERMARK,
    DEFAULT_BODY_RESUME_WATERMARK,
    DEFAULT_HEAD_TIMEOUT,
    HTTPProtocol,
    ServerState,
)
from sonix.types import ASGIApp

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8000
DEFAULT_SHUTDOWN_TIMEOUT = 10.0


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

    # How long shutdown waits for in-flight requests before forcing sockets
    # closed. A deadline rather than an unbounded wait: a hung handler must
    # not be able to prevent the process from exiting.
    shutdown_timeout: float = DEFAULT_SHUTDOWN_TIMEOUT

    def protocol_factory(self, state: ServerState) -> functools.partial[HTTPProtocol]:
        return functools.partial(
            HTTPProtocol,
            self.app,
            max_header_size=self.max_header_size,
            max_headers=self.max_headers,
            max_body_size=self.max_body_size,
            head_timeout=self.head_timeout,
            body_pause_watermark=self.body_pause_watermark,
            body_resume_watermark=self.body_resume_watermark,
            server_state=state,
        )


@dataclass(slots=True)
class Server:
    config: Config
    state: ServerState = field(default_factory=ServerState)
    _server: asyncio.Server | None = field(default=None, init=False)

    @property
    def sockets(self) -> list:
        """Bound sockets, available after startup(). Port 0 resolves here."""
        if self._server is None:
            return []
        return list(self._server.sockets)

    async def startup(self) -> None:
        loop = asyncio.get_running_loop()
        self._server = await loop.create_server(
            self.config.protocol_factory(self.state),
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
