"""ASGI lifespan: startup and shutdown, driven by the server.

The lifespan protocol is how a server tells an application "you are about to
start serving" and "you are about to stop", which is what lets an app open a
database connection once rather than per request.

Sonix's primary API is an async context manager, not on_startup/on_shutdown
lists:

    @contextlib.asynccontextmanager
    async def lifespan(app):
        connection = sqlite3.connect(...)
        yield {"db": connection}
        connection.close()

The context manager keeps setup and its matching teardown in one function and
lets the resource live in a local variable between them, which paired event
lists cannot express without a module-level global. Whatever it yields is
merged into scope["state"], which the server copies into every request scope.

The event-list form is still supported as sugar, because a startup hook with
no resource to hand onward is common enough that requiring a context manager
for it would be noise.
"""

from __future__ import annotations

import contextlib
import inspect
import traceback
from collections.abc import AsyncIterator, Callable, Mapping
from typing import Any

from sonix.types import Message, Receive, Scope, Send

LifespanFactory = Callable[[Any], contextlib.AbstractAsyncContextManager[Any]]


async def _call(fn: Callable[[], Any]) -> None:
    result = fn()
    if inspect.isawaitable(result):
        await result


def events_lifespan(
    on_startup: list[Callable[[], Any]],
    on_shutdown: list[Callable[[], Any]],
) -> LifespanFactory:
    """Adapt on_startup/on_shutdown lists to the context-manager form."""

    @contextlib.asynccontextmanager
    async def lifespan(app: Any) -> AsyncIterator[None]:
        for fn in on_startup:
            await _call(fn)
        yield
        for fn in on_shutdown:
            await _call(fn)

    return lifespan


@contextlib.asynccontextmanager
async def _null_lifespan(app: Any) -> AsyncIterator[None]:
    yield


class LifespanHandler:
    """Handles a scope of type "lifespan" on behalf of a Sonix app."""

    def __init__(self, app: Any, factory: LifespanFactory | None = None) -> None:
        self.app = app
        self.factory = factory if factory is not None else _null_lifespan

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        await self._expect(receive, "lifespan.startup")

        context = self.factory(self.app)
        try:
            state = await context.__aenter__()
        except BaseException:
            # Reported to the server as a protocol message rather than raised.
            # A server that asked for lifespan is waiting on a reply, and
            # raising here would leave it waiting forever.
            await send(
                {
                    "type": "lifespan.startup.failed",
                    "message": traceback.format_exc(),
                }
            )
            return

        if isinstance(state, Mapping):
            # The server owns this dict and copies it into every request
            # scope, so it must be updated in place rather than replaced.
            scope.setdefault("state", {}).update(state)

        await send({"type": "lifespan.startup.complete"})

        await self._expect(receive, "lifespan.shutdown")

        try:
            await context.__aexit__(None, None, None)
        except BaseException:
            await send(
                {
                    "type": "lifespan.shutdown.failed",
                    "message": traceback.format_exc(),
                }
            )
            return

        await send({"type": "lifespan.shutdown.complete"})

    @staticmethod
    async def _expect(receive: Receive, expected: str) -> Message:
        message = await receive()
        if message["type"] != expected:
            raise RuntimeError(
                f"expected ASGI message {expected!r}, got {message['type']!r}"
            )
        return message
