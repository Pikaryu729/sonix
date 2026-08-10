"""Middleware composition, and the exception layer built on it.

Middleware is ASGI-onion wrapping: ``middleware(app) -> new_app``, where the
new app does pre-work, awaits the inner app (optionally wrapping receive/send
to observe or transform messages), then does post-work. Deliberately not a
before/after hook list -- a hook list cannot express streaming interception of
a response body that arrives as several http.response.body events, and onion
wrapping is a composition model the server bridge and the router already use
rather than a second one to learn.

    class SomeMiddleware:
        def __init__(self, app, **options):
            self.app = app

        async def __call__(self, scope, receive, send): ...
"""

from __future__ import annotations

import traceback
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from sonix.app.exceptions import HTTPException, ValidationError
from sonix.app.requests import ClientDisconnect, Request
from sonix.app.responses import JSONResponse, PlainTextResponse, Response
from sonix.types import ASGIApp, Message, Receive, Scope, Send

ExceptionHandler = Callable[[Request, Exception], Awaitable[Response]]

# A handler may be registered against an exception class or, for
# HTTPException specifically, against a status code -- so that "give me a
# custom 404 page" doesn't require subclassing anything.
HandlerKey = type[Exception] | int


@dataclass(frozen=True, slots=True)
class Middleware:
    """A middleware class plus the options to construct it with.

    Deferred construction matters: the stack cannot be built until every
    route and middleware is registered, so what gets stored at registration
    time is the recipe, not the instance.
    """

    cls: Callable[..., ASGIApp]
    options: dict[str, Any] = field(default_factory=dict)

    def build(self, app: ASGIApp) -> ASGIApp:
        return self.cls(app, **self.options)


async def http_exception_handler(request: Request, exc: Exception) -> Response:
    assert isinstance(exc, HTTPException)
    return PlainTextResponse(
        exc.detail,
        status_code=exc.status_code,
        headers=exc.headers,
    )


async def validation_error_handler(request: Request, exc: Exception) -> Response:
    assert isinstance(exc, ValidationError)
    # JSON rather than plain text: this is the one error a client is expected
    # to act on programmatically, and it carries a list rather than a sentence.
    return JSONResponse({"detail": exc.errors}, status_code=exc.status_code)


async def server_error_handler(request: Request, exc: Exception) -> Response:
    # Deliberately says nothing about the underlying exception: the traceback
    # goes to the log, not to the client.
    return PlainTextResponse("Internal Server Error", status_code=500)


class ExceptionMiddleware:
    """Turns exceptions into responses.

    Placed outermost in the stack so it also catches failures raised by other
    middleware, not only by handlers. Before it existed, an unhandled handler
    exception propagated to the protocol layer, which closed the transport --
    the client saw a dropped connection rather than a 500.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        handlers: dict[HandlerKey, ExceptionHandler] | None = None,
        debug: bool = False,
    ) -> None:
        self.app = app
        self.debug = debug
        self._handlers: dict[HandlerKey, ExceptionHandler] = {
            # ValidationError is an HTTPException subclass; _lookup walks the
            # MRO, so this more specific entry wins over the generic one.
            ValidationError: validation_error_handler,
            HTTPException: http_exception_handler,
            Exception: server_error_handler,
        }
        if handlers:
            self._handlers.update(handlers)

    def _lookup(self, exc: Exception) -> ExceptionHandler | None:
        # Status code first, so a handler registered for 404 beats the generic
        # HTTPException handler.
        if isinstance(exc, HTTPException):
            by_status = self._handlers.get(exc.status_code)
            if by_status is not None:
                return by_status
        for cls in type(exc).__mro__:
            handler = self._handlers.get(cls)
            if handler is not None:
                return handler
        return None

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        response_started = False

        async def wrapped_send(message: Message) -> None:
            nonlocal response_started
            if message["type"] == "http.response.start":
                response_started = True
            await send(message)

        try:
            await self.app(scope, receive, wrapped_send)
        except ClientDisconnect:
            # There is nobody left to answer. Not an error, and emphatically
            # not a 500 -- writing a response to a closed transport would be
            # the only thing that could go wrong from here.
            return
        except Exception as exc:
            handler = self._lookup(exc)
            if handler is None or (self.debug and not isinstance(exc, HTTPException)):
                raise

            if response_started:
                # The status line and headers are already on the wire, so
                # there is no way to turn this into an error response -- the
                # only honest option is to let it propagate and have the
                # server close the connection. Emitting a second
                # http.response.start here would corrupt the stream.
                raise

            if not isinstance(exc, HTTPException):
                traceback.print_exc()

            response = await handler(Request(scope, receive), exc)
            await response(scope, receive, send)


def build_stack(
    router: ASGIApp,
    middleware: list[Middleware],
    *,
    handlers: dict[HandlerKey, ExceptionHandler] | None = None,
    debug: bool = False,
) -> ASGIApp:
    """Wrap ``router`` in ``middleware``, with the exception layer outermost.

    Registration order is outside-in: the first-registered middleware ends up
    outermost, so it sees a request first and a response last. Note this
    applies uniformly to both the constructor list and successive
    add_middleware() calls -- Starlette's add_middleware is LIFO while its
    constructor list is not, and one consistent rule is easier to reason about
    than matching that split.
    """
    app = router
    for spec in reversed(middleware):
        app = spec.build(app)
    return ExceptionMiddleware(app, handlers=handlers, debug=debug)
