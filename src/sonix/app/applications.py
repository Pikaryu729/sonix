"""Sonix: the app class and @app.get/@app.post/... decorators.

No DI yet (that is a later milestone) -- a registered handler always takes
exactly one argument, a Request, mirroring the calling convention frameworks
like Starlette started with before adding signature-based dependency
injection on top. A sync handler runs via asyncio.to_thread so it never
blocks the event loop; that behavior is independent of DI.

Middleware composition lives in app/middleware.py; this module owns the
registration lists and decides when the stack gets built -- once, on the
first request, because it cannot be assembled until every route and
middleware is registered.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from typing import Any

from sonix.app.middleware import (
    ExceptionHandler,
    HandlerKey,
    Middleware,
    build_stack,
)
from sonix.app.requests import Request
from sonix.app.responses import JSONResponse, PlainTextResponse, Response
from sonix.app.routing import Router
from sonix.types import ASGIApp, Receive, Scope, Send

# Any is broad enough to cover a sync return value or an Awaitable one --
# a separate Awaitable[Any] arm would be redundant.
Handler = Callable[[Request], Any]


def _is_coroutine_callable(handler: Handler) -> bool:
    # inspect.iscoroutinefunction(handler) alone misses a callable *object*
    # with an async __call__ (a class-based view, a decorator that wraps a
    # plain function in one, etc.) -- it returns False for those, which
    # would route them into asyncio.to_thread(handler, request) below.
    # That calls handler(request) in a worker thread, which for an async
    # __call__ only constructs a coroutine and never runs it -- the
    # coroutine then leaks (unawaited) and _coerce_response chokes trying
    # to JSON-serialize it. Checking __call__ too covers that case.
    return inspect.iscoroutinefunction(handler) or inspect.iscoroutinefunction(
        getattr(handler, "__call__", None)  # noqa: B004 -- not a callable() check
    )


def _coerce_response(result: Any) -> Response:
    if isinstance(result, Response):
        return result
    if result is None:
        return Response(status_code=204)
    if isinstance(result, str):
        return PlainTextResponse(result)
    if isinstance(result, bytes):
        return Response(result)
    # dict / list / dataclass / any other json.dumps-able primitive.
    return JSONResponse(result)


def _endpoint(handler: Handler) -> ASGIApp:
    is_coroutine = _is_coroutine_callable(handler)

    async def endpoint(scope: Scope, receive: Receive, send: Send) -> None:
        request = Request(scope, receive)
        if is_coroutine:
            result = await handler(request)
        else:
            result = await asyncio.to_thread(handler, request)
        response = _coerce_response(result)
        await response(scope, receive, send)

    return endpoint


class Sonix:
    def __init__(
        self,
        *,
        debug: bool = False,
        middleware: list[Middleware] | None = None,
        exception_handlers: dict[HandlerKey, ExceptionHandler] | None = None,
    ) -> None:
        self.router = Router()
        self.debug = debug
        self._middleware: list[Middleware] = list(middleware or [])
        self._exception_handlers: dict[HandlerKey, ExceptionHandler] = dict(
            exception_handlers or {}
        )
        self._stack: ASGIApp | None = None

    # -- middleware and exception handlers -----------------------------------

    def add_middleware(self, cls: Callable[..., ASGIApp], **options: Any) -> None:
        """Register a middleware class. First registered ends up outermost."""
        self._assert_not_started("add_middleware")
        self._middleware.append(Middleware(cls, options))

    def add_exception_handler(self, key: HandlerKey, handler: ExceptionHandler) -> None:
        self._assert_not_started("add_exception_handler")
        self._exception_handlers[key] = handler

    def exception_handler(
        self, key: HandlerKey
    ) -> Callable[[ExceptionHandler], ExceptionHandler]:
        def decorator(handler: ExceptionHandler) -> ExceptionHandler:
            self.add_exception_handler(key, handler)
            return handler

        return decorator

    def _assert_not_started(self, what: str) -> None:
        # The stack is built once, on the first request. Mutating the
        # registration lists afterwards would silently have no effect, which
        # is worse than refusing.
        if self._stack is not None:
            raise RuntimeError(
                f"{what}() cannot be called after the app has started serving"
            )

    def build_middleware_stack(self) -> ASGIApp:
        return build_stack(
            self.router,
            self._middleware,
            handlers=self._exception_handlers,
            debug=self.debug,
        )

    def route(
        self, path: str, methods: list[str] | None = None
    ) -> Callable[[Handler], Handler]:
        def decorator(handler: Handler) -> Handler:
            self.add_route(path, handler, methods=methods)
            return handler

        return decorator

    def add_route(
        self, path: str, handler: Handler, methods: list[str] | None = None
    ) -> None:
        self.router.add_route(path, _endpoint(handler), methods=methods)

    def get(self, path: str) -> Callable[[Handler], Handler]:
        return self.route(path, methods=["GET"])

    def post(self, path: str) -> Callable[[Handler], Handler]:
        return self.route(path, methods=["POST"])

    def put(self, path: str) -> Callable[[Handler], Handler]:
        return self.route(path, methods=["PUT"])

    def patch(self, path: str) -> Callable[[Handler], Handler]:
        return self.route(path, methods=["PATCH"])

    def delete(self, path: str) -> Callable[[Handler], Handler]:
        return self.route(path, methods=["DELETE"])

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        scope_type = scope["type"]
        if scope_type not in ("http", "websocket"):
            # Previously this method delegated to the router unconditionally,
            # so a lifespan scope reached Router.__call__ and died on
            # scope["path"] with a bare KeyError. Lifespan support lands in
            # app/lifespan.py; until then, fail with something that names the
            # actual problem.
            raise RuntimeError(f"unsupported ASGI scope type {scope_type!r}")

        if self._stack is None:
            self._stack = self.build_middleware_stack()
        await self._stack(scope, receive, send)
