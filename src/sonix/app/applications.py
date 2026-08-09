"""Sonix: the app class and @app.get/@app.post/... decorators.

First pass, no DI or middleware yet (those are later milestones) -- a
registered handler always takes exactly one argument, a Request, mirroring
the calling convention frameworks like Starlette started with before
adding signature-based dependency injection on top. A sync handler runs
via asyncio.to_thread so it never blocks the event loop; this part of the
"await handler(...) directly if async, else asyncio.to_thread" behavior
is independent of DI and safe to build now.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from typing import Any

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
    def __init__(self) -> None:
        self.router = Router()

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
        await self.router(scope, receive, send)
