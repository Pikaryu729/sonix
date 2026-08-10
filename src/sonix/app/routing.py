"""Router/Route: path template compiling, param coercion, 404/405.

Router is an ASGI app in its own right -- it only ever touches
scope/receive/send, matching app/requests.py and app/responses.py's
runtime-agnostic design. Handlers registered here are called as raw ASGI
apps (await handler(scope, receive, send)); wrapping a plain Python
function's signature into that shape is applications.py/di.py's job in a
later milestone, not routing.py's.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from sonix.app.exceptions import HTTPException, WebSocketException
from sonix.types import ASGIApp, Receive, Scope, Send


@dataclass(frozen=True, slots=True)
class Converter:
    regex: str
    convert: Callable[[str], Any]


CONVERTERS: dict[str, Converter] = {
    "str": Converter(regex=r"[^/]+", convert=str),
    "int": Converter(regex=r"[0-9]+", convert=int),
    "float": Converter(regex=r"[0-9]+(?:\.[0-9]+)?", convert=float),
    "uuid": Converter(
        regex=r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}",
        convert=uuid.UUID,
    ),
    "path": Converter(regex=r".+", convert=str),
}


class RoutePathError(ValueError):
    """Raised at route-registration time for a malformed path template --
    unknown converter, duplicate parameter name, or a non-final `path`
    segment. Never raised at match/request time -- by the time a Route
    exists, its template has already been validated.
    """


_PARAM_RE = re.compile(
    r"{(?P<name>[a-zA-Z_][a-zA-Z0-9_]*)(?::(?P<type>[a-zA-Z_][a-zA-Z0-9_]*))?}"
)


def compile_path(path: str) -> tuple[re.Pattern[str], dict[str, Converter]]:
    """Compile a template like '/items/{id:int}' into a regex (no anchors --
    callers must use .fullmatch()) with one named group per path parameter,
    plus a name -> Converter mapping used to coerce matched values.

    Literal segments (including a trailing '/') are re.escape()'d verbatim,
    which is what makes '/items' and '/items/' compile to two genuinely
    different patterns -- trailing-slash strictness falls out of this
    naturally, with no special-casing needed.
    """
    parts: list[str] = []
    converters: dict[str, Converter] = {}
    seen: set[str] = set()
    last_end = 0

    for match in _PARAM_RE.finditer(path):
        parts.append(re.escape(path[last_end : match.start()]))

        name = match.group("name")
        type_name = match.group("type") or "str"

        if name in seen:
            raise RoutePathError(
                f"duplicate path parameter {name!r} in template {path!r}"
            )
        seen.add(name)

        try:
            converter = CONVERTERS[type_name]
        except KeyError:
            raise RoutePathError(
                f"unknown path converter {type_name!r} for parameter "
                f"{name!r} in template {path!r}; available converters: "
                f"{sorted(CONVERTERS)}"
            ) from None

        if type_name == "path" and match.end() != len(path):
            raise RoutePathError(
                f"'path' converter for {name!r} in template {path!r} must "
                "be the final path segment"
            )

        converters[name] = converter
        parts.append(f"(?P<{name}>{converter.regex})")
        last_end = match.end()

    parts.append(re.escape(path[last_end:]))
    return re.compile("".join(parts)), converters


@dataclass(frozen=True, slots=True)
class Route:
    pattern: re.Pattern[str]
    converters: dict[str, Converter]
    methods: tuple[str, ...]
    handler: ASGIApp
    # "http" or "websocket". A field rather than overloading methods=None
    # (which add_route already reads as "default to GET") or keeping a second
    # route list (which would split the registration-order, first-match-wins
    # invariant across two orderings). Websocket routes carry methods=().
    scope_type: str = "http"
    # No di_plan field. docs/architecture.md originally put the DI plan here,
    # but by the time a Route exists the handler has already been wrapped into
    # an ASGIApp that closes over its own plan -- which is where the signature
    # was known in the first place. A field routing never reads is worse than
    # no field.


class Router:
    """Routes are meant to be registered once, before the app starts
    serving requests -- add_route mutates a plain list with no locking,
    which is safe under that pattern (the same one Starlette/FastAPI use)
    but not under concurrent registration during live traffic.
    """

    def __init__(self) -> None:
        self._routes: list[Route] = []

    def add_route(
        self,
        path: str,
        handler: ASGIApp,
        methods: list[str] | None = None,
    ) -> None:
        pattern, converters = compile_path(path)
        # methods=None defaults to GET; methods=[] is distinct from that
        # and is almost certainly a caller bug (a route with zero allowed
        # methods can never be dispatched to) rather than "use the
        # default" -- silently falling back to GET for it would hide that.
        if methods is not None and not methods:
            raise ValueError(f"add_route({path!r}, ...) needs at least one HTTP method")
        route_methods = tuple(
            m.upper() for m in (methods if methods is not None else ["GET"])
        )
        self._routes.append(Route(pattern, converters, route_methods, handler))

    def add_websocket_route(self, path: str, handler: ASGIApp) -> None:
        """Register a websocket route. Shares the path space with HTTP routes.

        The same path may carry both an HTTP and a websocket route without
        colliding, since matching considers scope type as well as path.
        """
        pattern, converters = compile_path(path)
        self._routes.append(Route(pattern, converters, (), handler, "websocket"))

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        # Before scope["method"], which a websocket scope does not have.
        if scope["type"] == "websocket":
            await self._dispatch_websocket(scope, receive, send)
            return

        path = scope["path"]
        method = scope["method"]

        allowed: dict[str, None] = {}  # insertion-ordered set
        for route in self._routes:
            # Explicit rather than emergent. Iterating a websocket route's
            # empty methods tuple would also add nothing to `allowed`, so an
            # HTTP request to a websocket-only path would 404 either way --
            # but relying on that is one refactor away from a 405 carrying an
            # empty Allow header.
            if route.scope_type != "http":
                continue
            match = route.pattern.fullmatch(path)
            if match is None:
                continue

            # HTTP method tokens are case-sensitive on the wire (RFC 9110)
            # and server/parser.py never normalizes them, so this
            # comparison is intentionally exact -- no .upper() on `method`.
            if method not in route.methods:
                for m in route.methods:
                    allowed.setdefault(m, None)
                continue

            scope["path_params"] = {
                name: route.converters[name].convert(value)
                for name, value in match.groupdict().items()
            }
            await route.handler(scope, receive, send)
            return

        # Raised rather than returned, so that a custom 404 or 405 is a matter
        # of registering an exception handler. Constructing the response here
        # made these two statuses the only ones in the framework with no
        # override hook. It does mean Router is not a complete ASGI app on its
        # own -- it expects the ExceptionMiddleware that Sonix always wraps it
        # in.
        if allowed:
            raise HTTPException(405, headers={"allow": ", ".join(allowed)})
        raise HTTPException(404)

    async def _dispatch_websocket(
        self, scope: Scope, receive: Receive, send: Send
    ) -> None:
        path = scope["path"]
        for route in self._routes:
            if route.scope_type != "websocket":
                continue
            match = route.pattern.fullmatch(path)
            if match is None:
                continue
            scope["path_params"] = {
                name: route.converters[name].convert(value)
                for name, value in match.groupdict().items()
            }
            await route.handler(scope, receive, send)
            return

        # Not HTTPException(404): a websocket has no status line to put a 404
        # on. The only refusal a websocket scope understands is a close, so
        # "no such websocket path" is deliberately not observable as a 404 --
        # before accept it reaches the client as the HTTP 403 the ASGI spec
        # mandates for a denied handshake.
        raise WebSocketException(1000, f"no websocket route for {path!r}")
