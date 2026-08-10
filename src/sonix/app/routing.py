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

from sonix.app.exceptions import HTTPException
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
    di_plan: Any | None = None


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
        self._routes.append(Route(pattern, converters, route_methods, handler, None))

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        path = scope["path"]
        method = scope["method"]

        allowed: dict[str, None] = {}  # insertion-ordered set
        for route in self._routes:
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
