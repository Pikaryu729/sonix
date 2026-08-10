"""Request object -- built purely from (scope, receive), no server/ dependency.

Proves layer 2's runtime-agnosticism: this class only ever touches
sonix.types.Scope/Receive, never sonix.server internals.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from functools import cached_property
from typing import Any
from urllib.parse import parse_qsl

from sonix.types import Receive, Scope


class ClientDisconnect(Exception):
    """Raised by Request.stream()/body() if the client disconnects before
    the body finishes streaming.

    Deliberately not HTTPException -- app/exceptions.py doesn't exist until
    a later milestone. A future ExceptionMiddleware can special-case this
    into a response without requests.py knowing about HTTP status codes.
    """


class Headers:
    """Case-insensitive, read-only view over scope["headers"].

    Header names are already lowercased by server/parser.py by the time a
    real scope reaches here, but lookup must not depend on that upstream
    guarantee to stay correct against a hand-built test scope (or a future
    non-Sonix ASGI server) -- so both the stored keys and the lookup key
    are always lowercased here, independent of what arrived.
    """

    __slots__ = ("_by_name",)

    def __init__(self, raw: list[tuple[bytes, bytes]]) -> None:
        by_name: dict[bytes, bytes] = {}
        for name, value in raw:
            key = name.lower()
            if key not in by_name:  # first occurrence wins for duplicates
                by_name[key] = value
        self._by_name = by_name

    def get(self, name: str, default: str | None = None) -> str | None:
        raw = self._by_name.get(name.lower().encode("latin-1"))
        return default if raw is None else raw.decode("latin-1")

    def __getitem__(self, name: str) -> str:
        value = self.get(name)
        if value is None:
            raise KeyError(name)
        return value

    def __contains__(self, name: str) -> bool:
        return self.get(name) is not None

    def __iter__(self):
        # Keys are stored as bytes internally, but every other method on
        # this class takes/returns str -- yielding raw bytes here would
        # break the natural `for name in headers: headers.get(name)`
        # idiom, since bytes has no .encode() for get()'s lookup to use.
        return (key.decode("latin-1") for key in self._by_name)

    def __len__(self) -> int:
        return len(self._by_name)


class QueryParams:
    """Read-only view over scope["query_string"].

    query_string bytes are legitimately percent-encoded on the wire (unlike
    `path`, which server/parser.py never decodes -- a known, separate gap).
    parse_qsl's default unquoting is therefore correct here, not a
    workaround for that gap.

    keep_blank_values=True so `?flag=` yields "" rather than dropping the
    key -- silently discarding an explicitly-present-but-empty param would
    be surprising.
    """

    __slots__ = ("_by_name",)

    def __init__(self, query_string: bytes) -> None:
        pairs = parse_qsl(query_string.decode("latin-1"), keep_blank_values=True)
        by_name: dict[str, list[str]] = {}
        for key, value in pairs:
            by_name.setdefault(key, []).append(value)
        self._by_name = by_name

    def get(self, name: str, default: str | None = None) -> str | None:
        values = self._by_name.get(name)
        return default if not values else values[0]

    def get_list(self, name: str) -> list[str]:
        return list(self._by_name.get(name, []))

    def __getitem__(self, name: str) -> str:
        value = self.get(name)
        if value is None:
            raise KeyError(name)
        return value

    def __contains__(self, name: str) -> bool:
        return name in self._by_name

    def __iter__(self):
        return iter(self._by_name)

    def __len__(self) -> int:
        return len(self._by_name)


class Request:
    def __init__(self, scope: Scope, receive: Receive) -> None:
        self._scope = scope
        self._receive = receive
        self._body: bytes | None = None
        self._stream_consumed = False

    @property
    def scope(self) -> Scope:
        return self._scope

    @property
    def method(self) -> str:
        return self._scope["method"]

    @property
    def path(self) -> str:
        # NOT percent-decoded -- inherited as-is from server/parser.py's
        # target parsing, which never decodes. Documented gap, not fixed
        # here.
        return self._scope["path"]

    @property
    def raw_path(self) -> bytes:
        return self._scope["raw_path"]

    @property
    def query_string(self) -> bytes:
        return self._scope["query_string"]

    @property
    def http_version(self) -> str:
        return self._scope["http_version"]

    @property
    def scheme(self) -> str:
        return self._scope["scheme"]

    @property
    def root_path(self) -> str:
        return self._scope["root_path"]

    @property
    def client(self) -> tuple[str, int] | None:
        return self._scope["client"]

    @property
    def server(self) -> tuple[str, int] | None:
        return self._scope["server"]

    @property
    def path_params(self) -> dict[str, Any]:
        # Written by app/routing.py's Router during matching -- absent
        # (rather than an empty dict already in scope) until a route with
        # at least one {param} has matched, so this defaults to {} rather
        # than a plain __getitem__ lookup.
        return self._scope.get("path_params", {})

    @property
    def state(self) -> dict[str, Any]:
        # A shallow copy of whatever the lifespan yielded, put here by the
        # server. Per-request, so a handler may stash values on it without
        # leaking them into the next request; the objects the lifespan opened
        # stay shared. Empty when running without a lifespan, so reading it is
        # always safe.
        return self._scope.get("state", {})

    @cached_property
    def headers(self) -> Headers:
        return Headers(self._scope["headers"])

    @cached_property
    def query_params(self) -> QueryParams:
        return QueryParams(self._scope["query_string"])

    async def stream(self) -> AsyncIterator[bytes]:
        if self._body is not None:
            # body() already drained and cached everything; replay it as a
            # single chunk instead of touching receive() again, which has
            # nothing left to give and would hang. Match the live path's
            # contract of never yielding an empty chunk (relevant for a
            # body-less request, e.g. a GET).
            if self._body:
                yield self._body
            return
        if self._stream_consumed:
            raise RuntimeError("Request.stream() was already consumed")
        self._stream_consumed = True
        while True:
            message = await self._receive()
            # protocol.py's receive() only ever produces these two message
            # types -- the disconnect check must happen before touching
            # message["body"], since a disconnect message carries no
            # "body" key at all.
            if message.get("type") == "http.disconnect":
                raise ClientDisconnect("client disconnected while reading request body")
            chunk = message.get("body", b"")
            if chunk:
                yield chunk
            if not message.get("more_body", False):
                break

    async def body(self) -> bytes:
        if self._body is None:
            chunks = [chunk async for chunk in self.stream()]
            self._body = b"".join(chunks)
        return self._body

    async def json(self) -> Any:
        return json.loads(await self.body())
