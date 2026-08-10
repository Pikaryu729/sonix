"""Shared test helpers.

These factories were originally duplicated verbatim across four test modules.
They live in a plain importable module rather than in conftest.py because they
are factories, not fixtures -- an explicit ``from helpers import make_scope``
reads better at the call site than injecting a factory-returning fixture into
every test method. ``pythonpath = ["tests"]`` in pyproject.toml puts this
module on sys.path for the whole suite.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

# Typing the helpers against the real aliases (rather than dict[str, Any])
# keeps them assignable where an ASGI app expects Receive/Send: Message is a
# MutableMapping, and parameter types are contravariant, so a send() declared
# to take a dict would not satisfy Send.
from sonix.types import Message, Receive, Scope, Send

# A scope that a conforming ASGI server would build for GET / -- the same
# shape server/protocol.py's build_scope() emits, so app-layer tests exercise
# the real contract without a socket anywhere in sight.
_BASE_SCOPE: dict[str, Any] = {
    "type": "http",
    "asgi": {"version": "3.0", "spec_version": "2.3"},
    "http_version": "1.1",
    "method": "GET",
    "scheme": "http",
    "path": "/",
    "raw_path": b"/",
    "query_string": b"",
    "root_path": "",
    "headers": [(b"host", b"example.com")],
    "client": ("127.0.0.1", 54321),
    "server": ("127.0.0.1", 8000),
}


# The websocket equivalent. Deliberately not make_scope(type="websocket"):
# that would leave "method" in place, and a websocket scope having no method
# is exactly the difference that breaks code assuming there is one.
_BASE_WS_SCOPE: dict[str, Any] = {
    "type": "websocket",
    "asgi": {"version": "3.0", "spec_version": "2.3"},
    "http_version": "1.1",
    "scheme": "ws",
    "path": "/ws",
    "raw_path": b"/ws",
    "query_string": b"",
    "root_path": "",
    "headers": [(b"host", b"example.com")],
    "client": ("127.0.0.1", 54321),
    "server": ("127.0.0.1", 8000),
    "subprotocols": [],
}


def make_scope(**overrides: Any) -> Scope:
    """Build an HTTP scope, overriding any key."""
    scope = dict(_BASE_SCOPE)
    scope.update(overrides)
    return scope


def make_ws_scope(**overrides: Any) -> Scope:
    """Build a websocket scope, overriding any key."""
    scope = dict(_BASE_WS_SCOPE)
    scope.update(overrides)
    return scope


async def fake_receive() -> Message:
    """A receive() that fails loudly if a test's code path ever calls it."""
    raise AssertionError("receive() should not be called in this test")


def make_receive(messages: Iterable[Message]) -> Receive:
    """A receive() that yields ``messages`` in order."""
    it = iter(messages)

    async def receive() -> Message:
        return next(it)

    return receive


def make_send() -> tuple[Send, list[Message]]:
    """A send() plus the list it records every message into."""
    sent: list[Message] = []

    async def send(message: Message) -> None:
        sent.append(message)

    return send, sent
