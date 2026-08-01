"""Shared ASGI type contract used by both sonix.server and sonix.app.

No logic lives here, only the aliases both layers depend on, so that
depending on this module never means depending on the other layer.
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable, MutableMapping

Scope = MutableMapping[str, Any]
Message = MutableMapping[str, Any]

Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]

ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]
