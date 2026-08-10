"""The WebSocket a handler sees -- built purely from (scope, receive, send).

**This module may not import sonix.server, and specifically not
sonix.server.websockets.** No opcode, mask byte, frame boundary or
fragmentation rule appears anywhere below: the vocabulary here is entirely
ASGI ``websocket.*`` messages, which is why the same handler runs unchanged
on uvicorn. That constraint is the most concrete payoff of the two-layer
split, and ``tests/test_layering.py`` walks this file's AST to enforce it.

Close *codes* do appear, since they are part of the ASGI message contract
(``websocket.close`` carries one, ``websocket.disconnect`` reports one) --
not part of the frame format.
"""

from __future__ import annotations

import enum
import json
from collections.abc import AsyncIterator
from typing import Any

from sonix.app.requests import HTTPConnection
from sonix.types import Message, Receive, Scope, Send

# The three codes this module names. The full registry lives in the server
# layer's codec, where the wire format that validates them lives too.
CLOSE_NORMAL = 1000
CLOSE_POLICY_VIOLATION = 1008
CLOSE_INTERNAL_ERROR = 1011


class WebSocketState(enum.Enum):
    CONNECTING = enum.auto()
    CONNECTED = enum.auto()
    DISCONNECTED = enum.auto()


class WebSocketDisconnect(Exception):
    """The peer went away. The websocket analogue of ClientDisconnect.

    Raised by the receive helpers rather than returned, so an echo loop is
    ``while True: await ws.send_text(await ws.receive_text())`` and ends by
    exception instead of by a sentinel check on every iteration.
    """

    def __init__(self, code: int = CLOSE_NORMAL, reason: str = "") -> None:
        super().__init__(f"{code}: {reason}" if reason else str(code))
        self.code = code
        self.reason = reason


class WebSocket(HTTPConnection):
    def __init__(self, scope: Scope, receive: Receive, send: Send) -> None:
        super().__init__(scope)
        self._receive = receive
        self._send = send
        self.client_state = WebSocketState.CONNECTING
        self.application_state = WebSocketState.CONNECTING

    @property
    def subprotocols(self) -> list[str]:
        """Subprotocols the client offered, in its order of preference."""
        return self._scope.get("subprotocols", [])

    # -- handshake ----------------------------------------------------------

    async def accept(
        self,
        subprotocol: str | None = None,
        headers: list[tuple[bytes, bytes]] | None = None,
    ) -> None:
        if self.client_state is WebSocketState.CONNECTING:
            # ASGI requires the application to consume websocket.connect
            # before answering it. Doing it here rather than making every
            # handler write `await ws.receive()` first is the whole reason
            # this class exists.
            await self.receive()
        message: Message = {"type": "websocket.accept", "subprotocol": subprotocol}
        if headers is not None:
            message["headers"] = headers
        await self._send(message)
        self.application_state = WebSocketState.CONNECTED

    async def close(self, code: int = CLOSE_NORMAL, reason: str = "") -> None:
        """Close the connection. Idempotent.

        Idempotence is not politeness: ``finally: await ws.close()`` is the
        standard shape for a websocket handler, and it must not raise because
        the peer closed first.
        """
        if self.application_state is WebSocketState.DISCONNECTED:
            return
        self.application_state = WebSocketState.DISCONNECTED
        await self._send({"type": "websocket.close", "code": code, "reason": reason})

    # -- receiving ----------------------------------------------------------

    async def receive(self) -> Message:
        """The next raw ASGI message, disconnect included."""
        if self.client_state is WebSocketState.DISCONNECTED:
            raise RuntimeError(
                "cannot receive on a websocket that has already disconnected"
            )
        message = await self._receive()
        message_type = message["type"]
        if message_type == "websocket.connect":
            self.client_state = WebSocketState.CONNECTED
        elif message_type == "websocket.disconnect":
            self.client_state = WebSocketState.DISCONNECTED
            self.application_state = WebSocketState.DISCONNECTED
        return message

    async def _receive_data(self) -> Message:
        if self.application_state is not WebSocketState.CONNECTED:
            raise RuntimeError("accept() must be awaited before receiving messages")
        message = await self.receive()
        if message["type"] == "websocket.disconnect":
            raise WebSocketDisconnect(
                message.get("code", CLOSE_NORMAL), message.get("reason", "")
            )
        return message

    async def receive_text(self) -> str:
        message = await self._receive_data()
        text = message.get("text")
        if text is None:
            # A type mismatch is a bug in one of the two peers, not a reason
            # to close: the caller asked for the wrong thing, or the client
            # sent the wrong thing. Either way, say which.
            raise RuntimeError("expected a text message, got a binary one")
        return text

    async def receive_bytes(self) -> bytes:
        message = await self._receive_data()
        data = message.get("bytes")
        if data is None:
            raise RuntimeError("expected a binary message, got a text one")
        return data

    async def receive_json(self) -> Any:
        return json.loads(await self.receive_text())

    async def iter_text(self) -> AsyncIterator[str]:
        """Yield text messages until the peer disconnects."""
        try:
            while True:
                yield await self.receive_text()
        except WebSocketDisconnect:
            return

    async def iter_bytes(self) -> AsyncIterator[bytes]:
        try:
            while True:
                yield await self.receive_bytes()
        except WebSocketDisconnect:
            return

    # -- sending ------------------------------------------------------------

    async def _send_data(self, message: Message) -> None:
        if self.application_state is not WebSocketState.CONNECTED:
            raise RuntimeError("accept() must be awaited before sending messages")
        await self._send(message)

    async def send_text(self, data: str) -> None:
        await self._send_data({"type": "websocket.send", "text": data})

    async def send_bytes(self, data: bytes) -> None:
        await self._send_data({"type": "websocket.send", "bytes": data})

    async def send_json(self, data: Any) -> None:
        await self.send_text(json.dumps(data))
