"""A minimal RFC 6455 client, built on the server's own codec.

Legitimate in tests, and it exercises the codec's client side for free
rather than duplicating a decoder that could share its bugs.

Note the two directional settings, which are the whole reason the codec
takes them as parameters: masking in RFC 6455 is a *client* obligation, so
this masks everything it sends and configures its parser with
``require_mask=False``. A client that forgot the second would reject every
frame the server sends.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
from typing import Any

from sonix.server.websockets import (
    BinaryMessage,
    CloseCode,
    CloseReceived,
    FrameEvent,
    FrameParser,
    Opcode,
    Ping,
    Pong,
    TextMessage,
    accept_key,
    client_key,
    encode_close_frame,
    encode_frame,
)


class HandshakeRejected(Exception):
    """The server answered the upgrade with an ordinary HTTP response."""

    def __init__(self, status: int, raw: bytes) -> None:
        super().__init__(f"handshake rejected with HTTP {status}")
        self.status = status
        self.raw = raw


class WSTestClient:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self._reader = reader
        self._writer = writer
        self._parser = FrameParser(require_mask=False)
        self._pending: list[FrameEvent] = []

    @classmethod
    async def connect(
        cls,
        host: str,
        port: int,
        path: str = "/ws",
        *,
        headers: list[tuple[str, str]] | None = None,
        subprotocols: list[str] | None = None,
    ) -> WSTestClient:
        reader, writer = await asyncio.open_connection(host, port)
        key = client_key()
        lines = [
            f"GET {path} HTTP/1.1",
            f"Host: {host}:{port}",
            "Upgrade: websocket",
            "Connection: Upgrade",
            f"Sec-WebSocket-Key: {key.decode()}",
            "Sec-WebSocket-Version: 13",
        ]
        if subprotocols:
            lines.append(f"Sec-WebSocket-Protocol: {', '.join(subprotocols)}")
        lines.extend(f"{name}: {value}" for name, value in headers or [])
        writer.write(("\r\n".join(lines) + "\r\n\r\n").encode())
        await writer.drain()

        head = await reader.readuntil(b"\r\n\r\n")
        status = int(head.split(b" ", 2)[1])
        if status != 101:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
            raise HandshakeRejected(status, head)
        assert accept_key(key) in head, "Sec-WebSocket-Accept did not match"
        return cls(reader, writer)

    # -- sending ------------------------------------------------------------

    async def send_frame(
        self, opcode: Opcode, payload: bytes = b"", *, fin: bool = True
    ) -> None:
        self._writer.write(encode_frame(opcode, payload, fin=fin, mask=os.urandom(4)))
        await self._writer.drain()

    async def send_text(self, text: str) -> None:
        await self.send_frame(Opcode.TEXT, text.encode("utf-8"))

    async def send_bytes(self, data: bytes) -> None:
        await self.send_frame(Opcode.BINARY, data)

    async def send_json(self, value: Any) -> None:
        await self.send_text(json.dumps(value))

    async def send_raw(self, data: bytes) -> None:
        """Write bytes verbatim -- for deliberately malformed frames."""
        self._writer.write(data)
        await self._writer.drain()

    async def close(self, code: int = CloseCode.NORMAL, reason: str = "") -> None:
        self._writer.write(encode_close_frame(code, reason, mask=os.urandom(4)))
        await self._writer.drain()

    # -- receiving ----------------------------------------------------------

    async def receive(self) -> FrameEvent:
        """The next decoded event, or a CloseReceived(1006) on a dead socket."""
        while not self._pending:
            data = await self._reader.read(65536)
            if not data:
                return CloseReceived(CloseCode.ABNORMAL, "")
            self._pending.extend(self._parser.feed_data(data))
        return self._pending.pop(0)

    async def receive_message(self) -> FrameEvent:
        """Like receive(), but transparent to ping/pong -- as an app is."""
        while True:
            event = await self.receive()
            if not isinstance(event, Ping | Pong):
                return event

    async def receive_text(self) -> str:
        event = await self.receive_message()
        assert isinstance(event, TextMessage), event
        return event.data

    async def receive_bytes(self) -> bytes:
        event = await self.receive_message()
        assert isinstance(event, BinaryMessage), event
        return event.data

    async def receive_json(self) -> Any:
        return json.loads(await self.receive_text())

    async def receive_close(self) -> CloseReceived:
        event = await self.receive_message()
        assert isinstance(event, CloseReceived), event
        return event

    # -- teardown -----------------------------------------------------------

    async def disconnect(self) -> None:
        self._writer.close()
        with contextlib.suppress(Exception):
            await self._writer.wait_closed()

    async def __aenter__(self) -> WSTestClient:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.disconnect()
