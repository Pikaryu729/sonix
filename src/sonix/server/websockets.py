"""WebSocket handshake and frame codec (RFC 6455).

Pure and synchronous, exactly like ``server/parser.py``: no ``asyncio``
import, no I/O, no ASGI vocabulary. Bytes go in, decoded *messages* come
out. Nothing in ``sonix/app`` may import this module -- the application
layer speaks ASGI ``websocket.*`` messages and never sees an opcode, a
mask, or a frame boundary. ``tests/test_layering.py`` enforces that.

Two shapes, chosen for two different reasons:

* :class:`FrameParser` is a class because decoding is stateful across
  calls -- partial frames spanning TCP segments, continuation reassembly,
  per-message size accounting. A free ``decode_frame(bytes)`` would push
  "do I have a whole frame yet?" up into ``protocol.py``, which is the
  same mistake ``HTTP11Parser`` exists to prevent, one layer down.
* ``encode_frame``/``encode_close_frame`` are free functions because
  encoding genuinely is one message in, one byte string out.

The parser emits **messages, not frames**: fragmentation is reassembled
here, so ``protocol.py`` never sees a CONTINUATION opcode and never
decides where a message ends. That is the frame-level analogue of
"protocol.py never re-inspects headers".

Masking is *directional* in RFC 6455 -- a client must mask, a server must
not. That is why ``require_mask`` and ``encode_frame(mask=...)`` exist
rather than being hardcoded: with them the same codec drives a client,
which is what the end-to-end tests use instead of a second decoder that
could share this one's bugs.
"""

from __future__ import annotations

import base64
import enum
import hashlib
import os
import struct
from dataclasses import dataclass

# RFC 6455 section 1.3. Concatenated with the client's Sec-WebSocket-Key
# before hashing, so that a cached HTTP response cannot be replayed as a
# successful handshake.
GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

WS_VERSION = b"13"

DEFAULT_MAX_MESSAGE_SIZE = 16 * 1024 * 1024

# Control frames carry at most 125 bytes, of which a close frame spends two
# on the status code.
MAX_CONTROL_PAYLOAD = 125
MAX_CLOSE_REASON_BYTES = MAX_CONTROL_PAYLOAD - 2


class CloseCode(enum.IntEnum):
    NORMAL = 1000
    GOING_AWAY = 1001
    PROTOCOL_ERROR = 1002
    UNSUPPORTED_DATA = 1003
    NO_STATUS = 1005
    ABNORMAL = 1006
    INVALID_PAYLOAD = 1007
    POLICY_VIOLATION = 1008
    MESSAGE_TOO_BIG = 1009
    INTERNAL_ERROR = 1011


# 1005 and 1006 are "not present on the wire" sentinels: the RFC forbids
# sending them in a close frame. 1004 and 1015 are reserved.
_SENDABLE_CLOSE_CODES = frozenset(range(1000, 1004)) | frozenset(range(1007, 1015))


def _is_valid_close_code(code: int) -> bool:
    if 3000 <= code <= 4999:
        # 3000-3999 registered with IANA, 4000-4999 private use.
        return True
    return code in _SENDABLE_CLOSE_CODES


class Opcode(enum.IntEnum):
    CONTINUATION = 0x0
    TEXT = 0x1
    BINARY = 0x2
    CLOSE = 0x8
    PING = 0x9
    PONG = 0xA


_CONTROL_OPCODES = frozenset({Opcode.CLOSE, Opcode.PING, Opcode.PONG})


@dataclass(frozen=True, slots=True)
class TextMessage:
    data: str


@dataclass(frozen=True, slots=True)
class BinaryMessage:
    data: bytes


@dataclass(frozen=True, slots=True)
class Ping:
    data: bytes


@dataclass(frozen=True, slots=True)
class Pong:
    data: bytes


@dataclass(frozen=True, slots=True)
class CloseReceived:
    code: int
    reason: str


FrameEvent = TextMessage | BinaryMessage | Ping | Pong | CloseReceived


class WebSocketProtocolError(Exception):
    """A frame-stream violation, carrying the close code to answer with.

    ``partial_events`` mirrors ``HTTPParserError``: a batch in which the
    first message decodes cleanly and the second is malformed should still
    deliver the first before the connection closes.
    """

    partial_events: list[FrameEvent]

    def __init__(self, code: int, reason: str) -> None:
        super().__init__(f"{code}: {reason}")
        self.code = code
        self.reason = reason
        self.partial_events = []


class HandshakeError(Exception):
    """The upgrade request is not a valid RFC 6455 handshake.

    Carries the HTTP status to answer with, since a failed handshake is
    still an ordinary HTTP response -- 101 never happens.
    """

    def __init__(
        self,
        status: int,
        detail: str,
        headers: list[tuple[bytes, bytes]] | None = None,
    ) -> None:
        super().__init__(f"{status}: {detail}")
        self.status = status
        self.detail = detail
        self.headers = headers or []


# -- masking ----------------------------------------------------------------


def apply_mask(payload: bytes, mask: bytes) -> bytes:
    """XOR ``payload`` with a repeating 4-byte ``mask``.

    Masking is an involution, so this both masks and unmasks. The bigint
    form is markedly faster in pure Python than a per-byte loop, which
    matters because every inbound byte on a WebSocket goes through here.
    """
    if not payload:
        return b""
    key = (mask * (len(payload) // 4 + 1))[: len(payload)]
    return (int.from_bytes(payload, "big") ^ int.from_bytes(key, "big")).to_bytes(
        len(payload), "big"
    )


# -- encoding ---------------------------------------------------------------


def encode_frame(
    opcode: Opcode,
    payload: bytes = b"",
    *,
    fin: bool = True,
    mask: bytes | None = None,
) -> bytes:
    """Serialize one frame.

    ``mask`` is None for every frame this server sends -- RFC 6455 section
    5.1 forbids a server from masking. It exists so the same codec can act
    as a client in tests.
    """
    if mask is not None and len(mask) != 4:
        raise ValueError("a websocket mask must be exactly 4 bytes")
    if opcode in _CONTROL_OPCODES:
        if len(payload) > MAX_CONTROL_PAYLOAD:
            raise ValueError(
                f"control frame payload of {len(payload)} bytes exceeds the "
                f"{MAX_CONTROL_PAYLOAD}-byte limit"
            )
        if not fin:
            raise ValueError("control frames must not be fragmented")

    first = (0x80 if fin else 0x00) | int(opcode)
    length = len(payload)
    mask_bit = 0x80 if mask is not None else 0x00
    if length < 126:
        header = struct.pack("!BB", first, mask_bit | length)
    elif length < 65536:
        header = struct.pack("!BBH", first, mask_bit | 126, length)
    else:
        header = struct.pack("!BBQ", first, mask_bit | 127, length)

    if mask is None:
        return header + payload
    return header + mask + apply_mask(payload, mask)


def encode_close_frame(
    code: int | None = CloseCode.NORMAL,
    reason: str = "",
    *,
    mask: bytes | None = None,
) -> bytes:
    """Serialize a close frame, truncating an over-long reason.

    The truncation is load-bearing rather than defensive: an
    ``HTTPException.detail`` or a validation summary used as a close reason
    routinely runs past what a control frame can carry, and the result
    would otherwise be a ValueError while trying to report an error.
    """
    if code is None:
        # A close frame may legitimately carry no payload at all. 1005 is
        # what a receiver reports for that; it is never sent on the wire.
        return encode_frame(Opcode.CLOSE, b"", mask=mask)
    encoded = reason.encode("utf-8")
    if len(encoded) > MAX_CLOSE_REASON_BYTES:
        encoded = _truncate_utf8(encoded, MAX_CLOSE_REASON_BYTES)
    return encode_frame(Opcode.CLOSE, struct.pack("!H", code) + encoded, mask=mask)


def _truncate_utf8(data: bytes, limit: int) -> bytes:
    """Cut ``data`` to at most ``limit`` bytes on a character boundary."""
    truncated = data[:limit]
    while truncated:
        try:
            truncated.decode("utf-8")
        except UnicodeDecodeError:
            truncated = truncated[:-1]
        else:
            break
    return truncated


# -- handshake --------------------------------------------------------------


def accept_key(sec_websocket_key: str | bytes) -> bytes:
    """Compute Sec-WebSocket-Accept from the client's Sec-WebSocket-Key."""
    if isinstance(sec_websocket_key, str):
        sec_websocket_key = sec_websocket_key.encode("ascii")
    return base64.b64encode(hashlib.sha1(sec_websocket_key + GUID).digest())


def _header_values(headers: list[tuple[bytes, bytes]], name: bytes) -> list[bytes]:
    return [value for key, value in headers if key.lower() == name]


def parse_subprotocols(headers: list[tuple[bytes, bytes]]) -> list[str]:
    """Client-offered subprotocols, in preference order.

    Reading this header is application metadata, not a framing decision --
    it says nothing about where bytes begin or end -- so it does not
    trespass on the parser's territory.
    """
    offered: list[str] = []
    for value in _header_values(headers, b"sec-websocket-protocol"):
        for token in value.split(b","):
            name = token.strip()
            if not name:
                continue
            try:
                offered.append(name.decode("ascii"))
            except UnicodeDecodeError:
                continue
    return offered


def validate_handshake(headers: list[tuple[bytes, bytes]]) -> bytes:
    """Check a websocket upgrade request and return its Sec-WebSocket-Key.

    The caller has already established from the parser that this request
    upgrades to "websocket"; what is checked here is whether it is a
    *valid* RFC 6455 handshake.
    """
    versions = _header_values(headers, b"sec-websocket-version")
    if not versions or versions[0].strip() != WS_VERSION:
        # RFC 6455 section 4.4: advertise what we do speak, so a client on a
        # draft version can retry rather than guess.
        raise HandshakeError(
            426,
            "this server speaks WebSocket version 13 only",
            [(b"sec-websocket-version", WS_VERSION)],
        )

    keys = _header_values(headers, b"sec-websocket-key")
    if len(keys) != 1:
        raise HandshakeError(400, "exactly one Sec-WebSocket-Key header is required")
    key = keys[0].strip()
    try:
        decoded = base64.b64decode(key, validate=True)
    except (ValueError, TypeError) as exc:
        raise HandshakeError(400, "Sec-WebSocket-Key is not valid base64") from exc
    if len(decoded) != 16:
        raise HandshakeError(400, "Sec-WebSocket-Key must decode to 16 bytes")
    return key


def encode_handshake_response(
    key: bytes,
    subprotocol: str | None = None,
    extra_headers: list[tuple[bytes, bytes]] | None = None,
) -> bytes:
    """Serialize the 101 Switching Protocols response.

    Deliberately not routed through protocol.py's _encode_response_head,
    which appends a Connection: keep-alive/close header of its own. On a 101
    that header is wrong, and strict clients reject the upgrade over it.
    """
    lines = [
        b"HTTP/1.1 101 Switching Protocols",
        b"Upgrade: websocket",
        b"Connection: Upgrade",
        b"Sec-WebSocket-Accept: " + accept_key(key),
    ]
    if subprotocol is not None:
        lines.append(b"Sec-WebSocket-Protocol: " + subprotocol.encode("ascii"))
    for name, value in extra_headers or []:
        lines.append(name + b": " + value)
    return b"\r\n".join(lines) + b"\r\n\r\n"


def client_key() -> bytes:
    """A random Sec-WebSocket-Key. For clients -- i.e. for tests."""
    return base64.b64encode(os.urandom(16))


# -- decoding ---------------------------------------------------------------


@dataclass(slots=True)
class _Frame:
    fin: bool
    opcode: Opcode
    payload: bytes


class FrameParser:
    """Incremental frame decoder emitting whole messages.

    Mirrors ``HTTP11Parser``'s shape on purpose -- one bytearray, destructive
    front-deletion, a ``while True`` loop that stops when nothing can
    progress -- so that anyone who has read the HTTP parser can read this.
    """

    def __init__(
        self,
        *,
        max_message_size: int = DEFAULT_MAX_MESSAGE_SIZE,
        require_mask: bool = True,
    ) -> None:
        self.max_message_size = max_message_size
        # True for a server (clients MUST mask), False for a client (servers
        # MUST NOT mask). The RFC's rule is directional, not absolute.
        self.require_mask = require_mask
        self._buffer = bytearray()
        self._closed = False
        self._fragments: list[bytes] = []
        self._fragment_opcode: Opcode | None = None
        self._fragment_size = 0

    @property
    def closed(self) -> bool:
        """True once a close frame has been received; terminal."""
        return self._closed

    def feed_data(self, data: bytes) -> list[FrameEvent]:
        if self._closed:
            # Anything after a close frame is discarded rather than decoded.
            # RFC 6455 section 5.5.1: the peer must send nothing more.
            return []
        self._buffer.extend(data)
        events: list[FrameEvent] = []
        while True:
            try:
                frame = self._take_frame()
            except WebSocketProtocolError as exc:
                exc.partial_events = events
                raise
            if frame is None:
                break
            try:
                event = self._handle_frame(frame)
            except WebSocketProtocolError as exc:
                exc.partial_events = events
                raise
            if event is not None:
                events.append(event)
            if self._closed:
                break
        return events

    # -- framing ------------------------------------------------------------

    def _take_frame(self) -> _Frame | None:
        buffer = self._buffer
        if len(buffer) < 2:
            return None

        first, second = buffer[0], buffer[1]
        fin = bool(first & 0x80)
        if first & 0x70:
            # We negotiate no extensions, ever, so a reserved bit can only
            # mean the peer assumed one (compression) that is not in play.
            raise WebSocketProtocolError(
                CloseCode.PROTOCOL_ERROR, "reserved bits must be zero"
            )
        try:
            opcode = Opcode(first & 0x0F)
        except ValueError:
            raise WebSocketProtocolError(
                CloseCode.PROTOCOL_ERROR, f"reserved opcode {first & 0x0F:#x}"
            ) from None

        masked = bool(second & 0x80)
        if masked != self.require_mask:
            raise WebSocketProtocolError(
                CloseCode.PROTOCOL_ERROR,
                "frames from a client must be masked"
                if self.require_mask
                else "frames from a server must not be masked",
            )

        length = second & 0x7F
        offset = 2
        if length == 126:
            if len(buffer) < offset + 2:
                return None
            length = struct.unpack_from("!H", buffer, offset)[0]
            offset += 2
        elif length == 127:
            if len(buffer) < offset + 8:
                return None
            length = struct.unpack_from("!Q", buffer, offset)[0]
            offset += 8
            if length & (1 << 63):
                raise WebSocketProtocolError(
                    CloseCode.PROTOCOL_ERROR,
                    "the most significant bit of a 64-bit length must be zero",
                )

        if opcode in _CONTROL_OPCODES:
            if length > MAX_CONTROL_PAYLOAD:
                raise WebSocketProtocolError(
                    CloseCode.PROTOCOL_ERROR,
                    f"control frame payload of {length} bytes exceeds "
                    f"{MAX_CONTROL_PAYLOAD}",
                )
            if not fin:
                raise WebSocketProtocolError(
                    CloseCode.PROTOCOL_ERROR, "control frames must not be fragmented"
                )
        else:
            # Checked here, against the *declared* length, rather than after
            # accumulating: a peer declaring a terabyte and sending nothing
            # must be rejected now, not buffered toward forever.
            projected = self._fragment_size + length
            if projected > self.max_message_size:
                raise WebSocketProtocolError(
                    CloseCode.MESSAGE_TOO_BIG,
                    f"message of at least {projected} bytes exceeds the "
                    f"{self.max_message_size}-byte limit",
                )

        if masked:
            if len(buffer) < offset + 4:
                return None
            mask = bytes(buffer[offset : offset + 4])
            offset += 4
        else:
            mask = b""

        if len(buffer) < offset + length:
            return None
        payload = bytes(buffer[offset : offset + length])
        del buffer[: offset + length]
        if mask:
            payload = apply_mask(payload, mask)
        return _Frame(fin=fin, opcode=opcode, payload=payload)

    # -- message assembly ---------------------------------------------------

    def _handle_frame(self, frame: _Frame) -> FrameEvent | None:
        if frame.opcode is Opcode.PING:
            return Ping(frame.payload)
        if frame.opcode is Opcode.PONG:
            return Pong(frame.payload)
        if frame.opcode is Opcode.CLOSE:
            self._closed = True
            return self._decode_close(frame.payload)

        if frame.opcode is Opcode.CONTINUATION:
            if self._fragment_opcode is None:
                raise WebSocketProtocolError(
                    CloseCode.PROTOCOL_ERROR,
                    "continuation frame with no message in progress",
                )
        elif self._fragment_opcode is not None:
            raise WebSocketProtocolError(
                CloseCode.PROTOCOL_ERROR,
                "a new data frame arrived while a fragmented message was "
                "still in progress",
            )
        else:
            self._fragment_opcode = frame.opcode

        self._fragments.append(frame.payload)
        self._fragment_size += len(frame.payload)
        if not frame.fin:
            return None

        opcode = self._fragment_opcode
        payload = b"".join(self._fragments)
        self._fragments = []
        self._fragment_opcode = None
        self._fragment_size = 0

        if opcode is Opcode.BINARY:
            return BinaryMessage(payload)
        # Validated on the reassembled message rather than per fragment: a
        # multi-byte character may legitimately straddle a fragment boundary,
        # and per-fragment validation would reject correct traffic.
        return TextMessage(_decode_utf8(payload, "text message"))

    def _decode_close(self, payload: bytes) -> CloseReceived:
        if not payload:
            # No status present. 1005 is the reportable stand-in, and is
            # itself never sendable.
            return CloseReceived(CloseCode.NO_STATUS, "")
        if len(payload) == 1:
            raise WebSocketProtocolError(
                CloseCode.PROTOCOL_ERROR,
                "a close payload must be empty or at least two bytes",
            )
        code = struct.unpack_from("!H", payload, 0)[0]
        if not _is_valid_close_code(code):
            raise WebSocketProtocolError(
                CloseCode.PROTOCOL_ERROR, f"invalid close code {code}"
            )
        return CloseReceived(code, _decode_utf8(payload[2:], "close reason"))


def _decode_utf8(payload: bytes, what: str) -> str:
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise WebSocketProtocolError(
            CloseCode.INVALID_PAYLOAD, f"{what} is not valid UTF-8"
        ) from exc
