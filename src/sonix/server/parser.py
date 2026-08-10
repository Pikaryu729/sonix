"""Incremental HTTP/1.1 request parser.

Pure and synchronous: no ``asyncio`` import, no I/O. Bytes go in via
``feed_data``/``feed_eof``, parsed events come out. This module never
inspects headers to decide framing more than once per rule -- the same
request must not be parsed two different ways by two different pieces
of code, which is the classic root cause of request smuggling.

That rule is why "this connection has stopped speaking HTTP" is decided
here rather than a layer up. Deciding it above would be too late anyway:
``feed_data`` runs to exhaustion before it returns, so by the time a
caller could inspect the head's headers, the bytes after an upgrade
handshake have already been parsed as though they were another request.
"""

from __future__ import annotations

import enum
import re
from dataclasses import dataclass, field


class HTTPParserError(Exception):
    """Base class for all errors raised by :class:`HTTP11Parser`.

    ``partial_events`` holds whatever events ``feed_data`` had already
    accumulated for earlier, fully-parsed pipelined requests before this
    error was raised, so a caller can still act on them instead of losing
    them along with the exception.
    """

    partial_events: list[Event]

    def __init__(self, *args: object) -> None:
        super().__init__(*args)
        self.partial_events = []


class MalformedRequest(HTTPParserError):
    """The input is not a syntactically valid HTTP/1.1 request."""


class RequestTooLarge(HTTPParserError):
    """A configured header or body size limit was exceeded."""


@dataclass(frozen=True, slots=True)
class RequestHead:
    method: str
    target: str
    path: str
    query_string: bytes
    http_version: str
    headers: list[tuple[bytes, bytes]] = field(default_factory=list)
    # The lowercased Upgrade token when this request switches the connection to
    # another protocol ("websocket"), otherwise None. A token rather than a
    # bool: HTTP/1.1 Upgrade is a general mechanism, and a string lets the
    # caller say "I only implement websocket" by comparing a value the parser
    # handed it -- rather than re-tokenizing the header itself, which is
    # exactly the duplicated-framing-decision this module exists to prevent.
    upgrade: str | None = None


@dataclass(frozen=True, slots=True)
class RequestHeadComplete:
    head: RequestHead


@dataclass(frozen=True, slots=True)
class BodyChunk:
    data: bytes
    more_body: bool


@dataclass(frozen=True, slots=True)
class RequestComplete:
    pass


Event = RequestHeadComplete | BodyChunk | RequestComplete


class _State(enum.Enum):
    START_LINE = enum.auto()
    HEADERS = enum.auto()
    BODY = enum.auto()
    # Terminal. HTTP framing has stopped for the life of this connection
    # because the request head switched it to another protocol.
    UPGRADED = enum.auto()


class _ChunkState(enum.Enum):
    SIZE = enum.auto()
    DATA = enum.auto()
    DATA_CRLF = enum.auto()
    TRAILERS = enum.auto()


DEFAULT_UPGRADE_PROTOCOLS = frozenset({"websocket"})

_TOKEN_RE = re.compile(rb"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")
_HEX_RE = re.compile(rb"^[0-9A-Fa-f]+$")
# 2**64 is 20 decimal digits, so nothing longer can be a real length. The cap
# is not cosmetic: CPython refuses str->int conversion past 4300 digits with a
# plain ValueError, which is not an HTTPParserError and so escaped the whole
# "reject, never resolve" contract -- the connection died on an unhandled
# exception instead of getting a clean 4xx. A digit string long enough to
# trigger that fits easily inside max_header_size.
_MAX_LENGTH_DIGITS = 20
_DIGITS_RE = re.compile(rb"^[0-9]+$")
_VALID_VERSIONS = {b"HTTP/1.0": "1.0", b"HTTP/1.1": "1.1"}


class HTTP11Parser:
    """An incremental HTTP/1.1 request parser for a single connection.

    ``feed_data`` may return events spanning more than one request --
    pipelined requests concatenated in a single call are fully drained
    in one pass, with any leftover partial request retained internally
    for the next call.
    """

    def __init__(
        self,
        *,
        max_header_size: int = 8 * 1024,
        max_headers: int = 100,
        max_body_size: int = 16 * 1024 * 1024,
        upgrade_protocols: frozenset[str] = DEFAULT_UPGRADE_PROTOCOLS,
    ) -> None:
        self.max_header_size = max_header_size
        self.max_headers = max_headers
        self.max_body_size = max_body_size
        # Which Upgrade tokens actually stop HTTP framing on this connection.
        # This is what keeps the parser ignorant of WebSocket *semantics* while
        # still owning the framing *consequence*: an offer this server does not
        # implement (say `Upgrade: h2c` with h2c absent here) leaves
        # head.upgrade as None and the request is parsed as an ordinary
        # HTTP/1.1 request, which is what RFC 9110 section 7.8 prescribes.
        self.upgrade_protocols = upgrade_protocols
        self._buffer = bytearray()
        self._state = _State.START_LINE
        self._reset_for_next_request()

    @property
    def upgraded(self) -> bool:
        """True once a request head switched the connection off HTTP."""
        return self._state is _State.UPGRADED

    @property
    def mid_request(self) -> bool:
        """True when a request is partly received: not at a request boundary.

        Exists so a caller can tell "this connection is idle between requests"
        from "a request head is being dripped in one byte at a time". Those
        are two different conditions with two different right answers -- close
        quietly, versus 408 -- and nothing else a caller can see separates
        them.

        Deliberately a predicate rather than a buffer accessor. Handing out
        the buffer would let a caller reach its own conclusion about where a
        request begins, which is the one thing this module exists to prevent.

        Note both halves are load-bearing. A partial *head* leaves the state
        at START_LINE with bytes buffered, so a state-only test would call
        that idle -- and a state-only test is exactly the mistake that hands
        a slow-loris client the more generous timeout.
        """
        return self._state is not _State.START_LINE or bool(self._buffer)

    def feed_data(self, data: bytes) -> list[Event]:
        if self._state is _State.UPGRADED:
            # Post-upgrade bytes belong to whatever protocol took over. Buffer
            # them rather than raising, so a caller that has not yet drained
            # take_buffer() loses nothing.
            self._buffer.extend(data)
            return []
        self._buffer.extend(data)
        events: list[Event] = []
        while True:
            try:
                if self._state is _State.START_LINE:
                    progressed = self._consume_start_line(events)
                elif self._state is _State.HEADERS:
                    progressed = self._consume_headers(events)
                elif self._state is _State.BODY:
                    progressed = self._consume_body(events)
                else:
                    # UPGRADED. Terminal, so nothing can progress -- and this
                    # is the whole point: no second RequestHeadComplete can be
                    # emitted for bytes the client believes are tunnelled.
                    progressed = False
            except HTTPParserError as exc:
                exc.partial_events = events
                raise
            if not progressed:
                break
        return events

    def feed_eof(self) -> None:
        if self._state is _State.UPGRADED:
            # An upgraded connection closing is not an incomplete request --
            # there is no request in flight, and never will be again.
            return
        if self.mid_request:
            raise MalformedRequest("connection closed with an incomplete request")

    def take_buffer(self) -> bytes:
        """Hand over the bytes that follow an upgrade head, and clear them.

        Clients may send the first frame of the new protocol in the same TCP
        segment as the handshake, in which case those bytes are already sitting
        in this parser's buffer. Without this they would be silently lost.

        Only legal once upgraded: in any other state the buffer holds a
        partially-parsed request that this parser still owns.
        """
        if self._state is not _State.UPGRADED:
            raise RuntimeError("take_buffer() is only valid after an upgrade")
        data = bytes(self._buffer)
        del self._buffer[:]
        return data

    # -- internal state ---------------------------------------------------

    def _reset_for_next_request(self) -> None:
        self._state = _State.START_LINE
        self._method: str | None = None
        self._target: str | None = None
        self._path: str | None = None
        self._query_string: bytes | None = None
        self._http_version: str | None = None
        self._headers: list[tuple[bytes, bytes]] = []
        self._headers_size = 0
        self._header_count = 0
        self._content_length: int | None = None
        self._transfer_encoding_seen = False
        self._chunked = False
        self._connection_tokens: set[bytes] = set()
        self._upgrade_token: bytes | None = None
        self._upgrade_token_valid = False
        self._body_remaining = 0
        self._chunk_state = _ChunkState.SIZE
        self._chunk_remaining = 0
        self._body_bytes_total = 0

    def _find_bounded_line(self, limit: int) -> bytes | None:
        idx = self._buffer.find(b"\r\n")
        if idx == -1:
            if len(self._buffer) > limit:
                raise RequestTooLarge(f"line exceeds {limit}-byte limit")
            return None
        if idx > limit:
            raise RequestTooLarge(f"line exceeds {limit}-byte limit")
        line = bytes(self._buffer[:idx])
        del self._buffer[: idx + 2]
        return line

    # -- start line ---------------------------------------------------------

    def _consume_start_line(self, events: list[Event]) -> bool:
        line = self._find_bounded_line(self.max_header_size)
        if line is None:
            return False
        self._parse_request_line(line)
        self._state = _State.HEADERS
        return True

    def _parse_request_line(self, line: bytes) -> None:
        if not line:
            raise MalformedRequest("empty request line")
        if any(b < 0x20 or b == 0x7F for b in line):
            raise MalformedRequest("control character in request line")
        parts = line.split(b" ")
        if len(parts) != 3:
            raise MalformedRequest(f"malformed request line: {line!r}")
        method, target, version = parts
        if not _TOKEN_RE.fullmatch(method):
            raise MalformedRequest(f"invalid method token: {method!r}")
        if version not in _VALID_VERSIONS:
            raise MalformedRequest(f"unsupported HTTP version: {version!r}")
        if not target:
            raise MalformedRequest("empty request target")
        try:
            target_str = target.decode("ascii")
        except UnicodeDecodeError as exc:
            raise MalformedRequest("request target is not ASCII") from exc
        path_str, _, query = target_str.partition("?")
        self._method = method.decode("ascii")
        self._target = target_str
        self._path = path_str
        self._query_string = query.encode("ascii")
        self._http_version = _VALID_VERSIONS[version]

    # -- headers --------------------------------------------------------------

    def _consume_headers(self, events: list[Event]) -> bool:
        line = self._find_bounded_line(self.max_header_size)
        if line is None:
            return False
        if line == b"":
            self._finish_headers(events)
            return True
        self._consume_header_line(line)
        return True

    def _finish_headers(self, events: list[Event]) -> None:
        if self._content_length is not None and self._chunked:
            raise MalformedRequest(
                "Content-Length and Transfer-Encoding must not both be present"
            )
        assert self._method is not None
        assert self._target is not None
        assert self._path is not None
        assert self._query_string is not None
        assert self._http_version is not None
        upgrade = self._detect_upgrade()
        if upgrade is not None and (self._chunked or (self._content_length or 0) > 0):
            raise MalformedRequest(
                "an upgrade request must not declare a body: the bytes after "
                "the head would be ambiguously body or post-upgrade protocol "
                "data"
            )
        head = RequestHead(
            method=self._method,
            target=self._target,
            path=self._path,
            query_string=self._query_string,
            http_version=self._http_version,
            headers=list(self._headers),
            upgrade=upgrade,
        )
        events.append(RequestHeadComplete(head))
        if upgrade is not None:
            # Deliberately no BodyChunk, no RequestComplete, and no reset. HTTP
            # framing is over: whatever follows in the buffer belongs to the
            # new protocol and is handed out by take_buffer().
            #
            # This is also a request-smuggling defense, not just bookkeeping.
            # Without it, a client sending
            #     GET /ws HTTP/1.1 ... Upgrade: websocket\r\n\r\n
            #     GET /admin HTTP/1.1\r\nHost: x\r\n\r\n
            # in one segment would produce *two* RequestHeadComplete events and
            # the second would be dispatched as a real request -- on a
            # connection the client believes is an opaque tunnel.
            self._state = _State.UPGRADED
            return
        self._body_remaining = self._content_length or 0
        self._state = _State.BODY

    def _detect_upgrade(self) -> str | None:
        if b"upgrade" not in self._connection_tokens:
            return None
        if self._upgrade_token is None or not self._upgrade_token_valid:
            return None
        # RFC 6455 section 4.1: the handshake is a bodyless HTTP/1.1 GET. A
        # server that switched protocols on anything else would be guessing.
        if self._method != "GET" or self._http_version != "1.1":
            return None
        try:
            token = self._upgrade_token.decode("ascii")
        except UnicodeDecodeError:
            return None
        if token not in self.upgrade_protocols:
            return None
        return token

    def _consume_header_line(self, line: bytes) -> None:
        if line[:1] in (b" ", b"\t"):
            raise MalformedRequest("header folding (obs-fold) is not supported")
        if any((b < 0x20 and b != 0x09) or b == 0x7F for b in line):
            raise MalformedRequest("control character in header line")
        name, sep, value = line.partition(b":")
        if not sep:
            raise MalformedRequest(f"malformed header line: {line!r}")
        if not _TOKEN_RE.fullmatch(name):
            raise MalformedRequest(f"invalid header name: {name!r}")
        value = value.strip(b" \t")
        name_lower = name.lower()
        self._headers.append((name_lower, value))
        self._headers_size += len(line) + 2
        if self._headers_size > self.max_header_size:
            raise RequestTooLarge("cumulative header size exceeds max_header_size")
        self._header_count += 1
        if self._header_count > self.max_headers:
            raise RequestTooLarge("too many headers")
        if name_lower == b"content-length":
            self._handle_content_length(value)
        elif name_lower == b"transfer-encoding":
            self._handle_transfer_encoding(value)
        elif name_lower == b"connection":
            self._connection_tokens.update(
                token.strip().lower() for token in value.split(b",")
            )
        elif name_lower == b"upgrade":
            self._handle_upgrade(value)

    def _handle_upgrade(self, value: bytes) -> None:
        # Only a single-token Upgrade offer is honoured. A list ("websocket,
        # h2c") would make "which protocol did we switch to" a negotiation,
        # and a parser that guesses wrong hands the connection to the wrong
        # reader -- so the whole header is ignored instead.
        token = value.strip().lower()
        if self._upgrade_token is not None:
            self._upgrade_token_valid = False
            return
        self._upgrade_token = token
        self._upgrade_token_valid = bool(token) and b"," not in token

    def _handle_content_length(self, value: bytes) -> None:
        if self._content_length is not None:
            raise MalformedRequest("duplicate Content-Length header")
        if not _DIGITS_RE.fullmatch(value):
            raise MalformedRequest(f"invalid Content-Length value: {value!r}")
        if len(value) > _MAX_LENGTH_DIGITS:
            raise RequestTooLarge(
                f"Content-Length has {len(value)} digits; no real length does"
            )
        length = int(value)
        if length > self.max_body_size:
            raise RequestTooLarge("Content-Length exceeds max_body_size")
        self._content_length = length

    def _handle_transfer_encoding(self, value: bytes) -> None:
        if self._transfer_encoding_seen:
            raise MalformedRequest("duplicate Transfer-Encoding header")
        self._transfer_encoding_seen = True
        if b"," in value:
            raise MalformedRequest(
                "Transfer-Encoding must not combine multiple codings"
            )
        if value.lower() != b"chunked":
            raise MalformedRequest(f"unsupported Transfer-Encoding value: {value!r}")
        self._chunked = True

    # -- body -------------------------------------------------------------

    def _consume_body(self, events: list[Event]) -> bool:
        if self._chunked:
            return self._consume_chunked_body(events)
        if self._body_remaining == 0:
            events.append(BodyChunk(b"", False))
            events.append(RequestComplete())
            self._reset_for_next_request()
            return True
        if not self._buffer:
            return False
        take = min(len(self._buffer), self._body_remaining)
        chunk = bytes(self._buffer[:take])
        del self._buffer[:take]
        self._body_remaining -= take
        more_body = self._body_remaining > 0
        events.append(BodyChunk(chunk, more_body))
        if not more_body:
            events.append(RequestComplete())
            self._reset_for_next_request()
        return True

    def _consume_chunked_body(self, events: list[Event]) -> bool:
        if self._chunk_state is _ChunkState.SIZE:
            line = self._find_bounded_line(self.max_header_size)
            if line is None:
                return False
            size_token = line.split(b";", 1)[0].strip()
            if len(size_token) > _MAX_LENGTH_DIGITS:
                # Hex conversion is not subject to CPython's decimal-digit
                # limit, but relying on that implementation detail to stay
                # safe is fragile, and no real chunk size is this long.
                raise RequestTooLarge(
                    f"chunk size has {len(size_token)} digits; no real size does"
                )
            if not size_token or not _HEX_RE.fullmatch(size_token):
                raise MalformedRequest(f"invalid chunk size: {line!r}")
            size = int(size_token, 16)
            if size == 0:
                self._chunk_state = _ChunkState.TRAILERS
                return True
            self._body_bytes_total += size
            if self._body_bytes_total > self.max_body_size:
                raise RequestTooLarge("chunked body exceeds max_body_size")
            self._chunk_remaining = size
            self._chunk_state = _ChunkState.DATA
            return True

        if self._chunk_state is _ChunkState.DATA:
            if not self._buffer:
                return False
            take = min(len(self._buffer), self._chunk_remaining)
            chunk = bytes(self._buffer[:take])
            del self._buffer[:take]
            self._chunk_remaining -= take
            events.append(BodyChunk(chunk, True))
            if self._chunk_remaining == 0:
                self._chunk_state = _ChunkState.DATA_CRLF
            return True

        if self._chunk_state is _ChunkState.DATA_CRLF:
            if len(self._buffer) < 2:
                return False
            if bytes(self._buffer[:2]) != b"\r\n":
                raise MalformedRequest("malformed chunk terminator")
            del self._buffer[:2]
            self._chunk_state = _ChunkState.SIZE
            return True

        if self._chunk_state is _ChunkState.TRAILERS:
            line = self._find_bounded_line(self.max_header_size)
            if line is None:
                return False
            if line == b"":
                events.append(BodyChunk(b"", False))
                events.append(RequestComplete())
                self._reset_for_next_request()
                return True
            # Trailers are discarded, but they must still be counted. Each
            # line is bounded by max_header_size, yet nothing bounded how
            # *many* could arrive -- and trailers count against neither
            # max_headers nor max_body_size, so an endless stream of them held
            # a connection open with no limit reachable at all.
            self._header_count += 1
            if self._header_count > self.max_headers:
                raise RequestTooLarge("too many trailer headers")
            return True

        raise AssertionError(  # pragma: no cover - exhaustive over _ChunkState
            f"unreachable chunk state: {self._chunk_state}"
        )
