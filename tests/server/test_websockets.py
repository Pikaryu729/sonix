"""Frame codec and handshake tests -- pure bytes, no event loop.

The same testing style as tests/server/test_parser.py, for the same reason:
server/websockets.py imports no asyncio and does no I/O, so every rule it
enforces can be pinned by feeding it a byte string.
"""

import base64
import os
import struct

import pytest

from sonix.server.websockets import (
    GUID,
    MAX_CONTROL_PAYLOAD,
    BinaryMessage,
    CloseCode,
    CloseReceived,
    FrameParser,
    HandshakeError,
    Opcode,
    Ping,
    Pong,
    TextMessage,
    WebSocketProtocolError,
    accept_key,
    apply_mask,
    encode_close_frame,
    encode_frame,
    encode_handshake_response,
    parse_subprotocols,
    validate_handshake,
)

MASK = b"\x37\xfa\x21\x3d"


def server_parser(**kwargs) -> FrameParser:
    """A parser configured as a server sees the wire: clients must mask."""
    return FrameParser(**kwargs)


def client_frame(opcode: Opcode, payload: bytes = b"", *, fin: bool = True) -> bytes:
    return encode_frame(opcode, payload, fin=fin, mask=MASK)


def handshake_headers(
    overrides: dict[bytes, bytes | None] | None = None,
) -> list[tuple[bytes, bytes]]:
    """A valid handshake's headers. A None override drops that header."""
    headers: dict[bytes, bytes | None] = {
        b"host": b"e.com",
        b"upgrade": b"websocket",
        b"connection": b"Upgrade",
        b"sec-websocket-key": b"dGhlIHNhbXBsZSBub25jZQ==",
        b"sec-websocket-version": b"13",
    }
    headers.update(overrides or {})
    return [(name, value) for name, value in headers.items() if value is not None]


class TestAcceptKey:
    def test_rfc6455_vector(self):
        # RFC 6455 section 1.3. If this ever fails, nothing else matters.
        assert (
            accept_key(b"dGhlIHNhbXBsZSBub25jZQ==") == b"s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
        )

    def test_accepts_str_or_bytes(self):
        assert accept_key("dGhlIHNhbXBsZSBub25jZQ==") == accept_key(
            b"dGhlIHNhbXBsZSBub25jZQ=="
        )

    def test_guid_is_the_rfc_value(self):
        assert GUID == b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


class TestMasking:
    def test_mask_is_an_involution(self):
        payload = os.urandom(1000)
        assert apply_mask(apply_mask(payload, MASK), MASK) == payload

    def test_empty_payload(self):
        assert apply_mask(b"", MASK) == b""

    def test_matches_a_naive_implementation(self):
        payload = os.urandom(37)  # deliberately not a multiple of four
        expected = bytes(b ^ MASK[i % 4] for i, b in enumerate(payload))
        assert apply_mask(payload, MASK) == expected


class TestEncodeFrame:
    def test_server_frames_are_never_masked(self):
        # The single most important property on the write side: RFC 6455
        # section 5.1 forbids a server from masking, and a client will drop
        # the connection over it.
        for size in (0, 5, 125, 126, 70000):
            frame = encode_frame(Opcode.BINARY, b"x" * size)
            assert frame[1] & 0x80 == 0

    def test_fin_and_opcode_bits(self):
        assert encode_frame(Opcode.TEXT, b"hi")[0] == 0x81
        assert encode_frame(Opcode.BINARY, b"hi", fin=False)[0] == 0x02
        assert encode_frame(Opcode.PING)[0] == 0x89

    def test_seven_bit_length(self):
        frame = encode_frame(Opcode.BINARY, b"x" * 125)
        assert frame[1] == 125
        assert len(frame) == 127

    def test_sixteen_bit_length(self):
        frame = encode_frame(Opcode.BINARY, b"x" * 126)
        assert frame[1] == 126
        assert struct.unpack_from("!H", frame, 2)[0] == 126

    def test_sixty_four_bit_length(self):
        frame = encode_frame(Opcode.BINARY, b"x" * 65536)
        assert frame[1] == 127
        assert struct.unpack_from("!Q", frame, 2)[0] == 65536

    def test_masked_frame_sets_the_bit_and_carries_the_key(self):
        frame = encode_frame(Opcode.TEXT, b"hello", mask=MASK)
        assert frame[1] & 0x80
        assert frame[2:6] == MASK
        assert apply_mask(frame[6:], MASK) == b"hello"

    def test_bad_mask_length_rejected(self):
        with pytest.raises(ValueError, match="exactly 4 bytes"):
            encode_frame(Opcode.TEXT, b"hi", mask=b"abc")

    def test_oversized_control_frame_rejected(self):
        with pytest.raises(ValueError, match="exceeds"):
            encode_frame(Opcode.PING, b"x" * 126)

    def test_fragmented_control_frame_rejected(self):
        with pytest.raises(ValueError, match="must not be fragmented"):
            encode_frame(Opcode.PING, b"x", fin=False)


class TestEncodeCloseFrame:
    def test_code_and_reason(self):
        frame = encode_close_frame(CloseCode.NORMAL, "bye")
        assert frame[0] == 0x88
        assert frame[2:] == struct.pack("!H", 1000) + b"bye"

    def test_no_code_means_empty_payload(self):
        assert encode_close_frame(None) == b"\x88\x00"

    def test_long_reason_is_truncated_to_fit_a_control_frame(self):
        # An HTTPException detail or validation summary used as a reason
        # routinely overruns the 125-byte control frame limit. Truncating
        # beats raising while trying to report an error.
        frame = encode_close_frame(CloseCode.POLICY_VIOLATION, "x" * 500)
        assert len(frame) - 2 <= MAX_CONTROL_PAYLOAD

    def test_truncation_lands_on_a_character_boundary(self):
        frame = encode_close_frame(CloseCode.POLICY_VIOLATION, "é" * 200)
        payload = frame[2:]
        payload[2:].decode("utf-8")  # must not raise
        assert len(payload) <= MAX_CONTROL_PAYLOAD


class TestDecodeHappyPath:
    def test_text_message(self):
        p = server_parser()
        events = p.feed_data(client_frame(Opcode.TEXT, "héllo".encode()))
        assert events == [TextMessage("héllo")]

    def test_binary_message(self):
        p = server_parser()
        events = p.feed_data(client_frame(Opcode.BINARY, b"\x00\xff"))
        assert events == [BinaryMessage(b"\x00\xff")]

    def test_empty_message(self):
        p = server_parser()
        assert p.feed_data(client_frame(Opcode.TEXT, b"")) == [TextMessage("")]

    def test_two_messages_in_one_feed(self):
        p = server_parser()
        data = client_frame(Opcode.TEXT, b"a") + client_frame(Opcode.TEXT, b"b")
        assert p.feed_data(data) == [TextMessage("a"), TextMessage("b")]

    def test_extended_lengths_round_trip(self):
        for size in (125, 126, 65535, 65536):
            p = server_parser()
            payload = os.urandom(size)
            assert p.feed_data(client_frame(Opcode.BINARY, payload)) == [
                BinaryMessage(payload)
            ]

    def test_ping_and_pong_surface_as_events(self):
        p = server_parser()
        events = p.feed_data(
            client_frame(Opcode.PING, b"n") + client_frame(Opcode.PONG, b"n")
        )
        assert events == [Ping(b"n"), Pong(b"n")]


class TestIncrementalFeeding:
    def test_one_byte_at_a_time(self):
        # The property that matters most for an incremental decoder: the
        # event stream must not depend on how the bytes were chunked.
        data = (
            client_frame(Opcode.TEXT, b"hello")
            + client_frame(Opcode.PING, b"p")
            + client_frame(Opcode.BINARY, b"x" * 200)
        )
        p = server_parser()
        events = []
        for index in range(len(data)):
            events.extend(p.feed_data(data[index : index + 1]))
        assert events == [TextMessage("hello"), Ping(b"p"), BinaryMessage(b"x" * 200)]

    @pytest.mark.parametrize("split", range(1, 20))
    def test_every_split_boundary_agrees(self, split):
        data = client_frame(Opcode.BINARY, b"x" * 300)
        p = server_parser()
        events = p.feed_data(data[:split]) + p.feed_data(data[split:])
        assert events == [BinaryMessage(b"x" * 300)]

    def test_partial_header_yields_nothing(self):
        p = server_parser()
        assert p.feed_data(b"\x81") == []


class TestFragmentation:
    def test_reassembles_a_fragmented_text_message(self):
        p = server_parser()
        data = client_frame(Opcode.TEXT, b"he", fin=False) + client_frame(
            Opcode.CONTINUATION, b"llo"
        )
        assert p.feed_data(data) == [TextMessage("hello")]

    def test_multibyte_character_across_a_fragment_boundary(self):
        # Per-fragment UTF-8 validation would reject this legitimate stream.
        encoded = "é".encode()
        data = client_frame(Opcode.TEXT, encoded[:1], fin=False) + client_frame(
            Opcode.CONTINUATION, encoded[1:]
        )
        assert server_parser().feed_data(data) == [TextMessage("é")]

    def test_control_frame_between_fragments_does_not_disturb_reassembly(self):
        p = server_parser()
        data = (
            client_frame(Opcode.TEXT, b"he", fin=False)
            + client_frame(Opcode.PING, b"p")
            + client_frame(Opcode.CONTINUATION, b"llo")
        )
        assert p.feed_data(data) == [Ping(b"p"), TextMessage("hello")]

    def test_three_fragments(self):
        p = server_parser()
        data = (
            client_frame(Opcode.TEXT, b"a", fin=False)
            + client_frame(Opcode.CONTINUATION, b"b", fin=False)
            + client_frame(Opcode.CONTINUATION, b"c")
        )
        assert p.feed_data(data) == [TextMessage("abc")]

    def test_continuation_with_no_message_in_progress(self):
        p = server_parser()
        with pytest.raises(WebSocketProtocolError) as excinfo:
            p.feed_data(client_frame(Opcode.CONTINUATION, b"x"))
        assert excinfo.value.code == CloseCode.PROTOCOL_ERROR

    def test_new_data_frame_while_fragmented(self):
        p = server_parser()
        with pytest.raises(WebSocketProtocolError) as excinfo:
            p.feed_data(
                client_frame(Opcode.TEXT, b"a", fin=False)
                + client_frame(Opcode.TEXT, b"b")
            )
        assert excinfo.value.code == CloseCode.PROTOCOL_ERROR


class TestFramingViolations:
    def test_unmasked_client_frame_rejected(self):
        p = server_parser()
        with pytest.raises(WebSocketProtocolError) as excinfo:
            p.feed_data(encode_frame(Opcode.TEXT, b"hi"))
        assert excinfo.value.code == CloseCode.PROTOCOL_ERROR
        assert "must be masked" in excinfo.value.reason

    def test_masked_server_frame_rejected_by_a_client_parser(self):
        p = FrameParser(require_mask=False)
        with pytest.raises(WebSocketProtocolError) as excinfo:
            p.feed_data(encode_frame(Opcode.TEXT, b"hi", mask=MASK))
        assert "must not be masked" in excinfo.value.reason

    @pytest.mark.parametrize("rsv", [0x40, 0x20, 0x10])
    def test_reserved_bits_rejected(self, rsv):
        # We negotiate no extensions ever, so a set RSV bit can only mean the
        # peer assumed compression that is not in play.
        frame = bytearray(client_frame(Opcode.TEXT, b"hi"))
        frame[0] |= rsv
        with pytest.raises(WebSocketProtocolError) as excinfo:
            server_parser().feed_data(bytes(frame))
        assert excinfo.value.code == CloseCode.PROTOCOL_ERROR

    @pytest.mark.parametrize("opcode", [0x3, 0x4, 0x7, 0xB, 0xF])
    def test_reserved_opcodes_rejected(self, opcode):
        frame = bytearray(client_frame(Opcode.TEXT, b"hi"))
        frame[0] = 0x80 | opcode
        with pytest.raises(WebSocketProtocolError) as excinfo:
            server_parser().feed_data(bytes(frame))
        assert excinfo.value.code == CloseCode.PROTOCOL_ERROR

    def test_oversized_control_frame_rejected(self):
        # Hand-built, since encode_frame refuses to produce one.
        header = struct.pack("!BB", 0x89, 0x80 | 126) + struct.pack("!H", 200)
        frame = header + MASK + apply_mask(b"x" * 200, MASK)
        with pytest.raises(WebSocketProtocolError) as excinfo:
            server_parser().feed_data(frame)
        assert excinfo.value.code == CloseCode.PROTOCOL_ERROR

    def test_fragmented_control_frame_rejected(self):
        frame = bytearray(client_frame(Opcode.PING, b"x"))
        frame[0] &= 0x7F  # clear FIN
        with pytest.raises(WebSocketProtocolError) as excinfo:
            server_parser().feed_data(bytes(frame))
        assert excinfo.value.code == CloseCode.PROTOCOL_ERROR

    def test_sixty_four_bit_length_with_the_high_bit_set(self):
        header = struct.pack("!BB", 0x82, 0x80 | 127) + struct.pack("!Q", 1 << 63)
        with pytest.raises(WebSocketProtocolError) as excinfo:
            server_parser().feed_data(header + MASK)
        assert excinfo.value.code == CloseCode.PROTOCOL_ERROR

    def test_invalid_utf8_in_a_text_message(self):
        p = server_parser()
        with pytest.raises(WebSocketProtocolError) as excinfo:
            p.feed_data(client_frame(Opcode.TEXT, b"\xff\xfe"))
        assert excinfo.value.code == CloseCode.INVALID_PAYLOAD

    def test_invalid_utf8_is_fine_in_a_binary_message(self):
        p = server_parser()
        assert p.feed_data(client_frame(Opcode.BINARY, b"\xff\xfe")) == [
            BinaryMessage(b"\xff\xfe")
        ]

    def test_partial_events_survive_a_violation(self):
        # A good message ahead of a malformed one must still be delivered,
        # exactly as HTTPParserError.partial_events does for pipelining.
        p = server_parser()
        good = client_frame(Opcode.TEXT, b"first")
        bad = bytearray(client_frame(Opcode.TEXT, b"second"))
        bad[0] |= 0x40
        with pytest.raises(WebSocketProtocolError) as excinfo:
            p.feed_data(good + bytes(bad))
        assert excinfo.value.partial_events == [TextMessage("first")]


class TestMessageSizeLimit:
    def test_accumulated_fragments_over_the_limit(self):
        p = server_parser(max_message_size=10)
        with pytest.raises(WebSocketProtocolError) as excinfo:
            p.feed_data(
                client_frame(Opcode.TEXT, b"x" * 6, fin=False)
                + client_frame(Opcode.CONTINUATION, b"x" * 6)
            )
        assert excinfo.value.code == CloseCode.MESSAGE_TOO_BIG

    def test_declared_but_unsent_length_is_rejected_immediately(self):
        # A peer declaring a terabyte and sending nothing must be refused
        # now, not buffered toward forever.
        p = server_parser(max_message_size=1024)
        header = struct.pack("!BB", 0x82, 0x80 | 127) + struct.pack("!Q", 1 << 40)
        with pytest.raises(WebSocketProtocolError) as excinfo:
            p.feed_data(header)
        assert excinfo.value.code == CloseCode.MESSAGE_TOO_BIG

    def test_exactly_at_the_limit_is_allowed(self):
        p = server_parser(max_message_size=10)
        assert p.feed_data(client_frame(Opcode.BINARY, b"x" * 10)) == [
            BinaryMessage(b"x" * 10)
        ]

    def test_control_frames_are_not_subject_to_the_message_limit(self):
        p = server_parser(max_message_size=1)
        assert p.feed_data(client_frame(Opcode.PING, b"x" * 100)) == [Ping(b"x" * 100)]


class TestClose:
    def test_code_and_reason(self):
        p = server_parser()
        events = p.feed_data(
            client_frame(Opcode.CLOSE, struct.pack("!H", 1000) + b"bye")
        )
        assert events == [CloseReceived(1000, "bye")]
        assert p.closed is True

    def test_empty_payload_reports_1005(self):
        p = server_parser()
        assert p.feed_data(client_frame(Opcode.CLOSE, b"")) == [
            CloseReceived(CloseCode.NO_STATUS, "")
        ]

    def test_one_byte_payload_rejected(self):
        p = server_parser()
        with pytest.raises(WebSocketProtocolError) as excinfo:
            p.feed_data(client_frame(Opcode.CLOSE, b"\x03"))
        assert excinfo.value.code == CloseCode.PROTOCOL_ERROR

    @pytest.mark.parametrize("code", [0, 999, 1004, 1005, 1006, 1015, 1016, 2999, 5000])
    def test_invalid_close_codes_rejected(self, code):
        p = server_parser()
        with pytest.raises(WebSocketProtocolError) as excinfo:
            p.feed_data(client_frame(Opcode.CLOSE, struct.pack("!H", code)))
        assert excinfo.value.code == CloseCode.PROTOCOL_ERROR

    @pytest.mark.parametrize("code", [1000, 1003, 1007, 1011, 3000, 4999])
    def test_valid_close_codes_accepted(self, code):
        p = server_parser()
        assert p.feed_data(client_frame(Opcode.CLOSE, struct.pack("!H", code))) == [
            CloseReceived(code, "")
        ]

    def test_invalid_utf8_reason(self):
        p = server_parser()
        with pytest.raises(WebSocketProtocolError) as excinfo:
            p.feed_data(client_frame(Opcode.CLOSE, struct.pack("!H", 1000) + b"\xff"))
        assert excinfo.value.code == CloseCode.INVALID_PAYLOAD

    def test_frames_after_close_are_discarded(self):
        p = server_parser()
        p.feed_data(client_frame(Opcode.CLOSE, struct.pack("!H", 1000)))
        assert p.feed_data(client_frame(Opcode.TEXT, b"ignored")) == []

    def test_frames_batched_after_close_stop_decoding(self):
        p = server_parser()
        data = client_frame(Opcode.CLOSE, struct.pack("!H", 1000)) + client_frame(
            Opcode.TEXT, b"ignored"
        )
        assert p.feed_data(data) == [CloseReceived(1000, "")]


class TestValidateHandshake:
    def test_valid_handshake_returns_the_key(self):
        assert validate_handshake(handshake_headers()) == b"dGhlIHNhbXBsZSBub25jZQ=="

    def test_missing_version_is_426_advertising_13(self):
        with pytest.raises(HandshakeError) as excinfo:
            validate_handshake(handshake_headers({b"sec-websocket-version": None}))
        assert excinfo.value.status == 426
        assert (b"sec-websocket-version", b"13") in excinfo.value.headers

    def test_wrong_version_is_426(self):
        with pytest.raises(HandshakeError) as excinfo:
            validate_handshake(handshake_headers({b"sec-websocket-version": b"8"}))
        assert excinfo.value.status == 426

    def test_missing_key_is_400(self):
        with pytest.raises(HandshakeError) as excinfo:
            validate_handshake(handshake_headers({b"sec-websocket-key": None}))
        assert excinfo.value.status == 400

    def test_key_that_is_not_base64_is_400(self):
        with pytest.raises(HandshakeError) as excinfo:
            validate_handshake(handshake_headers({b"sec-websocket-key": b"!!!!"}))
        assert excinfo.value.status == 400

    def test_key_of_the_wrong_length_is_400(self):
        short = base64.b64encode(b"too short")
        with pytest.raises(HandshakeError) as excinfo:
            validate_handshake(handshake_headers({b"sec-websocket-key": short}))
        assert excinfo.value.status == 400
        assert "16 bytes" in excinfo.value.detail

    def test_duplicate_key_headers_rejected(self):
        headers = handshake_headers()
        headers.append((b"sec-websocket-key", b"dGhlIHNhbXBsZSBub25jZQ=="))
        with pytest.raises(HandshakeError) as excinfo:
            validate_handshake(headers)
        assert excinfo.value.status == 400


class TestSubprotocols:
    def test_none_offered(self):
        assert parse_subprotocols(handshake_headers()) == []

    def test_comma_separated_list_in_preference_order(self):
        headers = handshake_headers({b"sec-websocket-protocol": b"chat, superchat"})
        assert parse_subprotocols(headers) == ["chat", "superchat"]

    def test_repeated_headers_are_concatenated(self):
        headers = handshake_headers({b"sec-websocket-protocol": b"chat"})
        headers.append((b"sec-websocket-protocol", b"superchat"))
        assert parse_subprotocols(headers) == ["chat", "superchat"]

    def test_empty_tokens_are_dropped(self):
        headers = handshake_headers({b"sec-websocket-protocol": b"chat, ,"})
        assert parse_subprotocols(headers) == ["chat"]


class TestHandshakeResponse:
    def test_shape(self):
        response = encode_handshake_response(b"dGhlIHNhbXBsZSBub25jZQ==")
        assert response.startswith(b"HTTP/1.1 101 Switching Protocols\r\n")
        assert b"Upgrade: websocket\r\n" in response
        assert b"Connection: Upgrade\r\n" in response
        assert b"Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n" in response
        assert response.endswith(b"\r\n\r\n")

    def test_no_connection_keep_alive_header(self):
        # protocol.py's HTTP response encoder always appends one, which is
        # wrong on a 101 and breaks strict clients. This response must not
        # be routed through it.
        response = encode_handshake_response(b"dGhlIHNhbXBsZSBub25jZQ==")
        assert b"keep-alive" not in response.lower()

    def test_subprotocol_echoed(self):
        response = encode_handshake_response(
            b"dGhlIHNhbXBsZSBub25jZQ==", subprotocol="chat"
        )
        assert b"Sec-WebSocket-Protocol: chat\r\n" in response

    def test_extra_headers_appended(self):
        response = encode_handshake_response(
            b"dGhlIHNhbXBsZSBub25jZQ==", extra_headers=[(b"x-trace", b"abc")]
        )
        assert b"x-trace: abc\r\n" in response


class TestCodecAsAClient:
    """The codec drives both ends, which is what the end-to-end client uses."""

    def test_round_trip_through_both_directions(self):
        client = FrameParser(require_mask=False)
        server = FrameParser(require_mask=True)
        mask = os.urandom(4)
        assert server.feed_data(encode_frame(Opcode.TEXT, b"ping", mask=mask)) == [
            TextMessage("ping")
        ]
        assert client.feed_data(encode_frame(Opcode.TEXT, b"pong")) == [
            TextMessage("pong")
        ]

    def test_close_frames_round_trip(self):
        client = FrameParser(require_mask=False)
        assert client.feed_data(encode_close_frame(CloseCode.GOING_AWAY, "bye")) == [
            CloseReceived(1001, "bye")
        ]
