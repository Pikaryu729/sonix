import pytest

from sonix.server.parser import (
    BodyChunk,
    HTTP11Parser,
    MalformedRequest,
    RequestComplete,
    RequestHeadComplete,
    RequestTooLarge,
)


def parser(**kwargs) -> HTTP11Parser:
    return HTTP11Parser(**kwargs)


def feed(p: HTTP11Parser, data: bytes) -> list:
    return p.feed_data(data)


class TestHappyPath:
    def test_simple_get(self):
        p = parser()
        events = feed(p, b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")
        assert len(events) == 3
        head_event, body_event, complete_event = events
        assert isinstance(head_event, RequestHeadComplete)
        assert head_event.head.method == "GET"
        assert head_event.head.path == "/"
        assert head_event.head.http_version == "1.1"
        assert (b"host", b"example.com") in head_event.head.headers
        assert isinstance(body_event, BodyChunk)
        assert body_event.data == b""
        assert body_event.more_body is False
        assert isinstance(complete_event, RequestComplete)

    def test_mixed_case_headers_lowercased(self):
        p = parser()
        events = feed(
            p,
            b"GET / HTTP/1.1\r\nHost: example.com\r\nX-Custom-Header: value\r\n\r\n",
        )
        head = events[0].head
        names = [name for name, _ in head.headers]
        assert b"host" in names
        assert b"x-custom-header" in names

    def test_post_with_content_length_body(self):
        p = parser()
        events = feed(
            p,
            b"POST / HTTP/1.1\r\nHost: e.com\r\nContent-Length: 5\r\n\r\nhello",
        )
        assert isinstance(events[0], RequestHeadComplete)
        body_chunks = [e for e in events if isinstance(e, BodyChunk)]
        assert b"".join(c.data for c in body_chunks) == b"hello"
        assert body_chunks[-1].more_body is False
        assert isinstance(events[-1], RequestComplete)

    def test_query_string_split_from_path(self):
        p = parser()
        events = feed(p, b"GET /items?limit=10 HTTP/1.1\r\nHost: e.com\r\n\r\n")
        head = events[0].head
        assert head.path == "/items"
        assert head.query_string == b"limit=10"
        assert head.target == "/items?limit=10"

    def test_no_body_request_emits_empty_final_chunk(self):
        p = parser()
        events = feed(p, b"GET / HTTP/1.1\r\nHost: e.com\r\n\r\n")
        body_events = [e for e in events if isinstance(e, BodyChunk)]
        assert len(body_events) == 1
        assert body_events[0] == BodyChunk(b"", False)


class TestPipelining:
    def test_two_requests_in_one_feed_call(self):
        p = parser()
        data = (
            b"GET /a HTTP/1.1\r\nHost: e.com\r\n\r\n"
            b"GET /b HTTP/1.1\r\nHost: e.com\r\n\r\n"
        )
        events = feed(p, data)
        head_events = [e for e in events if isinstance(e, RequestHeadComplete)]
        complete_events = [e for e in events if isinstance(e, RequestComplete)]
        assert [h.head.path for h in head_events] == ["/a", "/b"]
        assert len(complete_events) == 2

    def test_partial_third_request_retained(self):
        p = parser()
        data = (
            b"GET /a HTTP/1.1\r\nHost: e.com\r\n\r\n"
            b"GET /b HTTP/1.1\r\nHost: e.com\r\n\r\n"
            b"GET /c HTTP/1.1\r\nHost: e"
        )
        events = feed(p, data)
        complete_events = [e for e in events if isinstance(e, RequestComplete)]
        assert len(complete_events) == 2

        more_events = feed(p, b".com\r\n\r\n")
        head_events = [e for e in more_events if isinstance(e, RequestHeadComplete)]
        assert head_events[0].head.path == "/c"

    def test_body_then_pipelined_request_same_call(self):
        p = parser()
        data = (
            b"POST / HTTP/1.1\r\nHost: e.com\r\nContent-Length: 5\r\n\r\nhello"
            b"GET /next HTTP/1.1\r\nHost: e.com\r\n\r\n"
        )
        events = feed(p, data)
        head_events = [e for e in events if isinstance(e, RequestHeadComplete)]
        assert head_events[0].head.method == "POST"
        assert head_events[1].head.path == "/next"

    def test_good_request_then_malformed_request_exposes_partial_events(self):
        p = parser()
        data = (
            b"GET /a HTTP/1.1\r\nHost: e.com\r\n\r\n"
            b"GET/HTTP/1.1\r\nHost: e.com\r\n\r\n"  # missing SPs -> malformed
        )
        with pytest.raises(MalformedRequest) as exc_info:
            feed(p, data)
        partial_events = exc_info.value.partial_events
        head_events = [e for e in partial_events if isinstance(e, RequestHeadComplete)]
        complete_events = [e for e in partial_events if isinstance(e, RequestComplete)]
        assert [h.head.path for h in head_events] == ["/a"]
        assert len(complete_events) == 1


class TestIncrementalFeeding:
    def test_request_line_split_across_calls(self):
        p = parser()
        assert feed(p, b"GET / HTTP/1") == []
        events = feed(p, b".1\r\nHost: e.com\r\n\r\n")
        assert isinstance(events[0], RequestHeadComplete)
        assert events[0].head.http_version == "1.1"

    @pytest.mark.parametrize(
        "request_bytes",
        [
            b"GET / HTTP/1.1\r\nHost: e.com\r\n\r\n",
            b"POST /x HTTP/1.1\r\nHost: e.com\r\nContent-Length: 3\r\n\r\nabc",
        ],
    )
    def test_byte_at_a_time_matches_whole_feed(self, request_bytes):
        # Byte-at-a-time feeding legitimately produces more, smaller BodyChunk
        # events than a whole-request feed (each call only sees what it's
        # given) -- that's correct streaming behavior, not a bug. So this
        # compares semantic content (head + reassembled body + completion),
        # not raw event-list equality.
        def summarize(events):
            heads = [e.head for e in events if isinstance(e, RequestHeadComplete)]
            body = b"".join(e.data for e in events if isinstance(e, BodyChunk))
            num_complete = sum(1 for e in events if isinstance(e, RequestComplete))
            return heads, body, num_complete

        whole = parser()
        expected = summarize(feed(whole, request_bytes))

        incremental = parser()
        actual_events: list = []
        for i in range(len(request_bytes)):
            actual_events.extend(feed(incremental, request_bytes[i : i + 1]))

        assert summarize(actual_events) == expected

    def test_chunk_size_and_data_split_across_calls(self):
        p = parser()
        feed(
            p,
            b"POST / HTTP/1.1\r\nHost: e.com\r\nTransfer-Encoding: chunked\r\n\r\n",
        )
        assert feed(p, b"5\r\n") == []
        events = feed(p, b"hello\r\n0\r\n\r\n")
        body_chunks = [e for e in events if isinstance(e, BodyChunk)]
        assert body_chunks[0].data == b"hello"


class TestContentLengthSmuggling:
    def test_content_length_and_transfer_encoding_both_present_rejected(self):
        p = parser()
        with pytest.raises(MalformedRequest):
            feed(
                p,
                b"POST / HTTP/1.1\r\nHost: e.com\r\nContent-Length: 5\r\n"
                b"Transfer-Encoding: chunked\r\n\r\nhello",
            )

    def test_duplicate_content_length_differing_values_rejected(self):
        p = parser()
        with pytest.raises(MalformedRequest):
            feed(
                p,
                b"POST / HTTP/1.1\r\nHost: e.com\r\nContent-Length: 10\r\n"
                b"Content-Length: 20\r\n\r\n",
            )

    def test_duplicate_content_length_same_value_also_rejected(self):
        p = parser()
        with pytest.raises(MalformedRequest):
            feed(
                p,
                b"POST / HTTP/1.1\r\nHost: e.com\r\nContent-Length: 10\r\n"
                b"Content-Length: 10\r\n\r\n",
            )

    def test_non_numeric_content_length_rejected(self):
        p = parser()
        with pytest.raises(MalformedRequest):
            feed(p, b"POST / HTTP/1.1\r\nHost: e.com\r\nContent-Length: abc\r\n\r\n")

    def test_negative_content_length_rejected(self):
        p = parser()
        with pytest.raises(MalformedRequest):
            feed(p, b"POST / HTTP/1.1\r\nHost: e.com\r\nContent-Length: -1\r\n\r\n")


class TestTransferEncodingSmuggling:
    def test_chunked_alone_accepted(self):
        p = parser()
        events = feed(
            p,
            b"POST / HTTP/1.1\r\nHost: e.com\r\nTransfer-Encoding: chunked\r\n\r\n"
            b"0\r\n\r\n",
        )
        assert isinstance(events[0], RequestHeadComplete)

    def test_casing_variant_accepted(self):
        p = parser()
        events = feed(
            p,
            b"POST / HTTP/1.1\r\nHost: e.com\r\nTransfer-Encoding: Chunked\r\n\r\n"
            b"0\r\n\r\n",
        )
        assert isinstance(events[0], RequestHeadComplete)

    def test_extra_coding_rejected(self):
        p = parser()
        with pytest.raises(MalformedRequest):
            feed(
                p,
                b"POST / HTTP/1.1\r\nHost: e.com\r\n"
                b"Transfer-Encoding: gzip, chunked\r\n\r\n",
            )

    def test_chunked_not_last_rejected(self):
        p = parser()
        with pytest.raises(MalformedRequest):
            feed(
                p,
                b"POST / HTTP/1.1\r\nHost: e.com\r\n"
                b"Transfer-Encoding: chunked, gzip\r\n\r\n",
            )

    def test_internal_whitespace_rejected(self):
        p = parser()
        with pytest.raises(MalformedRequest):
            feed(
                p,
                b"POST / HTTP/1.1\r\nHost: e.com\r\n"
                b"Transfer-Encoding: chun ked\r\n\r\n",
            )

    def test_unrelated_encoding_rejected(self):
        p = parser()
        with pytest.raises(MalformedRequest):
            feed(
                p,
                b"POST / HTTP/1.1\r\nHost: e.com\r\n"
                b"Transfer-Encoding: identity\r\n\r\n",
            )


class TestHeaderFolding:
    def test_space_continuation_rejected(self):
        p = parser()
        with pytest.raises(MalformedRequest):
            feed(
                p,
                b"GET / HTTP/1.1\r\nHost: e.com\r\nX-Foo: bar\r\n baz\r\n\r\n",
            )

    def test_tab_continuation_rejected(self):
        p = parser()
        with pytest.raises(MalformedRequest):
            feed(
                p,
                b"GET / HTTP/1.1\r\nHost: e.com\r\nX-Foo: bar\r\n\tbaz\r\n\r\n",
            )


class TestMalformedRequestLine:
    def test_missing_sp_rejected(self):
        p = parser()
        with pytest.raises(MalformedRequest):
            feed(p, b"GET/HTTP/1.1\r\nHost: e.com\r\n\r\n")

    def test_extra_whitespace_rejected(self):
        p = parser()
        with pytest.raises(MalformedRequest):
            feed(p, b"GET  /  HTTP/1.1\r\nHost: e.com\r\n\r\n")

    def test_invalid_method_token_rejected(self):
        p = parser()
        with pytest.raises(MalformedRequest):
            feed(p, b"GE(T / HTTP/1.1\r\nHost: e.com\r\n\r\n")

    def test_unsupported_http_version_rejected(self):
        p = parser()
        with pytest.raises(MalformedRequest):
            feed(p, b"GET / HTTP/2.0\r\nHost: e.com\r\n\r\n")

    def test_malformed_version_token_rejected(self):
        p = parser()
        with pytest.raises(MalformedRequest):
            feed(p, b"GET / HTTP1.1\r\nHost: e.com\r\n\r\n")

    def test_control_character_in_request_line_rejected(self):
        p = parser()
        with pytest.raises(MalformedRequest):
            feed(p, b"GET /\x00 HTTP/1.1\r\nHost: e.com\r\n\r\n")

    def test_empty_request_line_rejected(self):
        p = parser()
        with pytest.raises(MalformedRequest):
            feed(p, b"\r\nHost: e.com\r\n\r\n")


class TestSizeLimits:
    def test_oversized_header_line_rejected_before_terminator(self):
        p = parser(max_header_size=32)
        with pytest.raises(RequestTooLarge):
            feed(p, b"GET / HTTP/1.1\r\n" + b"X-Foo: " + b"a" * 100)

    def test_cumulative_small_headers_exceed_limit(self):
        p = parser(max_header_size=64, max_headers=100)
        data = b"GET / HTTP/1.1\r\n"
        for i in range(20):
            data += f"X-H{i}: value\r\n".encode()
        with pytest.raises(RequestTooLarge):
            feed(p, data)

    def test_header_count_limit(self):
        p = parser(max_headers=3)
        data = b"GET / HTTP/1.1\r\n"
        for i in range(5):
            data += f"X-H{i}: v\r\n".encode()
        data += b"\r\n"
        with pytest.raises(RequestTooLarge):
            feed(p, data)

    def test_content_length_exceeding_max_body_size_rejected(self):
        p = parser(max_body_size=10)
        with pytest.raises(RequestTooLarge):
            feed(p, b"POST / HTTP/1.1\r\nHost: e.com\r\nContent-Length: 100\r\n\r\n")

    def test_chunked_body_exceeding_limit_rejected_at_chunk_size(self):
        p = parser(max_body_size=10)
        with pytest.raises(RequestTooLarge):
            feed(
                p,
                b"POST / HTTP/1.1\r\nHost: e.com\r\n"
                b"Transfer-Encoding: chunked\r\n\r\n"
                b"14\r\n",  # 0x14 == 20, over the 10-byte limit
            )

    def test_exact_boundary_accepted(self):
        # Cumulative header-size tracking counts the line plus its CRLF, so
        # the limit must account for that +2 to land exactly on the boundary.
        header_line = b"X-Foo: " + b"a" * 10  # 17 bytes
        limit = len(header_line) + 2
        p = parser(max_header_size=limit)
        events = feed(p, b"GET / HTTP/1.1\r\n" + header_line + b"\r\n\r\n")
        assert isinstance(events[0], RequestHeadComplete)

    def test_content_length_exactly_at_max_body_size_accepted(self):
        p = parser(max_body_size=5)
        events = feed(
            p,
            b"POST / HTTP/1.1\r\nHost: e.com\r\nContent-Length: 5\r\n\r\nhello",
        )
        assert isinstance(events[0], RequestHeadComplete)


class TestChunkedBody:
    def test_single_chunk_then_terminator(self):
        p = parser()
        events = feed(
            p,
            b"POST / HTTP/1.1\r\nHost: e.com\r\nTransfer-Encoding: chunked\r\n\r\n"
            b"5\r\nhello\r\n0\r\n\r\n",
        )
        body_chunks = [e for e in events if isinstance(e, BodyChunk)]
        assert body_chunks[0] == BodyChunk(b"hello", True)
        assert body_chunks[1] == BodyChunk(b"", False)
        assert isinstance(events[-1], RequestComplete)

    def test_multiple_chunks(self):
        p = parser()
        events = feed(
            p,
            b"POST / HTTP/1.1\r\nHost: e.com\r\nTransfer-Encoding: chunked\r\n\r\n"
            b"3\r\nfoo\r\n3\r\nbar\r\n0\r\n\r\n",
        )
        data = b"".join(e.data for e in events if isinstance(e, BodyChunk) and e.data)
        assert data == b"foobar"

    def test_chunk_extension_ignored(self):
        p = parser()
        events = feed(
            p,
            b"POST / HTTP/1.1\r\nHost: e.com\r\nTransfer-Encoding: chunked\r\n\r\n"
            b"5;foo=bar\r\nhello\r\n0\r\n\r\n",
        )
        body_chunks = [e for e in events if isinstance(e, BodyChunk)]
        assert body_chunks[0].data == b"hello"

    def test_malformed_chunk_size_rejected(self):
        p = parser()
        with pytest.raises(MalformedRequest):
            feed(
                p,
                b"POST / HTTP/1.1\r\nHost: e.com\r\nTransfer-Encoding: chunked\r\n\r\n"
                b"ZZ\r\nhello\r\n0\r\n\r\n",
            )

    def test_trailers_discarded(self):
        p = parser()
        events = feed(
            p,
            b"POST / HTTP/1.1\r\nHost: e.com\r\nTransfer-Encoding: chunked\r\n\r\n"
            b"0\r\nX-Trailer: value\r\n\r\n",
        )
        assert isinstance(events[-1], RequestComplete)


class TestHeaderEdgeCases:
    def test_header_names_case_insensitive(self):
        p = parser()
        events = feed(p, b"GET / HTTP/1.1\r\nContent-Type: text/plain\r\n\r\n")
        names = [name for name, _ in events[0].head.headers]
        assert b"content-type" in names
        assert b"Content-Type" not in names

    def test_trailing_whitespace_trimmed(self):
        p = parser()
        events = feed(p, b"GET / HTTP/1.1\r\nX-Foo: bar   \r\n\r\n")
        headers = dict(events[0].head.headers)
        assert headers[b"x-foo"] == b"bar"

    def test_empty_header_value_accepted(self):
        p = parser()
        events = feed(p, b"GET / HTTP/1.1\r\nX-Foo:\r\n\r\n")
        headers = dict(events[0].head.headers)
        assert headers[b"x-foo"] == b""

    def test_duplicate_non_content_length_headers_preserved(self):
        p = parser()
        events = feed(p, b"GET / HTTP/1.1\r\nX-Custom: a\r\nX-Custom: b\r\n\r\n")
        values = [v for name, v in events[0].head.headers if name == b"x-custom"]
        assert values == [b"a", b"b"]

    def test_missing_host_header_not_rejected(self):
        p = parser()
        events = feed(p, b"GET / HTTP/1.1\r\n\r\n")
        assert isinstance(events[0], RequestHeadComplete)


class TestConnectionLifecycle:
    def test_feed_eof_clean_between_requests(self):
        p = parser()
        feed(p, b"GET / HTTP/1.1\r\nHost: e.com\r\n\r\n")
        p.feed_eof()  # should not raise

    def test_feed_eof_mid_request_raises(self):
        p = parser()
        feed(p, b"GET / HTTP/1.1\r\n")
        with pytest.raises(MalformedRequest):
            p.feed_eof()

    def test_body_bearing_request_without_framing_rejected_via_next_parse(self):
        p = parser()
        # No Content-Length/Transfer-Encoding -> treated as a zero-length body.
        events = feed(p, b"POST / HTTP/1.1\r\nHost: e.com\r\n\r\n")
        complete_events = [e for e in events if isinstance(e, RequestComplete)]
        assert len(complete_events) == 1

        # Bytes the client intended as a body arrive next, but with no framing
        # declared the parser has already moved on to expecting a new request
        # line -- so they are rejected as a malformed request line rather than
        # silently accepted as a body.
        with pytest.raises(MalformedRequest):
            feed(p, b"not a valid request line\r\n\r\n")


HANDSHAKE = (
    b"GET /ws HTTP/1.1\r\n"
    b"Host: e.com\r\n"
    b"Upgrade: websocket\r\n"
    b"Connection: Upgrade\r\n"
    b"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
    b"Sec-WebSocket-Version: 13\r\n"
    b"\r\n"
)


class TestProtocolUpgrade:
    def test_upgrade_head_emits_only_the_head(self):
        p = parser()
        events = feed(p, HANDSHAKE)
        # No BodyChunk and no RequestComplete: HTTP framing has stopped, so
        # there is no "rest of this request" to describe.
        assert len(events) == 1
        assert isinstance(events[0], RequestHeadComplete)
        assert events[0].head.upgrade == "websocket"
        assert events[0].head.path == "/ws"
        assert p.upgraded is True

    def test_ordinary_request_has_no_upgrade(self):
        p = parser()
        events = feed(p, b"GET / HTTP/1.1\r\nHost: e.com\r\n\r\n")
        assert events[0].head.upgrade is None
        assert p.upgraded is False

    def test_take_buffer_returns_the_prologue(self):
        p = parser()
        # A client that sends its first frame in the same segment as the
        # handshake. Those bytes are inside the parser; without take_buffer()
        # they would be silently lost.
        events = feed(p, HANDSHAKE + b"\x81\x85frame")
        assert len(events) == 1
        assert p.take_buffer() == b"\x81\x85frame"
        assert p.take_buffer() == b""

    def test_take_buffer_before_upgrade_is_an_error(self):
        p = parser()
        feed(p, b"GET / HTTP/1.1\r\nHost: e.com\r\n\r\n")
        with pytest.raises(RuntimeError, match="only valid after an upgrade"):
            p.take_buffer()

    def test_bytes_fed_after_upgrade_are_buffered_not_parsed(self):
        p = parser()
        feed(p, HANDSHAKE)
        assert feed(p, b"\x89\x80mask") == []
        assert p.take_buffer() == b"\x89\x80mask"

    def test_feed_eof_after_upgrade_does_not_raise(self):
        p = parser()
        feed(p, HANDSHAKE)
        p.feed_eof()

    def test_pipelined_request_behind_an_upgrade_is_never_dispatched(self):
        # The smuggling case this state exists for. Without UPGRADED the
        # parser emits a second RequestHeadComplete for /admin, which the
        # bridge would dispatch as a genuine request -- on a connection the
        # client believes is an opaque tunnel.
        p = parser()
        smuggled = b"GET /admin HTTP/1.1\r\nHost: e.com\r\n\r\n"
        events = feed(p, HANDSHAKE + smuggled)
        heads = [e for e in events if isinstance(e, RequestHeadComplete)]
        assert len(heads) == 1
        assert heads[0].head.path == "/ws"
        # The smuggled request is inert: bytes, not an event.
        assert p.take_buffer() == smuggled

    def test_unoffered_protocol_parses_as_an_ordinary_request(self):
        # RFC 9110 section 7.8: a server that does not implement the offered
        # protocol ignores the Upgrade header and answers the request
        # normally. Only a protocol we actually switch to stops framing.
        p = parser()
        events = feed(
            p,
            b"GET / HTTP/1.1\r\nHost: e.com\r\n"
            b"Upgrade: h2c\r\nConnection: Upgrade\r\n\r\n",
        )
        assert events[0].head.upgrade is None
        assert isinstance(events[-1], RequestComplete)
        assert p.upgraded is False

    def test_upgrade_protocols_is_configurable(self):
        p = parser(upgrade_protocols=frozenset({"h2c"}))
        events = feed(
            p,
            b"GET / HTTP/1.1\r\nHost: e.com\r\n"
            b"Upgrade: h2c\r\nConnection: Upgrade\r\n\r\n",
        )
        assert events[0].head.upgrade == "h2c"

    def test_upgrade_protocols_empty_disables_upgrades(self):
        p = parser(upgrade_protocols=frozenset())
        events = feed(p, HANDSHAKE)
        assert events[0].head.upgrade is None
        assert isinstance(events[-1], RequestComplete)

    @pytest.mark.parametrize(
        "connection_value",
        [b"Upgrade", b"upgrade", b"keep-alive, Upgrade", b"Upgrade, keep-alive"],
    )
    def test_connection_header_is_tokenized(self, connection_value):
        p = parser()
        events = feed(
            p,
            b"GET /ws HTTP/1.1\r\nHost: e.com\r\nUpgrade: websocket\r\n"
            b"Connection: " + connection_value + b"\r\n\r\n",
        )
        assert events[0].head.upgrade == "websocket"

    def test_upgrade_without_connection_token_is_ignored(self):
        p = parser()
        events = feed(
            p,
            b"GET /ws HTTP/1.1\r\nHost: e.com\r\nUpgrade: websocket\r\n\r\n",
        )
        assert events[0].head.upgrade is None

    def test_connection_upgrade_without_upgrade_header_is_ignored(self):
        p = parser()
        events = feed(
            p,
            b"GET /ws HTTP/1.1\r\nHost: e.com\r\nConnection: Upgrade\r\n\r\n",
        )
        assert events[0].head.upgrade is None

    def test_multiple_upgrade_headers_are_ignored(self):
        p = parser()
        events = feed(
            p,
            b"GET /ws HTTP/1.1\r\nHost: e.com\r\nUpgrade: websocket\r\n"
            b"Upgrade: h2c\r\nConnection: Upgrade\r\n\r\n",
        )
        assert events[0].head.upgrade is None

    def test_upgrade_offer_list_is_ignored(self):
        p = parser()
        events = feed(
            p,
            b"GET /ws HTTP/1.1\r\nHost: e.com\r\nUpgrade: websocket, h2c\r\n"
            b"Connection: Upgrade\r\n\r\n",
        )
        assert events[0].head.upgrade is None

    @pytest.mark.parametrize("method", [b"POST", b"PUT", b"HEAD"])
    def test_upgrade_only_honoured_on_get(self, method):
        p = parser()
        events = feed(
            p,
            method + b" /ws HTTP/1.1\r\nHost: e.com\r\nUpgrade: websocket\r\n"
            b"Connection: Upgrade\r\nContent-Length: 0\r\n\r\n",
        )
        assert events[0].head.upgrade is None

    def test_upgrade_not_honoured_on_http_1_0(self):
        p = parser()
        events = feed(
            p,
            b"GET /ws HTTP/1.0\r\nHost: e.com\r\nUpgrade: websocket\r\n"
            b"Connection: Upgrade\r\n\r\n",
        )
        assert events[0].head.upgrade is None

    def test_upgrade_with_a_body_is_rejected(self):
        # Two readers would disagree about where the request ends: one sees
        # five body bytes, the other sees five bytes of the new protocol.
        p = parser()
        with pytest.raises(MalformedRequest, match="must not declare a body"):
            feed(
                p,
                b"GET /ws HTTP/1.1\r\nHost: e.com\r\nUpgrade: websocket\r\n"
                b"Connection: Upgrade\r\nContent-Length: 5\r\n\r\nhello",
            )

    def test_upgrade_with_chunked_body_is_rejected(self):
        p = parser()
        with pytest.raises(MalformedRequest, match="must not declare a body"):
            feed(
                p,
                b"GET /ws HTTP/1.1\r\nHost: e.com\r\nUpgrade: websocket\r\n"
                b"Connection: Upgrade\r\nTransfer-Encoding: chunked\r\n\r\n",
            )

    def test_upgrade_with_zero_content_length_is_allowed(self):
        # Unambiguous, and some clients send it.
        p = parser()
        events = feed(
            p,
            b"GET /ws HTTP/1.1\r\nHost: e.com\r\nUpgrade: websocket\r\n"
            b"Connection: Upgrade\r\nContent-Length: 0\r\n\r\n",
        )
        assert events[0].head.upgrade == "websocket"
        assert len(events) == 1

    def test_upgrade_split_across_feeds(self):
        p = parser()
        collected = []
        for index in range(len(HANDSHAKE)):
            collected.extend(feed(p, HANDSHAKE[index : index + 1]))
        assert len(collected) == 1
        assert collected[0].head.upgrade == "websocket"

    def test_request_before_an_upgrade_still_dispatches(self):
        # Pipelining a normal request ahead of the handshake is legal, and the
        # first request must still be answered.
        p = parser()
        events = feed(p, b"GET /a HTTP/1.1\r\nHost: e.com\r\n\r\n" + HANDSHAKE)
        heads = [e for e in events if isinstance(e, RequestHeadComplete)]
        assert [h.head.path for h in heads] == ["/a", "/ws"]
        assert [h.head.upgrade for h in heads] == [None, "websocket"]
