import pytest

from sonix.app.requests import ClientDisconnect, Request


def make_scope(**overrides):
    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/items/42",
        "raw_path": b"/items/42",
        "query_string": b"",
        "root_path": "",
        "headers": [(b"host", b"example.com")],
        "client": ("127.0.0.1", 54321),
        "server": ("127.0.0.1", 8000),
    }
    scope.update(overrides)
    return scope


def make_receive(messages):
    it = iter(messages)

    async def receive():
        return next(it)

    return receive


class TestHeaders:
    def test_case_insensitive_lookup(self):
        request = Request(
            make_scope(headers=[(b"content-type", b"application/json")]),
            make_receive([]),
        )
        assert request.headers.get("Content-Type") == "application/json"
        assert request.headers.get("CONTENT-TYPE") == "application/json"

    def test_missing_header_returns_default(self):
        request = Request(make_scope(headers=[]), make_receive([]))
        assert request.headers.get("x-missing") is None
        assert request.headers.get("x-missing", "fallback") == "fallback"

    def test_contains(self):
        request = Request(make_scope(headers=[(b"host", b"e.com")]), make_receive([]))
        assert "host" in request.headers
        assert "Host" in request.headers
        assert "x-other" not in request.headers

    def test_duplicate_names_first_wins(self):
        request = Request(
            make_scope(headers=[(b"x-custom", b"a"), (b"x-custom", b"b")]),
            make_receive([]),
        )
        assert request.headers.get("x-custom") == "a"

    def test_iterating_yields_str_names_usable_for_lookup(self):
        request = Request(
            make_scope(headers=[(b"host", b"e.com"), (b"x-custom", b"a")]),
            make_receive([]),
        )
        names = list(request.headers)
        assert names == ["host", "x-custom"]
        for name in names:
            assert isinstance(name, str)
            assert request.headers.get(name) is not None


class TestQueryParams:
    def test_single_value(self):
        request = Request(make_scope(query_string=b"q=hello"), make_receive([]))
        assert request.query_params.get("q") == "hello"

    def test_multi_value(self):
        request = Request(make_scope(query_string=b"tag=a&tag=b"), make_receive([]))
        assert request.query_params.get_list("tag") == ["a", "b"]
        assert request.query_params.get("tag") == "a"

    def test_empty_query_string(self):
        request = Request(make_scope(query_string=b""), make_receive([]))
        assert len(request.query_params) == 0
        assert request.query_params.get("q") is None

    def test_percent_encoded_value_decodes(self):
        request = Request(make_scope(query_string=b"q=a%20b"), make_receive([]))
        assert request.query_params.get("q") == "a b"

    def test_blank_value_kept(self):
        request = Request(make_scope(query_string=b"flag="), make_receive([]))
        assert "flag" in request.query_params
        assert request.query_params.get("flag") == ""


class TestBasicProperties:
    def test_passthrough_properties(self):
        scope = make_scope(
            method="POST",
            path="/a/b",
            raw_path=b"/a/b",
            query_string=b"x=1",
            http_version="1.0",
            scheme="http",
            root_path="",
            client=("10.0.0.1", 1234),
            server=("10.0.0.2", 8080),
        )
        request = Request(scope, make_receive([]))
        assert request.scope is scope
        assert request.method == "POST"
        assert request.path == "/a/b"
        assert request.raw_path == b"/a/b"
        assert request.query_string == b"x=1"
        assert request.http_version == "1.0"
        assert request.scheme == "http"
        assert request.root_path == ""
        assert request.client == ("10.0.0.1", 1234)
        assert request.server == ("10.0.0.2", 8080)


class TestPathParams:
    def test_defaults_to_empty_dict_when_absent(self):
        request = Request(make_scope(), make_receive([]))
        assert request.path_params == {}

    def test_reads_router_populated_value(self):
        scope = make_scope(path_params={"id": 42})
        request = Request(scope, make_receive([]))
        assert request.path_params == {"id": 42}


class TestBody:
    async def test_single_chunk_body(self):
        receive = make_receive(
            [{"type": "http.request", "body": b"hello", "more_body": False}]
        )
        request = Request(make_scope(), receive)
        assert await request.body() == b"hello"

    async def test_multi_chunk_body_concatenates_in_order(self):
        receive = make_receive(
            [
                {"type": "http.request", "body": b"ab", "more_body": True},
                {"type": "http.request", "body": b"cd", "more_body": True},
                {"type": "http.request", "body": b"ef", "more_body": False},
            ]
        )
        request = Request(make_scope(), receive)
        assert await request.body() == b"abcdef"

    async def test_body_is_cached_and_does_not_redrain(self):
        calls = 0

        async def receive():
            nonlocal calls
            calls += 1
            if calls > 1:
                raise AssertionError("receive() called more than once")
            return {"type": "http.request", "body": b"hello", "more_body": False}

        request = Request(make_scope(), receive)
        first = await request.body()
        second = await request.body()
        assert first == second == b"hello"
        assert calls == 1

    async def test_disconnect_mid_stream_raises(self):
        receive = make_receive(
            [
                {"type": "http.request", "body": b"partial", "more_body": True},
                {"type": "http.disconnect"},
            ]
        )
        request = Request(make_scope(), receive)
        with pytest.raises(ClientDisconnect):
            await request.body()

    async def test_disconnect_on_first_receive_raises(self):
        receive = make_receive([{"type": "http.disconnect"}])
        request = Request(make_scope(), receive)
        with pytest.raises(ClientDisconnect):
            await request.body()

    async def test_stream_called_twice_raises(self):
        receive = make_receive(
            [{"type": "http.request", "body": b"x", "more_body": False}]
        )
        request = Request(make_scope(), receive)
        async for _ in request.stream():
            pass
        with pytest.raises(RuntimeError):
            async for _ in request.stream():
                pass

    async def test_stream_after_body_cached_replays_without_receive(self):
        receive = make_receive(
            [{"type": "http.request", "body": b"hello", "more_body": False}]
        )
        request = Request(make_scope(), receive)
        await request.body()
        chunks = [chunk async for chunk in request.stream()]
        assert chunks == [b"hello"]

    async def test_empty_body_cached_replay_yields_no_chunks(self):
        # Matches the live-streaming path's contract of never yielding an
        # empty chunk, so a body-less request (e.g. GET) behaves the same
        # whether stream() is consumed before or after body() cached it.
        receive = make_receive(
            [{"type": "http.request", "body": b"", "more_body": False}]
        )
        request = Request(make_scope(), receive)
        await request.body()
        chunks = [chunk async for chunk in request.stream()]
        assert chunks == []


class TestJson:
    async def test_json_round_trips(self):
        receive = make_receive(
            [{"type": "http.request", "body": b'{"a": 1}', "more_body": False}]
        )
        request = Request(make_scope(), receive)
        assert await request.json() == {"a": 1}
