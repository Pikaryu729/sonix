import dataclasses
import json

import pytest

from sonix.app.responses import (
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
    Response,
)


def make_send():
    sent: list[dict] = []

    async def send(message):
        sent.append(message)

    return send, sent


FAKE_SCOPE: dict = {}


async def fake_receive():
    raise AssertionError("Response.__call__ should never call receive()")


class TestResponseCall:
    async def test_start_then_body_in_order(self):
        response = Response("hello")
        send, sent = make_send()
        await response(FAKE_SCOPE, fake_receive, send)

        assert len(sent) == 2
        assert sent[0]["type"] == "http.response.start"
        assert sent[0]["status"] == 200
        assert (b"content-length", b"5") in sent[0]["headers"]
        assert sent[1] == {
            "type": "http.response.body",
            "body": b"hello",
            "more_body": False,
        }

    async def test_none_content_renders_empty_body(self):
        response = Response()
        send, sent = make_send()
        await response(FAKE_SCOPE, fake_receive, send)
        assert sent[1]["body"] == b""

    async def test_bytes_content_passed_through(self):
        response = Response(b"raw-bytes")
        send, sent = make_send()
        await response(FAKE_SCOPE, fake_receive, send)
        assert sent[1]["body"] == b"raw-bytes"


class TestResponseRenderErrors:
    def test_non_str_bytes_content_raises_type_error(self):
        with pytest.raises(TypeError):
            Response(content=123)


class TestJSONResponse:
    async def test_dict_round_trips(self):
        response = JSONResponse({"a": 1, "b": [1, 2, 3]})
        send, sent = make_send()
        await response(FAKE_SCOPE, fake_receive, send)
        assert json.loads(sent[1]["body"]) == {"a": 1, "b": [1, 2, 3]}

    async def test_list_round_trips(self):
        response = JSONResponse([1, "two", 3.0])
        send, sent = make_send()
        await response(FAKE_SCOPE, fake_receive, send)
        assert json.loads(sent[1]["body"]) == [1, "two", 3.0]

    async def test_dataclass_round_trips(self):
        @dataclasses.dataclass
        class Item:
            name: str
            price: float

        response = JSONResponse(Item(name="widget", price=9.99))
        send, sent = make_send()
        await response(FAKE_SCOPE, fake_receive, send)
        assert json.loads(sent[1]["body"]) == {"name": "widget", "price": 9.99}

    async def test_content_type_default(self):
        response = JSONResponse({"a": 1})
        send, sent = make_send()
        await response(FAKE_SCOPE, fake_receive, send)
        assert (b"content-type", b"application/json; charset=utf-8") in sent[0][
            "headers"
        ]


class TestPlainTextResponse:
    async def test_content_type_default(self):
        response = PlainTextResponse("hi")
        send, sent = make_send()
        await response(FAKE_SCOPE, fake_receive, send)
        assert (b"content-type", b"text/plain; charset=utf-8") in sent[0]["headers"]


class TestHTMLResponse:
    async def test_content_type_default(self):
        response = HTMLResponse("<p>hi</p>")
        send, sent = make_send()
        await response(FAKE_SCOPE, fake_receive, send)
        assert (b"content-type", b"text/html; charset=utf-8") in sent[0]["headers"]


class TestHeaderOverride:
    async def test_caller_content_type_wins_and_is_not_modified(self):
        response = PlainTextResponse("x", headers={"content-type": "text/custom"})
        send, sent = make_send()
        await response(FAKE_SCOPE, fake_receive, send)
        assert (b"content-type", b"text/custom") in sent[0]["headers"]


class TestHeaderInputForms:
    async def test_dict_and_list_produce_identical_results(self):
        dict_response = Response("x", headers={"x-custom": "value"})
        list_response = Response("x", headers=[("x-custom", "value")])
        assert set(dict_response.headers) == set(list_response.headers)

    async def test_duplicate_header_names_are_preserved(self):
        # list[tuple[str, str]] is the shape used precisely to set the same
        # header name more than once (e.g. two Set-Cookie values) -- a dict
        # keyed by name would silently drop all but the last one.
        response = Response(
            "x",
            headers=[("set-cookie", "a=1"), ("set-cookie", "b=2")],
        )
        cookie_values = [
            value for name, value in response.headers if name == b"set-cookie"
        ]
        assert cookie_values == [b"a=1", b"b=2"]


class TestStatusCode:
    async def test_custom_status_code_passed_through(self):
        response = Response("created", status_code=201)
        send, sent = make_send()
        await response(FAKE_SCOPE, fake_receive, send)
        assert sent[0]["status"] == 201


class TestContentLengthAlwaysCorrect:
    async def test_bogus_caller_content_length_is_overridden(self):
        response = Response("hello", headers={"content-length": "999"})
        send, sent = make_send()
        await response(FAKE_SCOPE, fake_receive, send)
        assert (b"content-length", b"5") in sent[0]["headers"]
        assert not any(
            value == b"999"
            for name, value in sent[0]["headers"]
            if name == b"content-length"
        )


class TestBodylessStatusCodes:
    async def test_204_has_no_content_length_or_content_type(self):
        response = JSONResponse(None, status_code=204)
        send, sent = make_send()
        await response(FAKE_SCOPE, fake_receive, send)
        names = [name for name, _ in sent[0]["headers"]]
        assert b"content-length" not in names
        assert b"content-type" not in names

    async def test_304_has_no_content_length_or_content_type(self):
        response = Response(status_code=304)
        send, sent = make_send()
        await response(FAKE_SCOPE, fake_receive, send)
        names = [name for name, _ in sent[0]["headers"]]
        assert b"content-length" not in names
        assert b"content-type" not in names

    async def test_1xx_has_no_content_length_or_content_type(self):
        response = Response(status_code=103)
        send, sent = make_send()
        await response(FAKE_SCOPE, fake_receive, send)
        names = [name for name, _ in sent[0]["headers"]]
        assert b"content-length" not in names
        assert b"content-type" not in names

    async def test_200_is_unaffected(self):
        response = JSONResponse({"a": 1})
        send, sent = make_send()
        await response(FAKE_SCOPE, fake_receive, send)
        names = [name for name, _ in sent[0]["headers"]]
        assert b"content-length" in names
        assert b"content-type" in names
