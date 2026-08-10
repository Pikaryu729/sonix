import uuid

import pytest
from helpers import fake_receive, make_scope, make_send, make_ws_scope

from sonix.app.exceptions import HTTPException, WebSocketException
from sonix.app.responses import PlainTextResponse
from sonix.app.routing import CONVERTERS, RoutePathError, Router, compile_path


def ok_handler(marker=None, calls=None):
    async def handler(scope, receive, send):
        if calls is not None:
            calls.append(marker)
        await PlainTextResponse("ok")(scope, receive, send)

    return handler


class TestCompilePath:
    def test_static_template_no_params(self):
        pattern, converters = compile_path("/items")
        assert converters == {}
        assert pattern.fullmatch("/items")
        assert not pattern.fullmatch("/items/")
        assert not pattern.fullmatch("/other")

    def test_default_str_converter(self):
        pattern, converters = compile_path("/items/{name}")
        assert converters["name"] is CONVERTERS["str"]
        match = pattern.fullmatch("/items/widget")
        assert match is not None
        assert match.group("name") == "widget"

    def test_int_converter_compiles(self):
        pattern, converters = compile_path("/items/{id:int}")
        assert converters["id"] is CONVERTERS["int"]
        assert pattern.fullmatch("/items/42")

    def test_float_converter_compiles(self):
        pattern, converters = compile_path("/items/{price:float}")
        assert converters["price"] is CONVERTERS["float"]
        assert pattern.fullmatch("/items/9.99")

    def test_uuid_converter_compiles(self):
        pattern, converters = compile_path("/items/{token:uuid}")
        assert converters["token"] is CONVERTERS["uuid"]
        assert pattern.fullmatch("/items/12345678-1234-1234-1234-123456789abc")

    def test_path_converter_compiles(self):
        pattern, converters = compile_path("/static/{rest:path}")
        assert converters["rest"] is CONVERTERS["path"]
        assert pattern.fullmatch("/static/js/app.js")

    def test_duplicate_param_name_raises(self):
        with pytest.raises(RoutePathError):
            compile_path("/items/{id}/{id}")

    def test_unknown_converter_raises(self):
        with pytest.raises(RoutePathError):
            compile_path("/items/{id:bogus}")

    def test_path_converter_not_final_raises(self):
        with pytest.raises(RoutePathError):
            compile_path("/files/{rest:path}/extra")

    def test_non_numeric_segment_fails_to_match_not_coerce(self):
        pattern, _ = compile_path("/items/{id:int}")
        assert pattern.fullmatch("/items/abc") is None


class TestConverters:
    def test_int_convert(self):
        assert CONVERTERS["int"].convert("42") == 42

    def test_float_convert(self):
        assert CONVERTERS["float"].convert("3.5") == 3.5

    def test_uuid_convert(self):
        value = "12345678-1234-1234-1234-123456789abc"
        assert CONVERTERS["uuid"].convert(value) == uuid.UUID(value)

    def test_str_convert(self):
        assert CONVERTERS["str"].convert("x") == "x"

    def test_path_convert(self):
        assert CONVERTERS["path"].convert("a/b") == "a/b"


class TestAddRoute:
    def test_default_methods_is_get(self):
        router = Router()
        router.add_route("/items", ok_handler())
        assert router._routes[0].methods == ("GET",)

    def test_methods_normalized_to_uppercase(self):
        router = Router()
        router.add_route("/items", ok_handler(), methods=["get", "post"])
        assert router._routes[0].methods == ("GET", "POST")

    def test_duplicate_path_and_method_does_not_raise(self):
        router = Router()
        router.add_route("/items", ok_handler())
        router.add_route("/items", ok_handler())
        assert len(router._routes) == 2

    def test_explicit_empty_methods_list_raises(self):
        router = Router()
        with pytest.raises(ValueError, match="at least one HTTP method"):
            router.add_route("/items", ok_handler(), methods=[])


class TestRouterMatching:
    async def test_exact_literal_match_dispatches(self):
        router = Router()
        router.add_route("/items", ok_handler())
        send, sent = make_send()
        await router(make_scope(path="/items"), fake_receive, send)
        assert sent[0]["status"] == 200

    async def test_int_path_param_coerced(self):
        calls = []
        router = Router()

        async def handler(scope, receive, send):
            calls.append(scope["path_params"])
            await PlainTextResponse("ok")(scope, receive, send)

        router.add_route("/items/{id:int}", handler)
        send, _sent = make_send()
        await router(make_scope(path="/items/42"), fake_receive, send)
        assert calls == [{"id": 42}]
        assert isinstance(calls[0]["id"], int)

    async def test_combined_typed_params_coerce(self):
        calls = []

        async def handler(scope, receive, send):
            calls.append(scope["path_params"])
            await PlainTextResponse("ok")(scope, receive, send)

        router = Router()
        router.add_route("/a/{id:int}/{price:float}/{token:uuid}/{name}", handler)
        uid = "12345678-1234-1234-1234-123456789abc"
        send, _sent = make_send()
        await router(make_scope(path=f"/a/1/2.5/{uid}/bob"), fake_receive, send)
        assert calls == [
            {"id": 1, "price": 2.5, "token": uuid.UUID(uid), "name": "bob"}
        ]

    async def test_trailing_slash_is_strict(self):
        router = Router()
        router.add_route("/items", ok_handler())
        send, _sent = make_send()
        with pytest.raises(HTTPException) as excinfo:
            await router(make_scope(path="/items/"), fake_receive, send)
        assert excinfo.value.status_code == 404

    async def test_trailing_slash_is_strict_other_direction(self):
        router = Router()
        router.add_route("/items/", ok_handler())
        send, _sent = make_send()
        with pytest.raises(HTTPException) as excinfo:
            await router(make_scope(path="/items"), fake_receive, send)
        assert excinfo.value.status_code == 404

    async def test_registration_order_precedence(self):
        calls = []
        router = Router()
        router.add_route("/items", ok_handler(marker="first", calls=calls))
        router.add_route("/items", ok_handler(marker="second", calls=calls))
        send, _sent = make_send()
        await router(make_scope(path="/items"), fake_receive, send)
        assert calls == ["first"]

    async def test_path_catch_all_matches_multi_segment_tail(self):
        calls = []

        async def handler(scope, receive, send):
            calls.append(scope["path_params"])
            await PlainTextResponse("ok")(scope, receive, send)

        router = Router()
        router.add_route("/static/{rest:path}", handler)
        send, _sent = make_send()
        await router(make_scope(path="/static/js/app.js"), fake_receive, send)
        assert calls == [{"rest": "js/app.js"}]


class TestNotFound:
    async def test_no_matching_route_raises_404(self):
        # Raised, not returned: that is what makes a custom 404 a matter of
        # registering an exception handler. ExceptionMiddleware converts it.
        router = Router()
        router.add_route("/items", ok_handler())
        send, _sent = make_send()
        with pytest.raises(HTTPException) as excinfo:
            await router(make_scope(path="/nope"), fake_receive, send)
        assert excinfo.value.status_code == 404
        assert excinfo.value.headers is None


class TestMethodNotAllowed:
    async def test_single_route_wrong_method(self):
        router = Router()
        router.add_route("/items", ok_handler(), methods=["GET"])
        send, _sent = make_send()
        with pytest.raises(HTTPException) as excinfo:
            await router(make_scope(path="/items", method="POST"), fake_receive, send)
        assert excinfo.value.status_code == 405
        assert excinfo.value.headers == {"allow": "GET"}

    async def test_allow_header_is_scan_order_union(self):
        router = Router()
        router.add_route("/items", ok_handler(), methods=["GET"])
        router.add_route("/items", ok_handler(), methods=["POST"])
        send, _sent = make_send()
        with pytest.raises(HTTPException) as excinfo:
            await router(make_scope(path="/items", method="DELETE"), fake_receive, send)
        assert excinfo.value.status_code == 405
        assert excinfo.value.headers == {"allow": "GET, POST"}

    async def test_later_route_with_right_method_is_dispatched_not_405(self):
        calls = []
        router = Router()
        router.add_route(
            "/items", ok_handler(marker="get-handler", calls=calls), methods=["GET"]
        )
        router.add_route(
            "/items", ok_handler(marker="post-handler", calls=calls), methods=["POST"]
        )
        send, sent = make_send()
        await router(make_scope(path="/items", method="POST"), fake_receive, send)
        assert sent[0]["status"] == 200
        assert calls == ["post-handler"]


class TestPathParamsInScope:
    async def test_handler_sees_coerced_path_params_in_scope(self):
        seen_scopes = []

        async def handler(scope, receive, send):
            seen_scopes.append(scope)
            await PlainTextResponse("ok")(scope, receive, send)

        router = Router()
        router.add_route("/items/{id:int}", handler)
        send, _sent = make_send()
        await router(make_scope(path="/items/7"), fake_receive, send)
        assert seen_scopes[0]["path_params"] == {"id": 7}


class TestWebSocketRouting:
    async def test_websocket_route_matches_and_binds_path_params(self):
        seen = {}

        async def handler(scope, receive, send):
            seen.update(scope["path_params"])

        router = Router()
        router.add_websocket_route("/rooms/{room_id:int}", handler)
        await router(make_ws_scope(path="/rooms/7"), fake_receive, make_send()[0])
        assert seen == {"room_id": 7}

    async def test_http_and_websocket_routes_share_a_path(self):
        called = []

        async def http_handler(scope, receive, send):
            called.append("http")

        async def ws_handler(scope, receive, send):
            called.append("ws")

        router = Router()
        router.add_route("/x", http_handler)
        router.add_websocket_route("/x", ws_handler)

        send, _ = make_send()
        await router(make_scope(path="/x"), fake_receive, send)
        await router(make_ws_scope(path="/x"), fake_receive, send)
        assert called == ["http", "ws"]

    async def test_http_request_to_a_websocket_only_path_is_404_not_405(self):
        # A 405 here would carry an empty Allow header, which is worse than
        # useless -- it claims the path exists over HTTP with no usable method.
        async def ws_handler(scope, receive, send):
            raise AssertionError("must not be dispatched")

        router = Router()
        router.add_websocket_route("/ws", ws_handler)
        with pytest.raises(HTTPException) as excinfo:
            await router(make_scope(path="/ws"), fake_receive, make_send()[0])
        assert excinfo.value.status_code == 404

    async def test_websocket_to_an_http_only_path_raises_websocket_exception(self):
        async def http_handler(scope, receive, send):
            raise AssertionError("must not be dispatched")

        router = Router()
        router.add_route("/x", http_handler)
        with pytest.raises(WebSocketException) as excinfo:
            await router(make_ws_scope(path="/x"), fake_receive, make_send()[0])
        assert excinfo.value.code == 1000

    async def test_unmatched_websocket_path(self):
        router = Router()
        with pytest.raises(WebSocketException, match="no websocket route"):
            await router(make_ws_scope(path="/nope"), fake_receive, make_send()[0])

    async def test_websocket_routes_have_no_methods(self):
        async def ws_handler(scope, receive, send):
            pass

        router = Router()
        router.add_websocket_route("/ws", ws_handler)
        assert router._routes[0].methods == ()
        assert router._routes[0].scope_type == "websocket"

    async def test_http_routes_default_to_the_http_scope_type(self):
        async def handler(scope, receive, send):
            pass

        router = Router()
        router.add_route("/x", handler)
        assert router._routes[0].scope_type == "http"
