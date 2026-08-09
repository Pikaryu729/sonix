import dataclasses

from sonix.app.applications import Sonix
from sonix.app.requests import Request
from sonix.app.responses import PlainTextResponse


def make_scope(**overrides):
    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/",
        "raw_path": b"/",
        "query_string": b"",
        "root_path": "",
        "headers": [(b"host", b"example.com")],
        "client": ("127.0.0.1", 54321),
        "server": ("127.0.0.1", 8000),
    }
    scope.update(overrides)
    return scope


async def fake_receive():
    raise AssertionError("handler should never call receive() in these tests")


def make_send():
    sent: list[dict] = []

    async def send(message):
        sent.append(message)

    return send, sent


class TestRegistration:
    def test_get_decorator_returns_handler_unmodified(self):
        app = Sonix()

        @app.get("/")
        def handler(request: Request):
            return {"ok": True}

        assert handler.__name__ == "handler"
        assert callable(handler)

    def test_get_registers_a_get_only_route(self):
        app = Sonix()

        @app.get("/items")
        def handler(request: Request):
            return {}

        assert app.router._routes[0].methods == ("GET",)

    def test_post_registers_a_post_only_route(self):
        app = Sonix()

        @app.post("/items")
        def handler(request: Request):
            return {}

        assert app.router._routes[0].methods == ("POST",)


class TestDispatch:
    async def test_async_handler_result_is_sent(self):
        app = Sonix()

        @app.get("/")
        async def handler(request: Request):
            return {"message": "hi"}

        send, sent = make_send()
        await app(make_scope(), fake_receive, send)
        assert sent[0]["status"] == 200
        assert b'"message"' in sent[1]["body"]

    async def test_sync_handler_runs_via_to_thread(self):
        app = Sonix()

        @app.get("/")
        def handler(request: Request):
            return {"message": "hi"}

        send, sent = make_send()
        await app(make_scope(), fake_receive, send)
        assert sent[0]["status"] == 200
        assert b'"message"' in sent[1]["body"]

    async def test_handler_receives_a_request_object(self):
        seen = []
        app = Sonix()

        @app.get("/")
        def handler(request: Request):
            seen.append(request)

        send, _sent = make_send()
        await app(make_scope(), fake_receive, send)
        assert len(seen) == 1
        assert isinstance(seen[0], Request)

    async def test_async_callable_object_handler_is_awaited(self):
        # A plain `async def` function is the common case, but a callable
        # *object* with an async __call__ (class-based view, a decorator
        # wrapping a function in one, etc.) must also be detected as async
        # -- inspect.iscoroutinefunction(handler) alone returns False for
        # these, which would previously route it through
        # asyncio.to_thread(handler, request), producing an unawaited,
        # never-run coroutine instead of a result.
        class AsyncHandler:
            async def __call__(self, request: Request):
                return {"message": "hi"}

        app = Sonix()
        app.add_route("/", AsyncHandler())
        send, sent = make_send()
        await app(make_scope(), fake_receive, send)
        assert sent[0]["status"] == 200
        assert b'"message"' in sent[1]["body"]

    async def test_unmatched_route_delegates_to_router_404(self):
        app = Sonix()
        send, sent = make_send()
        await app(make_scope(path="/nope"), fake_receive, send)
        assert sent[0]["status"] == 404


class TestResponseCoercion:
    async def test_response_instance_used_directly(self):
        app = Sonix()

        @app.get("/")
        def handler(request: Request):
            return PlainTextResponse("custom", status_code=201)

        send, sent = make_send()
        await app(make_scope(), fake_receive, send)
        assert sent[0]["status"] == 201
        assert sent[1]["body"] == b"custom"

    async def test_dict_becomes_json_response(self):
        app = Sonix()

        @app.get("/")
        def handler(request: Request):
            return {"a": 1}

        send, sent = make_send()
        await app(make_scope(), fake_receive, send)
        assert (b"content-type", b"application/json; charset=utf-8") in sent[0][
            "headers"
        ]

    async def test_list_becomes_json_response(self):
        app = Sonix()

        @app.get("/")
        def handler(request: Request):
            return [1, 2, 3]

        send, sent = make_send()
        await app(make_scope(), fake_receive, send)
        assert sent[1]["body"] == b"[1,2,3]"

    async def test_dataclass_becomes_json_response(self):
        @dataclasses.dataclass
        class Item:
            name: str

        app = Sonix()

        @app.get("/")
        def handler(request: Request):
            return Item(name="widget")

        send, sent = make_send()
        await app(make_scope(), fake_receive, send)
        assert sent[1]["body"] == b'{"name":"widget"}'

    async def test_str_becomes_plain_text_response(self):
        app = Sonix()

        @app.get("/")
        def handler(request: Request):
            return "hello"

        send, sent = make_send()
        await app(make_scope(), fake_receive, send)
        assert (b"content-type", b"text/plain; charset=utf-8") in sent[0]["headers"]
        assert sent[1]["body"] == b"hello"

    async def test_none_becomes_204(self):
        app = Sonix()

        @app.get("/")
        def handler(request: Request):
            return None

        send, sent = make_send()
        await app(make_scope(), fake_receive, send)
        assert sent[0]["status"] == 204


class TestPathParams:
    async def test_handler_reads_coerced_path_param(self):
        seen = []

        app = Sonix()

        @app.get("/items/{item_id:int}")
        def handler(request: Request):
            seen.append(request.path_params["item_id"])
            return {"item_id": request.path_params["item_id"]}

        send, sent = make_send()
        await app(make_scope(path="/items/42"), fake_receive, send)
        assert seen == [42]
        assert sent[0]["status"] == 200
