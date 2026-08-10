"""Onion composition and the exception layer."""

from __future__ import annotations

import pytest
from helpers import fake_receive, make_scope, make_send

from sonix.app.applications import Sonix
from sonix.app.exceptions import HTTPException
from sonix.app.middleware import ExceptionMiddleware, Middleware, build_stack
from sonix.app.requests import ClientDisconnect, Request
from sonix.app.responses import PlainTextResponse
from sonix.types import Message, Receive, Scope, Send


def recorder(name: str, calls: list[str]):
    """A middleware class that records entry and exit around the inner app."""

    class Recorder:
        def __init__(self, app, **options):
            self.app = app

        async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
            calls.append(f"enter:{name}")
            await self.app(scope, receive, send)
            calls.append(f"exit:{name}")

    return Recorder


async def ok_app(scope: Scope, receive: Receive, send: Send) -> None:
    await PlainTextResponse("ok")(scope, receive, send)


async def noop_handler(request: Request, exc: Exception) -> PlainTextResponse:
    return PlainTextResponse("x", status_code=400)


def raising_app(exc: BaseException):
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        raise exc

    return app


class TestOnionOrdering:
    async def test_first_registered_is_outermost(self):
        calls: list[str] = []
        stack = build_stack(
            ok_app,
            [
                Middleware(recorder("a", calls)),
                Middleware(recorder("b", calls)),
            ],
        )
        send, _sent = make_send()
        await stack(make_scope(), fake_receive, send)
        assert calls == ["enter:a", "enter:b", "exit:b", "exit:a"]

    async def test_add_middleware_keeps_the_same_rule(self):
        # Starlette's add_middleware is LIFO while its constructor list is
        # not; Sonix uses one rule for both.
        calls: list[str] = []
        app = Sonix()
        app.add_middleware(recorder("a", calls))
        app.add_middleware(recorder("b", calls))

        @app.get("/")
        def handler(request: Request):
            return "ok"

        send, _sent = make_send()
        await app(make_scope(), fake_receive, send)
        assert calls == ["enter:a", "enter:b", "exit:b", "exit:a"]

    async def test_middleware_options_are_passed_to_the_constructor(self):
        seen = {}

        class Configurable:
            def __init__(self, app, **options):
                self.app = app
                seen.update(options)

            async def __call__(self, scope, receive, send):
                await self.app(scope, receive, send)

        app = Sonix()
        app.add_middleware(Configurable, flavor="vanilla")

        @app.get("/")
        def handler(request: Request):
            return "ok"

        send, _sent = make_send()
        await app(make_scope(), fake_receive, send)
        assert seen == {"flavor": "vanilla"}


class TestExceptionConversion:
    async def test_http_exception_becomes_its_status(self):
        stack = build_stack(raising_app(HTTPException(404, "no such room")), [])
        send, sent = make_send()
        await stack(make_scope(), fake_receive, send)
        assert sent[0]["status"] == 404
        assert sent[1]["body"] == b"no such room"

    async def test_http_exception_headers_are_emitted(self):
        stack = build_stack(
            raising_app(HTTPException(405, headers={"allow": "GET, POST"})), []
        )
        send, sent = make_send()
        await stack(make_scope(), fake_receive, send)
        assert (b"allow", b"GET, POST") in sent[0]["headers"]

    async def test_unhandled_exception_becomes_500(self, capsys):
        stack = build_stack(raising_app(RuntimeError("boom")), [])
        send, sent = make_send()
        await stack(make_scope(), fake_receive, send)
        assert sent[0]["status"] == 500
        # The detail goes to the log, never to the client.
        assert b"boom" not in sent[1]["body"]
        assert "boom" in capsys.readouterr().err

    async def test_a_raising_handler_returns_500_not_a_dropped_connection(self):
        # The headline behavior change: previously this propagated out of the
        # app, and protocol.py closed the transport.
        app = Sonix()

        @app.get("/")
        def handler(request: Request):
            raise RuntimeError("boom")

        send, sent = make_send()
        await app(make_scope(), fake_receive, send)
        assert sent[0]["status"] == 500

    async def test_exception_raised_by_middleware_is_also_caught(self):
        # ExceptionMiddleware sits outermost precisely so this works.
        class Exploding:
            def __init__(self, app, **options):
                self.app = app

            async def __call__(self, scope, receive, send):
                raise HTTPException(503, "middleware said no")

        app = Sonix()
        app.add_middleware(Exploding)

        @app.get("/")
        def handler(request: Request):
            return "unreachable"

        send, sent = make_send()
        await app(make_scope(), fake_receive, send)
        assert sent[0]["status"] == 503

    async def test_client_disconnect_is_swallowed_not_turned_into_500(self):
        stack = build_stack(raising_app(ClientDisconnect()), [])
        send, sent = make_send()
        await stack(make_scope(), fake_receive, send)
        assert sent == [], "there is nobody left to answer"

    async def test_debug_reraises_unhandled_exceptions(self):
        stack = build_stack(raising_app(RuntimeError("boom")), [], debug=True)
        send, _sent = make_send()
        with pytest.raises(RuntimeError, match="boom"):
            await stack(make_scope(), fake_receive, send)

    async def test_debug_still_converts_http_exceptions(self):
        # debug is about surfacing bugs; an HTTPException is not a bug.
        stack = build_stack(raising_app(HTTPException(404)), [], debug=True)
        send, sent = make_send()
        await stack(make_scope(), fake_receive, send)
        assert sent[0]["status"] == 404


class TestPartiallySentResponse:
    async def test_exception_after_response_start_propagates(self):
        # Once the status line is on the wire there is no way to turn this
        # into an error response. Emitting a second http.response.start would
        # corrupt the stream, so the only honest option is to let the server
        # close the connection.
        async def app(scope: Scope, receive: Receive, send: Send) -> None:
            await send({"type": "http.response.start", "status": 200, "headers": []})
            raise RuntimeError("failed midway through streaming")

        stack = build_stack(app, [])
        send, sent = make_send()
        with pytest.raises(RuntimeError, match="midway"):
            await stack(make_scope(), fake_receive, send)
        assert len(sent) == 1
        assert sent[0]["type"] == "http.response.start"


class TestCustomHandlers:
    async def test_handler_registered_by_status_code(self):
        app = Sonix()

        @app.exception_handler(404)
        async def not_found(request: Request, exc: Exception):
            return PlainTextResponse("custom 404", status_code=404)

        @app.get("/")
        def handler(request: Request):
            raise HTTPException(404)

        send, sent = make_send()
        await app(make_scope(), fake_receive, send)
        assert sent[1]["body"] == b"custom 404"

    async def test_handler_registered_by_exception_class(self):
        class MyError(Exception):
            pass

        app = Sonix()

        @app.exception_handler(MyError)
        async def handle(request: Request, exc: Exception):
            return PlainTextResponse("handled", status_code=418)

        @app.get("/")
        def handler(request: Request):
            raise MyError

        send, sent = make_send()
        await app(make_scope(), fake_receive, send)
        assert sent[0]["status"] == 418

    async def test_subclass_resolves_to_the_base_class_handler(self):
        class Base(Exception):
            pass

        class Derived(Base):
            pass

        app = Sonix()

        @app.exception_handler(Base)
        async def handle(request: Request, exc: Exception):
            return PlainTextResponse("base", status_code=418)

        @app.get("/")
        def handler(request: Request):
            raise Derived

        send, sent = make_send()
        await app(make_scope(), fake_receive, send)
        assert sent[0]["status"] == 418

    async def test_status_handler_beats_the_generic_http_exception_handler(self):
        app = Sonix()

        @app.exception_handler(HTTPException)
        async def generic(request: Request, exc: Exception):
            return PlainTextResponse("generic", status_code=500)

        @app.exception_handler(404)
        async def specific(request: Request, exc: Exception):
            return PlainTextResponse("specific", status_code=404)

        @app.get("/")
        def handler(request: Request):
            raise HTTPException(404)

        send, sent = make_send()
        await app(make_scope(), fake_receive, send)
        assert sent[1]["body"] == b"specific"

    async def test_handler_receives_the_request_and_the_exception(self):
        seen = {}
        app = Sonix()

        @app.exception_handler(HTTPException)
        async def handle(request: Request, exc: Exception):
            seen["request"] = request
            seen["exc"] = exc
            return PlainTextResponse("x", status_code=400)

        @app.get("/")
        def handler(request: Request):
            raise HTTPException(404, "detail here")

        send, _sent = make_send()
        await app(make_scope(), fake_receive, send)
        assert isinstance(seen["request"], Request)
        assert isinstance(seen["exc"], HTTPException)
        assert seen["exc"].detail == "detail here"


class TestStackLifecycle:
    async def test_stack_is_built_once_and_reused(self):
        app = Sonix()

        @app.get("/")
        def handler(request: Request):
            return "ok"

        send, _sent = make_send()
        await app(make_scope(), fake_receive, send)
        first = app._stack
        await app(make_scope(), fake_receive, send)
        assert app._stack is first

    async def test_middleware_registered_after_start_is_refused(self):
        app = Sonix()

        @app.get("/")
        def handler(request: Request):
            return "ok"

        send, _sent = make_send()
        await app(make_scope(), fake_receive, send)

        # Silently having no effect would be worse than refusing.
        with pytest.raises(RuntimeError, match="after the app has started"):
            app.add_middleware(recorder("late", []))

    async def test_exception_handler_registered_after_start_is_refused(self):
        app = Sonix()

        @app.get("/")
        def handler(request: Request):
            return "ok"

        send, _sent = make_send()
        await app(make_scope(), fake_receive, send)
        with pytest.raises(RuntimeError, match="after the app has started"):
            app.add_exception_handler(404, noop_handler)

    async def test_constructor_middleware_and_handlers_are_honored(self):
        calls: list[str] = []
        app = Sonix(middleware=[Middleware(recorder("ctor", calls))])

        @app.get("/")
        def handler(request: Request):
            return "ok"

        send, _sent = make_send()
        await app(make_scope(), fake_receive, send)
        assert calls == ["enter:ctor", "exit:ctor"]


class TestScopeTypes:
    async def test_non_http_scope_is_passed_through_by_exception_middleware(self):
        seen = []

        async def app(scope: Scope, receive: Receive, send: Send) -> None:
            seen.append(scope["type"])

        await ExceptionMiddleware(app)(
            make_scope(type="websocket"), fake_receive, make_send()[0]
        )
        assert seen == ["websocket"]

    async def test_unknown_scope_type_names_the_problem(self):
        app = Sonix()
        # Not a bare KeyError from deep inside the router.
        with pytest.raises(RuntimeError, match="unsupported ASGI scope type"):
            await app(make_scope(type="lifespan"), fake_receive, make_send()[0])


class TestStreamingInterception:
    async def test_middleware_can_observe_every_body_message(self):
        # The reason onion wrapping is used instead of a before/after hook
        # list: a hook list cannot see a body arriving in several messages.
        observed: list[Message] = []

        class Observer:
            def __init__(self, app, **options):
                self.app = app

            async def __call__(self, scope, receive, send):
                async def wrapped(message):
                    observed.append(message)
                    await send(message)

                await self.app(scope, receive, wrapped)

        async def streaming(scope: Scope, receive: Receive, send: Send) -> None:
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"a", "more_body": True})
            await send({"type": "http.response.body", "body": b"b", "more_body": False})

        stack = build_stack(streaming, [Middleware(Observer)])
        send, _sent = make_send()
        await stack(make_scope(), fake_receive, send)
        assert [m["type"] for m in observed] == [
            "http.response.start",
            "http.response.body",
            "http.response.body",
        ]
