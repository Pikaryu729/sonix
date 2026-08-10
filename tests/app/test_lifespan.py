"""ASGI lifespan on the app side."""

from __future__ import annotations

import contextlib

import pytest

from sonix.app.applications import Sonix
from sonix.app.lifespan import LifespanHandler
from sonix.types import Message


def lifespan_driver(messages: list[str]):
    """A receive()/send() pair driving the lifespan protocol by hand."""
    incoming = iter(messages)
    sent: list[Message] = []

    async def receive() -> Message:
        return {"type": next(incoming)}

    async def send(message: Message) -> None:
        sent.append(message)

    return receive, send, sent


def lifespan_scope() -> dict:
    return {
        "type": "lifespan",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "state": {},
    }


class TestNoLifespanRegistered:
    async def test_startup_and_shutdown_both_complete(self):
        app = Sonix()
        receive, send, sent = lifespan_driver(["lifespan.startup", "lifespan.shutdown"])
        await app(lifespan_scope(), receive, send)
        assert [m["type"] for m in sent] == [
            "lifespan.startup.complete",
            "lifespan.shutdown.complete",
        ]

    async def test_lifespan_scope_no_longer_raises_keyerror(self):
        # The regression this whole module exists for: Sonix.__call__ used to
        # delegate unconditionally to the router, which read scope["path"].
        app = Sonix()
        receive, send, _sent = lifespan_driver(
            ["lifespan.startup", "lifespan.shutdown"]
        )
        await app(lifespan_scope(), receive, send)


class TestContextManagerForm:
    async def test_setup_and_teardown_run_in_order(self):
        events = []

        @contextlib.asynccontextmanager
        async def lifespan(app):
            events.append("up")
            yield
            events.append("down")

        app = Sonix(lifespan=lifespan)
        receive, send, _sent = lifespan_driver(
            ["lifespan.startup", "lifespan.shutdown"]
        )
        await app(lifespan_scope(), receive, send)
        assert events == ["up", "down"]

    async def test_teardown_does_not_run_before_shutdown_is_requested(self):
        events = []

        @contextlib.asynccontextmanager
        async def lifespan(app):
            events.append("up")
            yield
            events.append("down")

        app = Sonix(lifespan=lifespan)
        # Only send startup; never send shutdown. The handler must block on
        # receive() rather than tearing down eagerly.
        messages = iter(["lifespan.startup"])
        sent: list[Message] = []

        async def receive() -> Message:
            try:
                return {"type": next(messages)}
            except StopIteration:
                raise _StopDriving from None

        async def send(message: Message) -> None:
            sent.append(message)

        with pytest.raises(_StopDriving):
            await app(lifespan_scope(), receive, send)
        assert events == ["up"]

    async def test_yielded_mapping_lands_in_scope_state(self):
        sentinel = object()

        @contextlib.asynccontextmanager
        async def lifespan(app):
            yield {"db": sentinel}

        app = Sonix(lifespan=lifespan)
        scope = lifespan_scope()
        receive, send, _sent = lifespan_driver(
            ["lifespan.startup", "lifespan.shutdown"]
        )
        await app(scope, receive, send)
        assert scope["state"]["db"] is sentinel

    async def test_state_dict_is_updated_in_place_not_replaced(self):
        # The server holds a reference to this dict and copies it into every
        # request scope; replacing it would silently orphan the server's copy.
        @contextlib.asynccontextmanager
        async def lifespan(app):
            yield {"added": 1}

        app = Sonix(lifespan=lifespan)
        scope = lifespan_scope()
        original = scope["state"]
        receive, send, _sent = lifespan_driver(
            ["lifespan.startup", "lifespan.shutdown"]
        )
        await app(scope, receive, send)
        assert scope["state"] is original

    async def test_yielding_nothing_is_fine(self):
        @contextlib.asynccontextmanager
        async def lifespan(app):
            yield

        app = Sonix(lifespan=lifespan)
        scope = lifespan_scope()
        receive, send, sent = lifespan_driver(["lifespan.startup", "lifespan.shutdown"])
        await app(scope, receive, send)
        assert sent[0]["type"] == "lifespan.startup.complete"

    async def test_the_app_is_passed_to_the_factory(self):
        seen = []

        @contextlib.asynccontextmanager
        async def lifespan(app):
            seen.append(app)
            yield

        app = Sonix(lifespan=lifespan)
        receive, send, _sent = lifespan_driver(
            ["lifespan.startup", "lifespan.shutdown"]
        )
        await app(lifespan_scope(), receive, send)
        assert seen == [app]


class TestFailures:
    async def test_startup_failure_is_reported_as_a_message_not_raised(self):
        # A server that asked for lifespan is waiting for a reply; raising
        # here would leave it waiting forever.
        @contextlib.asynccontextmanager
        async def lifespan(app):
            raise RuntimeError("database is down")
            yield  # pragma: no cover

        app = Sonix(lifespan=lifespan)
        receive, send, sent = lifespan_driver(["lifespan.startup"])
        await app(lifespan_scope(), receive, send)
        assert sent[0]["type"] == "lifespan.startup.failed"
        assert "database is down" in sent[0]["message"]

    async def test_shutdown_failure_is_reported_as_a_message(self):
        @contextlib.asynccontextmanager
        async def lifespan(app):
            yield
            raise RuntimeError("could not flush")

        app = Sonix(lifespan=lifespan)
        receive, send, sent = lifespan_driver(["lifespan.startup", "lifespan.shutdown"])
        await app(lifespan_scope(), receive, send)
        assert sent[0]["type"] == "lifespan.startup.complete"
        assert sent[1]["type"] == "lifespan.shutdown.failed"
        assert "could not flush" in sent[1]["message"]

    async def test_unexpected_message_type_is_rejected(self):
        app = Sonix()
        receive, send, _sent = lifespan_driver(["lifespan.shutdown"])
        with pytest.raises(RuntimeError, match="expected ASGI message"):
            await app(lifespan_scope(), receive, send)


class TestEventSugar:
    async def test_on_startup_and_on_shutdown_run(self):
        events = []
        app = Sonix()

        @app.on_startup
        def up():
            events.append("up")

        @app.on_shutdown
        async def down():
            events.append("down")

        receive, send, _sent = lifespan_driver(
            ["lifespan.startup", "lifespan.shutdown"]
        )
        await app(lifespan_scope(), receive, send)
        assert events == ["up", "down"]

    async def test_hooks_run_in_registration_order(self):
        events = []
        app = Sonix()
        app.on_startup(lambda: events.append("first"))
        app.on_startup(lambda: events.append("second"))

        receive, send, _sent = lifespan_driver(
            ["lifespan.startup", "lifespan.shutdown"]
        )
        await app(lifespan_scope(), receive, send)
        assert events == ["first", "second"]

    async def test_mixing_the_two_forms_is_refused(self):
        @contextlib.asynccontextmanager
        async def lifespan(app):
            yield

        app = Sonix(lifespan=lifespan)
        app.on_startup(lambda: None)

        receive, send, _sent = lifespan_driver(["lifespan.startup"])
        with pytest.raises(RuntimeError, match="not both"):
            await app(lifespan_scope(), receive, send)


class TestHandlerDirectly:
    async def test_handler_is_usable_without_a_sonix_app(self):
        handler = LifespanHandler(object())
        receive, send, sent = lifespan_driver(["lifespan.startup", "lifespan.shutdown"])
        await handler({"type": "lifespan", "state": {}}, receive, send)
        assert len(sent) == 2


class _StopDriving(Exception):
    """Signals that the test driver has run out of scripted messages."""
