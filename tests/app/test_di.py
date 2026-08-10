"""Dependency injection: plan building, coercion, caching, teardown.

Deliberately WITHOUT `from __future__ import annotations`, unlike every other
module here. Under PEP 563 an annotation is a string that get_type_hints must
evaluate against the defining module's globals -- so `Annotated[X,
Depends(f)]` where `f` is defined *inside a test function* cannot be resolved,
because `f` is a local and lives nowhere get_type_hints can see. Module-level
dependencies, which is what real applications write, resolve fine either way
(and `test_module_level_dependency_resolves_under_pep563` pins that). Without
the future import, annotations evaluate eagerly at def time and locals work.
"""

import asyncio
import contextlib
import uuid
from typing import Annotated

import pytest
from helpers import fake_receive, make_scope, make_send

from sonix.app.applications import Sonix
from sonix.app.di import (
    SCALAR_PARSERS,
    DependencyResolutionError,
    Depends,
    build_plan,
    resolve,
)
from sonix.app.exceptions import HTTPException
from sonix.app.requests import Request
from sonix.app.routing import CONVERTERS, compile_path


def plan_for(handler, path: str = "/"):
    _pattern, converters = compile_path(path)
    return build_plan(handler, path_converters=converters)


async def run(app, **scope_kwargs):
    """Drive an app once and return its (start, body) messages."""
    send, sent = make_send()
    await app(make_scope(**scope_kwargs), fake_receive, send)
    return sent


def body_of(sent):
    return sent[1]["body"]


class TestPlanShape:
    def test_single_request_parameter_is_trivial(self):
        def handler(request: Request): ...

        assert plan_for(handler).is_trivial

    def test_unannotated_request_is_also_trivial(self):
        # Handlers written before DI existed say exactly this.
        def handler(request): ...

        plan = plan_for(handler)
        assert plan.is_trivial
        assert plan.params[0].source == "request"

    def test_request_is_matched_by_annotation_not_name(self):
        def handler(req: Request): ...

        assert plan_for(handler).params[0].source == "request"

    def test_path_parameter_is_classified_as_path(self):
        def handler(item_id: int): ...

        plan = plan_for(handler, "/items/{item_id:int}")
        assert plan.params[0].source == "path"

    def test_query_parameter_is_classified_as_query(self):
        def handler(limit: int = 10): ...

        assert plan_for(handler).params[0].source == "query"

    def test_no_teardown_without_generator_dependencies(self):
        def dep() -> int:
            return 1

        def handler(value: Annotated[int, Depends(dep)]): ...

        assert plan_for(handler).needs_teardown is False

    def test_generator_dependency_sets_needs_teardown(self):
        def dep():
            yield 1

        def handler(value: Annotated[int, Depends(dep)]): ...

        assert plan_for(handler).needs_teardown is True

    def test_nested_generator_dependency_propagates_needs_teardown(self):
        def inner():
            yield 1

        def outer(value: Annotated[int, Depends(inner)]) -> int:
            return value

        def handler(value: Annotated[int, Depends(outer)]): ...

        assert plan_for(handler).needs_teardown is True

    def test_callable_object_annotations_are_visible(self):
        # get_type_hints(instance) inspects the CLASS's attribute annotations
        # and returns nothing for __call__'s parameters, so a naive
        # implementation makes every annotation on a class-based handler
        # silently invisible.
        class Handler:
            async def __call__(self, limit: int = 5): ...

        plan = plan_for(Handler())
        assert plan.params[0].source == "query"
        assert plan.params[0].query.type_name == "int"


class TestDecorationTimeErrors:
    def test_unsupported_annotation_names_handler_and_parameter(self):
        class Connection: ...

        def handler(db: Connection): ...

        with pytest.raises(DependencyResolutionError) as excinfo:
            plan_for(handler)
        assert "handler" in str(excinfo.value)
        assert "db" in str(excinfo.value)

    def test_unannotated_without_default_is_refused(self):
        def handler(mystery): ...

        with pytest.raises(DependencyResolutionError, match="mystery"):
            plan_for(handler)

    def test_path_annotation_disagreeing_with_template_is_refused(self):
        def handler(item_id: str): ...

        with pytest.raises(DependencyResolutionError, match="disagrees"):
            plan_for(handler, "/items/{item_id:int}")

    def test_path_annotation_disagreement_is_refused_both_directions(self):
        # /items/{name} converts to str; annotating int is equally wrong.
        def handler(name: int): ...

        with pytest.raises(DependencyResolutionError, match="disagrees"):
            plan_for(handler, "/items/{name}")

    def test_matching_path_annotation_is_accepted(self):
        def handler(item_id: int): ...

        assert plan_for(handler, "/items/{item_id:int}").params[0].source == "path"

    def test_var_args_refused(self):
        def handler(*args): ...

        with pytest.raises(DependencyResolutionError, match="variadic"):
            plan_for(handler)

    def test_var_kwargs_refused(self):
        def handler(**kwargs): ...

        with pytest.raises(DependencyResolutionError, match="variadic"):
            plan_for(handler)

    def test_positional_only_refused(self):
        # Handlers are called with **kwargs, so this could never be filled.
        def handler(request: Request, /): ...

        with pytest.raises(DependencyResolutionError, match="positional-only"):
            plan_for(handler)

    def test_two_depends_in_one_annotated_refused(self):
        def a() -> int:
            return 1

        def b() -> int:
            return 2

        def handler(value: Annotated[int, Depends(a), Depends(b)]): ...

        with pytest.raises(DependencyResolutionError, match="more than one"):
            plan_for(handler)

    def test_depends_in_both_annotated_and_default_refused(self):
        def a() -> int:
            return 1

        def b() -> int:
            return 2

        def handler(
            # Deliberately bad on both axes: two Depends, and the default-value
            # form's usual type unsoundness.
            value: Annotated[int, Depends(a)] = Depends(b),  # ty: ignore[invalid-parameter-default]
        ): ...

        with pytest.raises(DependencyResolutionError, match="one or the other"):
            plan_for(handler)

    def test_circular_dependency_refused(self):
        def a(value: Annotated[int, Depends(lambda: 1)]) -> int:
            return value

        # Build a genuine cycle by rebinding after definition.
        def b(value: Annotated[int, Depends(a)]) -> int:
            return value

        a.__annotations__["value"] = Annotated[int, Depends(b)]

        def handler(value: Annotated[int, Depends(a)]): ...

        with pytest.raises(DependencyResolutionError, match="circular"):
            plan_for(handler)

    def test_path_parameter_colliding_with_request_refused(self):
        def handler(request: Request): ...

        with pytest.raises(DependencyResolutionError, match="Rename one"):
            plan_for(handler, "/x/{request}")

    def test_depends_with_non_callable_refused(self):
        with pytest.raises(DependencyResolutionError, match="expected a callable"):
            Depends(42)  # ty: ignore[invalid-argument-type] -- the point

    def test_unresolvable_annotation_names_the_handler(self):
        namespace: dict = {}
        exec(
            "from __future__ import annotations\ndef handler(x: NotAThing): ...\n",
            namespace,
        )
        with pytest.raises(
            DependencyResolutionError, match="cannot resolve type hints"
        ):
            plan_for(namespace["handler"])

    def test_path_param_without_a_handler_parameter_is_fine(self):
        # The pre-DI convention: read it off request.path_params instead.
        def handler(request: Request): ...

        assert plan_for(handler, "/items/{item_id:int}").is_trivial


class TestPEP563:
    def test_module_level_dependency_resolves_under_pep563(self):
        # The realistic shape: an application module with `from __future__
        # import annotations` and its dependencies at module level. Locals
        # cannot work under PEP 563 (see this module's docstring), but nothing
        # a real app writes depends on that.
        namespace: dict = {}
        exec(
            "from __future__ import annotations\n"
            "from typing import Annotated\n"
            "from sonix.app.di import Depends\n"
            "def get_db() -> str: return 'db'\n"
            "def handler(db: Annotated[str, Depends(get_db)]): ...\n",
            namespace,
        )
        plan = plan_for(namespace["handler"])
        assert plan.params[0].source == "depends"


class TestCoercerParity:
    @pytest.mark.parametrize(
        ("type_name", "annotation", "raw"),
        [
            ("int", int, "42"),
            ("float", float, "2.5"),
            ("str", str, "hello"),
            ("uuid", uuid.UUID, "12345678-1234-5678-1234-567812345678"),
        ],
    )
    def test_query_and_path_coercion_agree(self, type_name, annotation, raw):
        # The two tables are deliberately separate -- routing keys by template
        # name, DI keys by type object -- so pin that they cannot drift.
        assert SCALAR_PARSERS[annotation](raw) == CONVERTERS[type_name].convert(raw)


class TestScalarCoercion:
    async def test_int_query_parameter(self):
        app = Sonix()

        @app.get("/")
        def handler(limit: int):
            return {"limit": limit}

        sent = await run(app, query_string=b"limit=42")
        assert body_of(sent) == b'{"limit":42}'

    async def test_default_used_when_absent(self):
        app = Sonix()

        @app.get("/")
        def handler(limit: int = 10):
            return {"limit": limit}

        assert body_of(await run(app)) == b'{"limit":10}'

    async def test_optional_without_default_is_none_when_absent(self):
        app = Sonix()

        @app.get("/")
        def handler(q: str | None):
            return {"q": q}

        assert body_of(await run(app)) == b'{"q":null}'

    async def test_missing_required_parameter_is_422(self):
        app = Sonix()

        @app.get("/")
        def handler(limit: int):
            return {"limit": limit}

        sent = await run(app)
        assert sent[0]["status"] == 422
        assert b"field required" in body_of(sent)

    async def test_uncoercible_value_is_422(self):
        app = Sonix()

        @app.get("/")
        def handler(limit: int):
            return {"limit": limit}

        sent = await run(app, query_string=b"limit=abc")
        assert sent[0]["status"] == 422
        assert b"expected int" in body_of(sent)

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            (b"flag=1", True),
            (b"flag=true", True),
            (b"flag=TRUE", True),
            (b"flag=yes", True),
            (b"flag=on", True),
            (b"flag=0", False),
            (b"flag=false", False),
            (b"flag=FALSE", False),
            (b"flag=no", False),
            (b"flag=off", False),
        ],
    )
    async def test_bool_token_table(self, raw, expected):
        app = Sonix()

        @app.get("/")
        def handler(flag: bool):
            return {"flag": flag}

        sent = await run(app, query_string=raw)
        assert body_of(sent) == (b'{"flag":true}' if expected else b'{"flag":false}')

    async def test_bool_rejects_python_truthiness(self):
        # The classic footgun: bool("false") is True.
        app = Sonix()

        @app.get("/")
        def handler(flag: bool):
            return {"flag": flag}

        assert body_of(await run(app, query_string=b"flag=false")) == b'{"flag":false}'

    async def test_valueless_flag_is_rejected_not_assumed_true(self):
        # `?flag` parses to "", which is in neither token table. Reject rather
        # than resolve -- the same posture the HTTP parser takes.
        app = Sonix()

        @app.get("/")
        def handler(flag: bool):
            return {"flag": flag}

        assert (await run(app, query_string=b"flag"))[0]["status"] == 422

    async def test_uuid_query_parameter(self):
        value = "12345678-1234-5678-1234-567812345678"
        app = Sonix()

        @app.get("/")
        def handler(token: uuid.UUID):
            return {"ok": str(token) == value}

        sent = await run(app, query_string=f"token={value}".encode())
        assert body_of(sent) == b'{"ok":true}'

    async def test_list_from_repeated_parameters(self):
        app = Sonix()

        @app.get("/")
        def handler(tag: list[str]):
            return {"tag": tag}

        sent = await run(app, query_string=b"tag=a&tag=b")
        assert body_of(sent) == b'{"tag":["a","b"]}'

    async def test_list_elements_are_coerced(self):
        app = Sonix()

        @app.get("/")
        def handler(n: list[int]):
            return {"n": n}

        assert body_of(await run(app, query_string=b"n=1&n=2")) == b'{"n":[1,2]}'

    async def test_every_bad_list_element_is_reported(self):
        app = Sonix()

        @app.get("/")
        def handler(n: list[int]):
            return {"n": n}

        sent = await run(app, query_string=b"n=1&n=x&n=y")
        assert sent[0]["status"] == 422
        assert body_of(sent).count(b"expected int") == 2

    async def test_empty_list_uses_default(self):
        app = Sonix()

        @app.get("/")
        def handler(tag: list[str] = []):  # noqa: B006
            return {"tag": tag}

        assert body_of(await run(app)) == b'{"tag":[]}'


class TestValidationErrors:
    async def test_all_failures_are_reported_in_one_response(self):
        app = Sonix()

        @app.get("/")
        def handler(limit: int, offset: int, name: str):
            return {}

        sent = await run(app, query_string=b"limit=abc")
        assert sent[0]["status"] == 422
        body = body_of(sent)
        # limit is uncoercible; offset and name are missing. All three.
        assert body.count(b'"loc"') == 3

    async def test_response_is_json(self):
        app = Sonix()

        @app.get("/")
        def handler(limit: int):
            return {}

        sent = await run(app, query_string=b"limit=abc")
        assert (b"content-type", b"application/json; charset=utf-8") in sent[0][
            "headers"
        ]

    async def test_error_entry_shape(self):
        import json

        app = Sonix()

        @app.get("/")
        def handler(limit: int):
            return {}

        payload = json.loads(body_of(await run(app, query_string=b"limit=abc")))
        assert payload["detail"] == [
            {"loc": ["query", "limit"], "msg": "expected int", "type": "int_parsing"}
        ]

    async def test_no_dependency_runs_when_a_scalar_fails(self):
        # The ordering guarantee: a bad query string must never leave a
        # half-opened resource behind.
        opened = []

        def get_db():
            opened.append("open")
            yield "db"
            opened.append("close")

        app = Sonix()

        @app.get("/")
        def handler(db: Annotated[str, Depends(get_db)], limit: int):
            return {}

        sent = await run(app, query_string=b"limit=abc")
        assert sent[0]["status"] == 422
        assert opened == [], "the dependency must not have been entered"

    async def test_validation_error_is_not_reraised_under_debug(self):
        # A 422 is a client mistake, not a bug to surface.
        app = Sonix(debug=True)

        @app.get("/")
        def handler(limit: int):
            return {}

        assert (await run(app))[0]["status"] == 422


class TestDependencies:
    async def test_simple_dependency_is_injected(self):
        def get_value() -> int:
            return 7

        app = Sonix()

        @app.get("/")
        def handler(value: Annotated[int, Depends(get_value)]):
            return {"value": value}

        assert body_of(await run(app)) == b'{"value":7}'

    async def test_default_value_form_also_works(self):
        def get_value() -> int:
            return 7

        app = Sonix()

        @app.get("/")
        # ty is right to object, and that objection is exactly why Annotated
        # is the canonical spelling: `value: int = Depends(...)` claims the
        # default is an int when it is a marker object. Annotated[int,
        # Depends(...)] says the same thing without lying to the type checker.
        def handler(value: int = Depends(get_value)):  # ty: ignore[invalid-parameter-default]
            return {"value": value}

        assert body_of(await run(app)) == b'{"value":7}'

    async def test_async_dependency(self):
        async def get_value() -> int:
            return 9

        app = Sonix()

        @app.get("/")
        def handler(value: Annotated[int, Depends(get_value)]):
            return {"value": value}

        assert body_of(await run(app)) == b'{"value":9}'

    async def test_sub_dependencies_resolve_recursively(self):
        def a() -> int:
            return 2

        def b(inner: Annotated[int, Depends(a)]) -> int:
            return inner * 3

        app = Sonix()

        @app.get("/")
        def handler(value: Annotated[int, Depends(b)]):
            return {"value": value}

        assert body_of(await run(app)) == b'{"value":6}'

    async def test_dependency_can_take_a_request(self):
        def peek(request: Request) -> str:
            return request.path

        app = Sonix()

        @app.get("/")
        def handler(seen: Annotated[str, Depends(peek)]):
            return {"seen": seen}

        assert body_of(await run(app)) == b'{"seen":"/"}'

    async def test_dependency_can_take_query_parameters(self):
        def paginate(limit: int = 10) -> int:
            return limit * 2

        app = Sonix()

        @app.get("/")
        def handler(doubled: Annotated[int, Depends(paginate)]):
            return {"doubled": doubled}

        sent = await run(app, query_string=b"limit=5")
        assert body_of(sent) == b'{"doubled":10}'

    async def test_sync_dependency_runs_off_the_event_loop(self):
        import threading

        loop_thread = threading.get_ident()
        seen = []

        def blocking() -> int:
            seen.append(threading.get_ident())
            return 1

        app = Sonix()

        @app.get("/")
        async def handler(value: Annotated[int, Depends(blocking)]):
            return {}

        await run(app)
        assert seen
        assert seen[0] != loop_thread


class TestDependencyCaching:
    async def test_one_dependency_used_twice_resolves_once(self):
        calls = []

        def get_db() -> str:
            calls.append(1)
            return "db"

        app = Sonix()

        @app.get("/")
        def handler(
            first: Annotated[str, Depends(get_db)],
            second: Annotated[str, Depends(get_db)],
        ):
            return {"same": first == second}

        assert body_of(await run(app)) == b'{"same":true}'
        assert len(calls) == 1

    async def test_shared_sub_dependency_resolves_once(self):
        calls = []

        def base() -> int:
            calls.append(1)
            return 1

        def left(value: Annotated[int, Depends(base)]) -> int:
            return value

        def right(value: Annotated[int, Depends(base)]) -> int:
            return value

        app = Sonix()

        @app.get("/")
        def handler(
            a: Annotated[int, Depends(left)],
            b: Annotated[int, Depends(right)],
        ):
            return {}

        await run(app)
        assert len(calls) == 1

    async def test_use_cache_false_resolves_every_time(self):
        calls = []

        def token() -> int:
            calls.append(1)
            return len(calls)

        app = Sonix()

        @app.get("/")
        def handler(
            first: Annotated[int, Depends(token, use_cache=False)],
            second: Annotated[int, Depends(token, use_cache=False)],
        ):
            return {}

        await run(app)
        assert len(calls) == 2

    async def test_cache_does_not_leak_between_requests(self):
        calls = []

        def get_db() -> str:
            calls.append(1)
            return "db"

        app = Sonix()

        @app.get("/")
        def handler(db: Annotated[str, Depends(get_db)]):
            return {}

        await run(app)
        await run(app)
        assert len(calls) == 2


class TestGeneratorDependencies:
    async def test_teardown_runs_after_the_response(self):
        events = []

        def get_db():
            events.append("open")
            yield "db"
            events.append("close")

        app = Sonix()

        @app.get("/")
        def handler(db: Annotated[str, Depends(get_db)]):
            events.append("handler")
            return {"db": db}

        await run(app)
        assert events == ["open", "handler", "close"]

    async def test_async_generator_dependency(self):
        events = []

        async def get_db():
            events.append("open")
            yield "db"
            events.append("close")

        app = Sonix()

        @app.get("/")
        def handler(db: Annotated[str, Depends(get_db)]):
            return {"db": db}

        assert body_of(await run(app)) == b'{"db":"db"}'
        assert events == ["open", "close"]

    async def test_teardown_is_lifo(self):
        events = []

        def outer_dep():
            events.append("outer-open")
            yield "outer"
            events.append("outer-close")

        def inner_dep(outer: Annotated[str, Depends(outer_dep)]):
            events.append("inner-open")
            yield "inner"
            events.append("inner-close")

        app = Sonix()

        @app.get("/")
        def handler(inner: Annotated[str, Depends(inner_dep)]):
            return {}

        await run(app)
        # A transaction opened inside a connection must close before it.
        assert events == [
            "outer-open",
            "inner-open",
            "inner-close",
            "outer-close",
        ]

    async def test_teardown_runs_when_the_handler_raises(self):
        events = []

        def get_db():
            events.append("open")
            try:
                yield "db"
            finally:
                events.append("close")

        app = Sonix()

        @app.get("/")
        def handler(db: Annotated[str, Depends(get_db)]):
            raise HTTPException(418)

        sent = await run(app)
        assert sent[0]["status"] == 418
        assert events == ["open", "close"]

    async def test_a_naked_yield_does_not_clean_up_on_error(self):
        # Documents the semantics rather than working around them. The
        # exception is thrown *into* the generator at the yield point, so code
        # after a bare `yield` is skipped -- exactly what contextlib.
        # contextmanager does, and what FastAPI does. Cleanup that must always
        # run needs try/finally; this test exists so nobody "fixes" it later.
        events = []

        def get_db():
            events.append("open")
            yield "db"
            events.append("close")

        app = Sonix()

        @app.get("/")
        def handler(db: Annotated[str, Depends(get_db)]):
            raise HTTPException(418)

        await run(app)
        assert events == ["open"]

    async def test_generator_sees_the_exception(self):
        seen = []

        def get_db():
            try:
                yield "db"
            except HTTPException as exc:
                seen.append(exc.status_code)
                raise

        app = Sonix()

        @app.get("/")
        def handler(db: Annotated[str, Depends(get_db)]):
            raise HTTPException(418)

        await run(app)
        assert seen == [418]


class TestBackwardsCompatibility:
    """The pre-DI convention, pinned. None of these needed editing when DI
    landed, and that is the point."""

    async def test_single_request_handler_still_works(self):
        app = Sonix()

        @app.get("/")
        def handler(request: Request):
            return {"path": request.path}

        assert body_of(await run(app)) == b'{"path":"/"}'

    async def test_unannotated_request_handler_still_works(self):
        app = Sonix()

        @app.get("/")
        def handler(request):
            return {"ok": True}

        assert body_of(await run(app)) == b'{"ok":true}'

    async def test_path_params_read_off_the_request_still_work(self):
        app = Sonix()

        @app.get("/items/{item_id:int}")
        def handler(request: Request):
            return {"item_id": request.path_params["item_id"]}

        sent = await run(app, path="/items/42")
        assert body_of(sent) == b'{"item_id":42}'

    async def test_trivial_plan_skips_the_exit_stack(self):
        app = Sonix()

        @app.get("/")
        def handler(request: Request):
            return {}

        # Not just an optimization: every handler in the benchmark takes this
        # path, so DI must cost them nothing.
        plan = plan_for(handler)
        assert plan.is_trivial
        assert not plan.needs_teardown


class TestRequestState:
    async def test_state_defaults_to_empty(self):
        request = Request(make_scope(), fake_receive)
        assert request.state == {}

    async def test_state_exposes_lifespan_values(self):
        request = Request(make_scope(state={"db": "connection"}), fake_receive)
        assert request.state["db"] == "connection"

    async def test_dependency_can_read_state(self):
        def get_db(request: Request) -> str:
            return request.state["db"]

        app = Sonix()

        @app.get("/")
        def handler(db: Annotated[str, Depends(get_db)]):
            return {"db": db}

        sent = await run(app, state={"db": "sqlite"})
        assert body_of(sent) == b'{"db":"sqlite"}'


class TestResolveDirectly:
    async def test_resolve_without_a_stack_when_no_teardown_needed(self):
        def handler(limit: int = 3): ...

        plan = plan_for(handler)
        request = Request(make_scope(), fake_receive)
        assert await resolve(plan, request) == {"limit": 3}

    async def test_resolve_uses_the_stack_for_generators(self):
        events = []

        def dep():
            events.append("open")
            yield 1
            events.append("close")

        def handler(value: Annotated[int, Depends(dep)]): ...

        plan = plan_for(handler)
        request = Request(make_scope(), fake_receive)
        async with contextlib.AsyncExitStack() as stack:
            assert await resolve(plan, request, stack) == {"value": 1}
            assert events == ["open"]
        assert events == ["open", "close"]

    async def test_concurrent_requests_do_not_share_a_cache(self):
        calls = []

        async def slow() -> int:
            await asyncio.sleep(0.01)
            calls.append(1)
            return 1

        app = Sonix()

        @app.get("/")
        async def handler(value: Annotated[int, Depends(slow)]):
            return {}

        await asyncio.gather(run(app), run(app))
        assert len(calls) == 2
