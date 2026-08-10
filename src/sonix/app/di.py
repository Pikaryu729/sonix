"""Dependency injection: what a handler declares, resolved per request.

A handler's signature is inspected exactly **once**, when ``@app.get(...)``
runs, and turned into a ``Plan``. Per request the plan is executed -- no
``inspect`` call, no ``get_type_hints`` call, no annotation parsing on the hot
path. That is the whole reason plan-building and resolution are separate
functions here rather than one clever wrapper.

Two orderings matter, and they are not the same thing.

**Classification precedence**, decided once per parameter at decoration time:
``Depends`` > special type (``Request``) > path parameter > query parameter.

**Execution order**, per request: *every* scalar in the whole dependency
graph is resolved and coerced first, collecting every failure, and a 422 is
raised before a single dependency callable runs. That ordering is what stops
a bad query string from leaving a half-opened database connection behind, and
it is why the client gets all its mistakes in one response instead of one per
round trip.

Anything the framework cannot satisfy raises ``DependencyResolutionError``
**at decoration time**, naming the handler and the parameter -- cheaper and
louder than FastAPI's deferred-to-request-time behavior. A signature the
framework cannot satisfy is a programming error and should surface when the
module is imported, not when a request happens to arrive.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import types
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Union, get_args, get_origin, get_type_hints

from sonix.app.exceptions import ValidationError
from sonix.app.requests import Request
from sonix.app.routing import Converter


class DependencyResolutionError(Exception):
    """A handler signature this framework cannot satisfy. Raised at decoration."""


class Depends:
    """Marker for an injected dependency.

    Canonically ``Annotated[Connection, Depends(get_db)]``. The older
    ``db: Connection = Depends(get_db)`` form also works, but ``Annotated``
    keeps the default slot free for an actual default value, which the
    default-value form cannot do.
    """

    __slots__ = ("dependency", "use_cache")

    def __init__(self, dependency: Callable[..., Any], *, use_cache: bool = True):
        if not callable(dependency):
            raise DependencyResolutionError(
                f"Depends({dependency!r}) -- expected a callable"
            )
        self.dependency = dependency
        self.use_cache = use_cache

    def __repr__(self) -> str:
        name = getattr(self.dependency, "__name__", repr(self.dependency))
        return f"Depends({name})"


# -- type analysis (all of this runs at decoration time only) -----------------


def _parse_bool(value: str) -> bool:
    # Python truthiness is exactly wrong here: bool("false") is True. HTTP has
    # no boolean type, so the accepted spellings must be stated explicitly.
    # Note `?flag` with no value yields "" and is therefore rejected rather
    # than treated as "present means true" -- the same reject-never-resolve
    # posture the HTTP parser takes.
    lowered = value.strip().lower()
    if lowered in ("1", "true", "t", "yes", "y", "on"):
        return True
    if lowered in ("0", "false", "f", "no", "n", "off"):
        return False
    raise ValueError(f"not a boolean: {value!r}")


# Keyed by the *type object*, because a query annotation is a type (`int`)
# whereas routing.CONVERTERS is keyed by the template's converter name
# ("int"). The conversion callables coincide for the four shared types, and a
# test pins that -- but they are genuinely different lookups, and collapsing
# them would mean every new path converter silently became a query type.
SCALAR_PARSERS: dict[Any, Callable[[str], Any]] = {
    str: str,
    int: int,
    float: float,
    bool: _parse_bool,
    uuid.UUID: uuid.UUID,
}


def _unwrap_annotated(annotation: Any) -> tuple[Any, tuple[Any, ...]]:
    """Split ``Annotated[X, a, b]`` into ``(X, (a, b))``."""
    metadata = getattr(annotation, "__metadata__", None)
    if metadata is None:
        return annotation, ()
    return annotation.__origin__, tuple(metadata)


def _unwrap_optional(annotation: Any) -> tuple[Any, bool]:
    """Split ``X | None`` into ``(X, True)``. Handles both union spellings."""
    origin = get_origin(annotation)
    if origin is Union or origin is types.UnionType:
        args = [arg for arg in get_args(annotation) if arg is not type(None)]
        if len(args) == 1:
            return args[0], True
    return annotation, False


def _unwrap_list(annotation: Any) -> tuple[Any, bool]:
    """Split ``list[X]`` into ``(X, True)``."""
    if annotation is list:
        return str, True
    if get_origin(annotation) is list:
        args = get_args(annotation)
        return (args[0] if args else str), True
    return annotation, False


def _target_function(call: Callable[..., Any]) -> Callable[..., Any]:
    """The object whose annotations describe ``call``'s parameters.

    For a callable *object*, ``get_type_hints(instance)`` inspects the class's
    *attribute* annotations and returns nothing useful for ``__call__``'s
    parameters -- silently, so every annotation on a class-based handler would
    be invisible. ``inspect.signature(instance)`` already resolves to
    ``__call__`` (minus ``self``), and hints are matched to parameters by
    name, so the two reconcile correctly.
    """
    if inspect.isfunction(call) or inspect.ismethod(call):
        return call
    call_attr = getattr(type(call), "__call__", None)  # noqa: B004
    return call_attr if call_attr is not None else call


def _hints_for(call: Callable[..., Any], label: str) -> dict[str, Any]:
    try:
        return get_type_hints(_target_function(call), include_extras=True)
    except Exception as exc:
        # Usually a NameError: an annotation that cannot be resolved from the
        # defining module, e.g. a TYPE_CHECKING-only import or a class defined
        # inside a function. Every module here uses `from __future__ import
        # annotations`, so this surfaces at user-module import time -- naming
        # the handler is far more actionable than a bare NameError.
        raise DependencyResolutionError(
            f"cannot resolve type hints for {label!r}: {exc}. "
            f"Annotations must be importable at runtime."
        ) from exc


def _find_depends(
    label: str, name: str, annotation: Any, default: Any
) -> Depends | None:
    _base, metadata = _unwrap_annotated(annotation)
    markers = [item for item in metadata if isinstance(item, Depends)]
    if len(markers) > 1:
        raise DependencyResolutionError(
            f"{label}({name}) has more than one Depends(...) in its Annotated "
            f"metadata; which one wins is not something to guess at."
        )
    default_marker = default if isinstance(default, Depends) else None
    if markers and default_marker is not None:
        raise DependencyResolutionError(
            f"{label}({name}) declares Depends(...) both in Annotated and as a "
            f"default value. Use one or the other."
        )
    if markers:
        return markers[0]
    return default_marker


# -- the plan ----------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class QuerySpec:
    name: str
    parse: Callable[[str], Any]
    type_name: str
    is_list: bool
    optional: bool
    has_default: bool
    default: Any


@dataclass(frozen=True, slots=True)
class DependencySpec:
    call: Callable[..., Any]
    plan: Plan
    use_cache: bool
    is_async: bool
    is_async_gen: bool
    is_sync_gen: bool

    @property
    def is_generator(self) -> bool:
        return self.is_async_gen or self.is_sync_gen


@dataclass(frozen=True, slots=True)
class Param:
    name: str
    # "request" | "path" | "query" | "depends"
    source: str
    query: QuerySpec | None = None
    dependency: DependencySpec | None = None


@dataclass(frozen=True, slots=True)
class Plan:
    params: tuple[Param, ...]
    # Every QuerySpec anywhere in the graph, flattened at build time so the
    # per-request scalar pass is one loop rather than a tree walk.
    query_specs: tuple[QuerySpec, ...]
    # True when any dependency in the graph is a generator, i.e. when an
    # AsyncExitStack actually has work to do. Constructing one per request is
    # not free, and the benchmark in a later milestone would notice.
    needs_teardown: bool
    # The historical single-Request convention -- the shape of every handler
    # written before DI existed, and of the benchmark's own handlers.
    is_trivial: bool


def _flatten_query_specs(params: tuple[Param, ...]) -> tuple[QuerySpec, ...]:
    specs: list[QuerySpec] = []
    for param in params:
        if param.source == "query" and param.query is not None:
            specs.append(param.query)
        elif param.source == "depends" and param.dependency is not None:
            specs.extend(param.dependency.plan.query_specs)
    return tuple(specs)


def _any_teardown(params: tuple[Param, ...]) -> bool:
    for param in params:
        spec = param.dependency
        if spec is not None and (spec.is_generator or spec.plan.needs_teardown):
            return True
    return False


def build_plan(
    call: Callable[..., Any],
    *,
    path_converters: dict[str, Converter] | None = None,
    where: str | None = None,
    _seen: frozenset[int] = frozenset(),
) -> Plan:
    """Inspect ``call`` once and describe how to satisfy each parameter."""
    path_converters = path_converters or {}
    label = where or getattr(call, "__name__", repr(call))

    if id(call) in _seen:
        raise DependencyResolutionError(
            f"circular dependency detected while resolving {label!r}"
        )

    try:
        signature = inspect.signature(call)
    except (ValueError, TypeError) as exc:
        # Some C callables have no introspectable signature.
        raise DependencyResolutionError(
            f"cannot inspect the signature of {label!r}: {exc}"
        ) from exc

    hints = _hints_for(call, label)
    params: list[Param] = []

    for name, parameter in signature.parameters.items():
        if parameter.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            # Cannot be satisfied by name, and silently ignoring **kwargs
            # means a handler expecting query params through it gets nothing.
            raise DependencyResolutionError(
                f"{label}(*{name}) -- variadic parameters are not injectable"
            )
        if parameter.kind is inspect.Parameter.POSITIONAL_ONLY:
            # Everything is passed as handler(**kwargs), so a positional-only
            # parameter could never be filled.
            raise DependencyResolutionError(
                f"{label}({name}, /) -- positional-only parameters cannot be "
                f"injected, since handlers are called with keyword arguments"
            )

        annotation = hints.get(name, parameter.annotation)
        if annotation is inspect.Parameter.empty:
            annotation = None
        default = parameter.default
        has_default = default is not inspect.Parameter.empty

        marker = _find_depends(label, name, annotation, default)
        if marker is not None:
            params.append(
                Param(
                    name=name,
                    source="depends",
                    dependency=_build_dependency(
                        marker, path_converters, _seen | {id(call)}
                    ),
                )
            )
            continue

        base, _metadata = _unwrap_annotated(annotation)

        if base is Request or (base is None and name == "request"):
            # The unannotated spelling is not a nicety: handlers written
            # before DI existed say `def handler(request):`, and without this
            # they would fall through to "required str query parameter" and
            # 422 on every request -- silently wrong, the worst outcome.
            if name in path_converters:
                raise DependencyResolutionError(
                    f"{label}({name}) is both a path parameter in the route "
                    f"template and the injected Request. Rename one."
                )
            params.append(Param(name=name, source="request"))
            continue

        if name in path_converters:
            _check_path_annotation(label, name, base, path_converters[name])
            params.append(Param(name=name, source="path"))
            continue

        params.append(
            Param(
                name=name,
                source="query",
                query=_build_query_spec(label, name, base, has_default, default),
            )
        )

    frozen = tuple(params)
    return Plan(
        params=frozen,
        query_specs=_flatten_query_specs(frozen),
        needs_teardown=_any_teardown(frozen),
        is_trivial=len(frozen) == 1 and frozen[0].source == "request",
    )


def _check_path_annotation(
    label: str, name: str, annotation: Any, converter: Converter
) -> None:
    if annotation is None:
        return
    inner, _optional = _unwrap_optional(annotation)
    # Every converter's `convert` callable *is* the type it produces
    # (int, float, str, uuid.UUID), so it doubles as the expected annotation.
    if inner is not converter.convert:
        expected = getattr(converter.convert, "__name__", converter.convert)
        actual = getattr(inner, "__name__", inner)
        raise DependencyResolutionError(
            f"{label}({name}: {actual}) disagrees with the route template, "
            f"which converts {name!r} to {expected}. The template is the "
            f"single source of truth for path-parameter types -- re-coercing "
            f"here would make 'the path matched but the value would not "
            f"convert' a reachable state, which routing deliberately avoids."
        )


def _build_query_spec(
    label: str, name: str, annotation: Any, has_default: bool, default: Any
) -> QuerySpec:
    inner, optional = _unwrap_optional(annotation)
    item, is_list = _unwrap_list(inner)
    item, item_optional = _unwrap_optional(item)
    optional = optional or item_optional

    if item is None:
        # Unannotated. Usable as a plain string, but only with a default --
        # otherwise there is nothing to say what it is, or what to do when the
        # client omits it.
        if not has_default:
            raise DependencyResolutionError(
                f"{label}({name}) has no annotation, no default, and is not a "
                f"path parameter, so there is nothing to resolve it from. Add "
                f"a type annotation, a default, or Depends(...)."
            )
        item = str

    parse = SCALAR_PARSERS.get(item)
    if parse is None:
        supported = ", ".join(sorted(t.__name__ for t in SCALAR_PARSERS))
        actual = getattr(item, "__name__", item)
        raise DependencyResolutionError(
            f"{label}({name}: {actual}) cannot be built from a query string. "
            f"Supported types: {supported} (optionally list[...] or | None). "
            f"For anything else, use Depends(...)."
        )

    return QuerySpec(
        name=name,
        parse=parse,
        type_name=getattr(item, "__name__", str(item)),
        is_list=is_list,
        optional=optional,
        has_default=has_default,
        default=default if has_default else None,
    )


def _build_dependency(
    marker: Depends,
    path_converters: dict[str, Converter],
    seen: frozenset[int],
) -> DependencySpec:
    call = marker.dependency
    target = _target_function(call)
    return DependencySpec(
        call=call,
        plan=build_plan(call, path_converters=path_converters, _seen=seen),
        use_cache=marker.use_cache,
        is_async=inspect.iscoroutinefunction(target),
        is_async_gen=inspect.isasyncgenfunction(target),
        is_sync_gen=inspect.isgeneratorfunction(target),
    )


# -- resolution (per request) -------------------------------------------------


def _error(source: str, name: str, msg: str, kind: str) -> dict:
    return {"loc": [source, name], "msg": msg, "type": kind}


def _convert(spec: QuerySpec, raw: str) -> Any:
    try:
        return spec.parse(raw)
    except (ValueError, TypeError) as exc:
        raise ValidationError(
            [
                _error(
                    "query",
                    spec.name,
                    f"expected {spec.type_name}",
                    f"{spec.type_name}_parsing",
                )
            ]
        ) from exc


def _resolve_query(spec: QuerySpec, request: Request) -> Any:
    params = request.query_params

    if spec.is_list:
        raw_values = params.get_list(spec.name)
        if not raw_values:
            if spec.has_default:
                return spec.default
            if spec.optional:
                return None
            raise ValidationError(
                [_error("query", spec.name, "field required", "missing")]
            )
        values, errors = [], []
        for raw in raw_values:
            try:
                values.append(_convert(spec, raw))
            except ValidationError as exc:
                errors.extend(exc.errors)
        if errors:
            raise ValidationError(errors)
        return values

    raw_value = params.get(spec.name)
    if raw_value is None:
        if spec.has_default:
            return spec.default
        if spec.optional:
            return None
        raise ValidationError([_error("query", spec.name, "field required", "missing")])
    return _convert(spec, raw_value)


def _resolve_all_scalars(plan: Plan, request: Request) -> dict[int, Any]:
    """Pass one: every query parameter in the graph, all errors at once.

    Runs before any dependency callable, so a malformed query string can never
    leave a half-opened resource behind, and the client sees every mistake in
    one response instead of one per round trip.
    """
    values: dict[int, Any] = {}
    errors: list[dict] = []
    for spec in plan.query_specs:
        try:
            values[id(spec)] = _resolve_query(spec, request)
        except ValidationError as exc:
            errors.extend(exc.errors)
    if errors:
        raise ValidationError(errors)
    return values


async def _call_dependency(
    spec: DependencySpec,
    kwargs: dict[str, Any],
    stack: contextlib.AsyncExitStack | None,
) -> Any:
    if spec.is_async_gen:
        assert stack is not None
        manager = contextlib.asynccontextmanager(spec.call)(**kwargs)
        return await stack.enter_async_context(manager)
    if spec.is_sync_gen:
        assert stack is not None
        # Entered and exited on the event-loop thread, deliberately. Running
        # the two halves in different worker threads would break any resource
        # with thread affinity -- sqlite3 connections default to
        # check_same_thread=True and raise on close from another thread, which
        # is exactly the shape the example app uses. A `yield` dependency's
        # cleanup is a close() or a release, not real work; anything expensive
        # belongs in an async dependency.
        manager = contextlib.contextmanager(spec.call)(**kwargs)
        return stack.enter_context(manager)
    if spec.is_async:
        return await spec.call(**kwargs)
    return await asyncio.to_thread(spec.call, **kwargs)


async def _resolve_dependency(
    spec: DependencySpec,
    request: Request,
    stack: contextlib.AsyncExitStack | None,
    scalars: dict[int, Any],
    cache: dict[int, Any],
) -> Any:
    key = id(spec.call)
    if spec.use_cache and key in cache:
        return cache[key]
    kwargs = await _resolve_params(spec.plan, request, stack, scalars, cache)
    value = await _call_dependency(spec, kwargs, stack)
    if spec.use_cache:
        cache[key] = value
    return value


async def _resolve_params(
    plan: Plan,
    request: Request,
    stack: contextlib.AsyncExitStack | None,
    scalars: dict[int, Any],
    cache: dict[int, Any],
) -> dict[str, Any]:
    """Pass two: assemble kwargs, invoking dependency callables as needed."""
    kwargs: dict[str, Any] = {}
    for param in plan.params:
        if param.source == "request":
            kwargs[param.name] = request
        elif param.source == "path":
            kwargs[param.name] = request.path_params[param.name]
        elif param.source == "query":
            assert param.query is not None
            kwargs[param.name] = scalars[id(param.query)]
        else:
            assert param.dependency is not None
            kwargs[param.name] = await _resolve_dependency(
                param.dependency, request, stack, scalars, cache
            )
    return kwargs


async def resolve(
    plan: Plan,
    request: Request,
    stack: contextlib.AsyncExitStack | None = None,
) -> dict[str, Any]:
    """Execute a plan against one request. No introspection happens here."""
    if plan.is_trivial:
        # The pre-DI convention. Skipped straight through so adding DI costs
        # existing handlers nothing measurable.
        return {plan.params[0].name: request}
    scalars = _resolve_all_scalars(plan, request)
    return await _resolve_params(plan, request, stack, scalars, {})
