"""Sonix: an async web framework built from scratch on asyncio.

Everything a handler author needs is re-exported here. Note that this module
is the one place that legitimately touches both layers -- ``sonix.app`` must
never import ``sonix.server`` (enforced by tests/test_layering.py), but the
top-level package assembles the two for convenience.
"""

from __future__ import annotations

import argparse
import importlib
import sys

from sonix.app.applications import Sonix
from sonix.app.requests import Request
from sonix.app.responses import HTMLResponse, JSONResponse, PlainTextResponse, Response
from sonix.server.protocol import HTTPProtocol
from sonix.server.server import DEFAULT_HOST, DEFAULT_PORT, Config, Server, run
from sonix.types import ASGIApp

__all__ = [
    "ASGIApp",
    "Config",
    "HTMLResponse",
    "HTTPProtocol",
    "JSONResponse",
    "PlainTextResponse",
    "Request",
    "Response",
    "Server",
    "Sonix",
    "main",
    "run",
]

DEFAULT_TARGET = "sonix._demo:app"


def import_from_string(target: str) -> ASGIApp:
    """Resolve a ``"module.path:attribute"`` string to the object it names.

    The same convention uvicorn uses, which matters beyond ergonomics: it
    lets a benchmark harness launch Sonix and uvicorn with structurally
    identical command lines, so the comparison isn't measuring two different
    startup paths.
    """
    module_name, separator, attribute = target.partition(":")
    if not separator or not module_name or not attribute:
        raise ValueError(
            f"invalid target {target!r}: expected the form 'module.path:attribute'"
        )

    # Console scripts don't get the working directory on sys.path the way
    # `python -m` does, so `sonix examples.chat.app:app` would fail from a
    # checkout without this.
    if "" not in sys.path and str(sys.path[0]) != "":
        sys.path.insert(0, "")

    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise ValueError(f"could not import module {module_name!r}: {exc}") from exc

    try:
        app = getattr(module, attribute)
    except AttributeError:
        raise ValueError(
            f"module {module_name!r} has no attribute {attribute!r}"
        ) from None

    if not callable(app):
        # ValueError, not TypeError (which is what TRY004 wants): the argument
        # to this function is a perfectly well-typed string. What's wrong is
        # the value it names. Keeping every failure here in one exception
        # family is also what lets the CLI report them all the same way.
        raise ValueError(  # noqa: TRY004
            f"{target!r} is not callable, so it cannot be an ASGI app"
        )
    return app


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="sonix",
        description="Serve an ASGI application on Sonix's own HTTP/1.1 server.",
    )
    parser.add_argument(
        "target",
        nargs="?",
        default=DEFAULT_TARGET,
        help=(
            "application to serve, as 'module.path:attribute' "
            f"(default: {DEFAULT_TARGET}, a built-in demo)"
        ),
    )
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        app = import_from_string(args.target)
    except ValueError as exc:
        print(f"sonix: {exc}", file=sys.stderr)
        return 2

    print(
        f"Sonix serving {args.target} on "
        f"http://{args.host}:{args.port} (Ctrl+C to stop)"
    )
    try:
        run(app, host=args.host, port=args.port)
    except KeyboardInterrupt:
        pass
    print("\nShutting down.")
    return 0
