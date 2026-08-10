"""Collection guard for the cross-implementation conformance tests.

These tests are the empirical form of the claim docs/architecture.md makes:
that Sonix's server is a real ASGI server and a Sonix app is a real ASGI app,
so either half can be swapped for someone else's. They need third-party
packages, which live in the `conformance` dependency group rather than `dev` --
so a plain `uv sync && uv run pytest` skips this directory instead of failing.

    uv sync --group conformance

Note the deliberately broad `except Exception`. importorskip only handles
ImportError, but a dependency can also be *installed and broken* against the
running interpreter -- pydantic raises TypeError on Python 3.14 release
candidates, because typing._eval_type changed signature before the final
release. That is an environment problem, not a Sonix problem, and it should
skip the affected module rather than fail collection for the whole suite.
"""

from __future__ import annotations

import importlib

collect_ignore: list[str] = []


def _importable(module: str) -> bool:
    try:
        importlib.import_module(module)
    except Exception:
        return False
    return True


_ALL = [
    "test_fastapi_on_sonix.py",
    "test_starlette_on_sonix.py",
    "test_sonix_under_uvicorn.py",
]

if not _importable("httpx"):
    collect_ignore.extend(_ALL)
else:
    if not _importable("fastapi"):
        collect_ignore.append("test_fastapi_on_sonix.py")
    if not _importable("starlette"):
        collect_ignore.append("test_starlette_on_sonix.py")
    if not _importable("uvicorn"):
        collect_ignore.append("test_sonix_under_uvicorn.py")
