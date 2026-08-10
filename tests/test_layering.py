"""Architecture conformance: sonix/app/** must never import sonix.server.

The two-layer split described in docs/architecture.md only holds if this
stays true, so it's enforced mechanically rather than by convention alone.

The concrete thing it buys, since WebSockets landed: no opcode, mask byte or
frame boundary can reach the application layer, because the only module that
knows what those are is one this test forbids importing.
"""

import ast
import pathlib

import pytest

import sonix.app

APP_DIR = pathlib.Path(sonix.app.__file__).parent
PACKAGE_ROOT = APP_DIR.parent.parent  # src/


def _package_of(path: pathlib.Path) -> str:
    """The dotted package a module lives in, e.g. "sonix.app"."""
    return ".".join(path.relative_to(PACKAGE_ROOT).parts[:-1])


def _imported_modules(path: pathlib.Path) -> set[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    package = _package_of(path)
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0:
                if node.module:
                    modules.add(node.module)
                continue
            # Relative: `from ..server.websockets import Opcode` parses with
            # module="server.websockets", which matches nothing when compared
            # against absolute names. Resolving it is what closes the hole --
            # without this the whole check is one dot away from useless.
            parts = package.split(".")
            base = ".".join(parts[: len(parts) - (node.level - 1)])
            modules.add(f"{base}.{node.module}" if node.module else base)
    return modules


def _server_imports(modules: set[str]) -> set[str]:
    return {m for m in modules if m == "sonix.server" or m.startswith("sonix.server.")}


def test_app_never_imports_server():
    offenders = {}
    for path in APP_DIR.rglob("*.py"):
        found = _server_imports(_imported_modules(path))
        if found:
            offenders[str(path)] = found
    assert not offenders, f"sonix/app/** must never import sonix.server: {offenders}"


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("from sonix.server.websockets import Opcode", True),
        ("import sonix.server", True),
        ("from ..server.websockets import Opcode", True),
        ("from ..server import protocol", True),
        ("from .requests import Request", False),
        ("from sonix.types import Scope", False),
    ],
)
def test_the_check_itself_catches_what_it_claims_to(source, expected):
    # A guard on the guard: the relative-import spellings above evaded this
    # test entirely until node.level was resolved, and a check that silently
    # passes everything is worse than no check.
    module = APP_DIR / "_layering_probe.py"
    module.write_text(source)
    try:
        assert bool(_server_imports(_imported_modules(module))) is expected
    finally:
        module.unlink()
