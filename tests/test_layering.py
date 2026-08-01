"""Architecture conformance: sonix/app/** must never import sonix.server.

The two-layer split described in docs/architecture.md only holds if this
stays true, so it's enforced mechanically rather than by convention alone.
"""

import ast
import pathlib

import sonix.app

APP_DIR = pathlib.Path(sonix.app.__file__).parent


def _imported_modules(path: pathlib.Path) -> set[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


def test_app_never_imports_server():
    offenders = {}
    for path in APP_DIR.rglob("*.py"):
        modules = _imported_modules(path)
        server_imports = {m for m in modules if m == "sonix.server" or m.startswith("sonix.server.")}
        if server_imports:
            offenders[str(path)] = server_imports
    assert not offenders, f"sonix/app/** must never import sonix.server: {offenders}"
