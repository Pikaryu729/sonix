"""The zero-runtime-dependency claim, enforced mechanically.

"Sonix has no runtime dependencies" is the project's central premise, and the
benchmark and conformance suites deliberately pull in FastAPI, uvicorn,
Starlette and h11 -- so the claim needs to be a checked invariant rather than
an argument from the absence of names in a file. If a package ever creeps into
[project].dependencies, this fails in CI.
"""

from __future__ import annotations

import pathlib
import tomllib

import sonix

PYPROJECT = pathlib.Path(sonix.__file__).parents[2] / "pyproject.toml"

# Groups allowed to contain third-party packages. Everything here is tooling,
# measurement, or cross-checking -- none of it is imported by sonix itself.
TOOLING_GROUPS = {"dev", "bench", "conformance", "docs"}


def _pyproject() -> dict:
    return tomllib.loads(PYPROJECT.read_text())


def test_pyproject_is_findable():
    # Guards the other tests: a wrong path would make them vacuously pass.
    assert PYPROJECT.is_file(), f"expected pyproject.toml at {PYPROJECT}"


def test_no_runtime_dependencies():
    dependencies = _pyproject()["project"].get("dependencies", [])
    assert dependencies == [], (
        "sonix must have zero runtime dependencies -- it implements its own "
        f"HTTP parser, server, and routing. Found: {dependencies}"
    )


def test_no_optional_runtime_dependencies():
    # An extra is still a runtime dependency, just an opt-in one; it would let
    # the premise erode via `pip install sonix[fast]`.
    optional = _pyproject()["project"].get("optional-dependencies", {})
    assert optional == {}, f"sonix must declare no extras. Found: {optional}"


def test_dependency_groups_are_tooling_only():
    groups = _pyproject().get("dependency-groups", {})
    unexpected = set(groups) - TOOLING_GROUPS
    assert not unexpected, (
        f"unrecognized dependency-groups {unexpected}: third-party packages "
        f"belong in one of {sorted(TOOLING_GROUPS)}, never in [project].dependencies"
    )


def test_sonix_imports_only_stdlib_and_itself():
    """No module under src/sonix/ may import a third-party package.

    A stricter companion to the pyproject check: it catches an import that
    happens to resolve because a *dev* dependency is installed, which the
    metadata check alone would miss.
    """
    import ast
    import sys

    package_root = pathlib.Path(sonix.__file__).parent
    stdlib = sys.stdlib_module_names
    offenders: dict[str, set[str]] = {}

    for path in package_root.rglob("*.py"):
        tree = ast.parse(path.read_text(), filename=str(path))
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                imported.add(node.module)

        foreign = {
            name
            for name in imported
            if name.split(".")[0] not in stdlib and name.split(".")[0] != "sonix"
        }
        if foreign:
            offenders[str(path.relative_to(package_root))] = foreign

    assert not offenders, f"sonix may only import stdlib and itself: {offenders}"
