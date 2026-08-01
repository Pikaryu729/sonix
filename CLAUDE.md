# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project goal

Sonix is an **async web framework built from scratch**, as a learning/signal project: the point is to understand what frameworks like FastAPI actually do under the hood, not to depend on them.

Scope:
- Built on `asyncio` (stdlib) only — **no external runtime dependencies**. If a dependency creeps into `pyproject.toml` for anything other than dev tooling (e.g. pytest), that's almost certainly a mistake, not an intentional addition.
- An ASGI-compliant server implemented from first principles:
  - event loop integration
  - HTTP/1.1 parser
  - routing with path parameters
  - middleware stack
  - dependency injection
  - WebSockets
- Success signal: it should be benchmarkable against FastAPI using `wrk`, and the implementation should make clear what a decorator like `@app.get(...)` is actually doing (route registration, request parsing, handler dispatch, response serialization).

## Project state

Currently `src/sonix/__init__.py` contains only a placeholder `main()` entry point — none of the framework above is implemented yet. There is no README content, no tests, and no established module layout yet. Treat structural decisions (how routing/middleware/DI/WebSocket modules are organized) as open; don't infer conventions that aren't there.

Note: `pyproject.toml` currently declares `asyncio>=4.0.0` as a dependency. Since `asyncio` is part of the Python 3.14 standard library, this is very likely a leftover/mistaken entry rather than an intentional pin — flag it if touching dependencies, since the stated goal is zero runtime deps.

## Commands

This project uses [uv](https://docs.astral.sh/uv/) for dependency management and requires Python 3.14+ (see `.python-version`).

- Install/sync dependencies: `uv sync`
- Run the CLI entry point: `uv run sonix`
- Run tests: `uv run pytest` (pytest is declared as a dev dependency in `pyproject.toml`, but no test files exist yet)
- Run a single test: `uv run pytest path/to/test_file.py::test_name`
- Add a dependency: `uv add <package>`
- Add a dev dependency: `uv add --dev <package>`

## Architecture

- Package layout follows the `src/` layout: importable code lives in `src/sonix/`.
- The console script `sonix` (defined in `pyproject.toml` under `[project.scripts]`) maps to `sonix:main`.
- Build backend is `uv_build` (declared in `[build-system]`).
