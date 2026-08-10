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

`docs/architecture.md` is the reference document for this project: module layout, the design decisions behind each layer, and a 10-step build order. Read it before adding a new subsystem — most structural questions are already answered there, with reasoning.

Built so far (build-order steps 1–9):

- `types.py` — shared ASGI type aliases.
- `server/parser.py` — incremental HTTP/1.1 parser: pure and synchronous, chunked bodies, pipelining, and "reject, never resolve" smuggling defenses.
- `server/protocol.py` — the `asyncio.Protocol` ↔ ASGI bridge: connection lifecycle, keep-alive, pipelined-response ordering, read/write backpressure, slow-loris timeout.
- `server/server.py` — `Config`/`Server`: socket binding, graceful shutdown, signal handling, and the server-side lifespan runner.
- `app/requests.py`, `app/responses.py` — `Request`, `Response`/`JSONResponse`/`PlainTextResponse`/`HTMLResponse`.
- `app/routing.py` — `Router`/`Route`: compiled path templates, param converters, 404 vs. 405.
- `app/applications.py` — the `Sonix` class and `@app.get`/`@app.post`/etc.
- `app/middleware.py`, `app/exceptions.py` — ASGI-onion composition and `ExceptionMiddleware`, so a raising handler returns a 500 instead of dropping the connection.
- `app/lifespan.py` — startup/shutdown, and the `scope["state"]` the server copies into each request.
- `app/di.py` — `Depends`, plans built once at decoration time, query/path coercion, generator dependencies.
- `server/websockets.py` — RFC 6455 handshake and frame codec: pure bytes, no `asyncio`, message-level events (fragmentation reassembled internally), and a directional `require_mask` so the same codec drives the test client.
- `app/websockets.py` — the `WebSocket` a handler sees, built only from `(scope, receive, send)`. **It must never import `server/websockets.py`**: no opcode, mask or frame boundary appears in the app layer, which is why the same handler runs on uvicorn.

Note the build order was **reordered**: middleware and exceptions (originally step 8) landed before DI (originally step 7), because DI's failure modes need somewhere to go. `docs/architecture.md` records this.

The parser owns one more decision than it used to: an upgrade request switches it to a terminal `UPGRADED` state, so HTTP framing stops and no second request head can be emitted. That is a smuggling defense, not bookkeeping — see the "Where an upgrade stops HTTP framing" section in `docs/architecture.md` before touching it.

`uv run sonix` serves a built-in demo on `127.0.0.1:8000`, and `uv run sonix module:app` serves any app, so changes to either layer can be exercised end-to-end with `curl`.

Not yet built: the example application, and the hardening/benchmark pass (build-order step 10). Follow the shape `docs/architecture.md` sketches unless there's a reason not to, and update that document if you deviate.

`tests/conformance/` needs its own dependency group (`uv run --group conformance pytest tests/conformance`); a plain `uv run pytest` skips it.

`tests/autobahn/` holds the RFC 6455 conformance suite. It runs in CI as a blocking job rather than under pytest, because it ships as a Docker image; `tests/autobahn/README.md` covers running it locally and what is excluded.

## Commands

This project uses [uv](https://docs.astral.sh/uv/) for dependency management and requires Python 3.14+ (see `.python-version`).

- Install/sync dependencies: `uv sync`
- Run the CLI entry point: `uv run sonix`
- Run tests: `uv run pytest`
- Run a single test: `uv run pytest path/to/test_file.py::test_name`
- Add a dependency: `uv add <package>`
- Add a dev dependency: `uv add --dev <package>`

## Code quality

- Formatting: `uv run ruff format .`
- Linting: `uv run ruff check .` (auto-fix most issues with `uv run ruff check --fix .`)
- Type checking: `uv run ty check`

`ruff` and `ty` are declared as dev dependencies, so `uv sync` installs them.

**Do not commit if `ruff format --check`, `ruff check`, or `ty check` fail.** Fix the underlying issue rather than working around it. The one exception is a `ty` diagnostic that is actually a false positive (not a real type error) — suppress that specific line with a `# ty: ignore` comment rather than leaving `ty check` failing or restructuring the code to dodge it.

## Architecture

- Package layout follows the `src/` layout: importable code lives in `src/sonix/`.
- The console script `sonix` (defined in `pyproject.toml` under `[project.scripts]`) maps to `sonix:main`.
- Build backend is `uv_build` (declared in `[build-system]`).

### The two-layer split (the load-bearing rule)

The framework is deliberately split into two layers, described in full in `docs/architecture.md`:

- **`sonix/server/`** — the "uvicorn" layer. A raw asyncio TCP server plus the hand-rolled HTTP/1.1 parser. Its job ends at the ASGI boundary: bytes on a socket become `(scope, receive, send)` calls into an ASGI application.
- **`sonix/app/`** — the "Starlette/FastAPI" layer. Routing, middleware, DI, and the request/response objects. **`sonix/app/**` must never import from `sonix.server`** — it depends only on `(scope, receive, send)` and `sonix.types`. `tests/test_layering.py` enforces this mechanically, so a violation shows up as a failing test, not a review comment.
- **`sonix/types.py`** sits at the top level, sibling to both, holding only the shared ASGI aliases (`Scope`, `Receive`, `Send`, `Message`, `ASGIApp`) and no logic — so depending on the shared contract never means depending on the other layer.

Two further rules that already shape the existing code:

- `server/parser.py` imports no `asyncio` and does no I/O. Framing decisions (`Content-Length` vs. chunked, where a request ends) live there and **only** there — `server/protocol.py` acts on parser events and never re-inspects headers to reach its own framing conclusion. The same request being parsed two different ways by two different pieces of code is the classic request-smuggling root cause.
- Both layers are testable without a socket: the parser by feeding it bytes, the app layer by handing it a hand-built scope with fake `receive`/`send`. Tests mirror the source layout under `tests/server/` and `tests/app/`.
