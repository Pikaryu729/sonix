# Sonix

[![CI](https://github.com/Pikaryu729/sonix/actions/workflows/ci.yml/badge.svg)](https://github.com/Pikaryu729/sonix/actions/workflows/ci.yml)
[![Python 3.14+](https://img.shields.io/badge/python-3.14+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Runtime dependencies: 0](https://img.shields.io/badge/runtime%20dependencies-0-brightgreen.svg)](#zero-dependencies-and-why-that-is-checked-not-claimed)

**An async web framework built from scratch on `asyncio` — including the HTTP
server underneath it.**

Sonix is not a wrapper around uvicorn. It implements the whole stack: the
HTTP/1.1 parser, the `asyncio.Protocol` connection handling, the ASGI bridge,
routing, and the request/response objects. The goal is to make explicit what a
decorator like `@app.get(...)` actually *does* — route registration, request
parsing, handler dispatch, response serialization — rather than to add another
framework to the ecosystem.

```python
from sonix import Sonix, Request

app = Sonix()


@app.get("/")
async def hello(request: Request) -> dict:
    return {"message": "Hello, world!"}


@app.get("/items/{item_id:int}")
async def get_item(request: Request) -> dict:
    # item_id is already coerced to int -- a non-numeric segment never
    # matched this route in the first place.
    return {"item_id": request.path_params["item_id"]}
```

```console
$ uv run sonix
Sonix serving on http://127.0.0.1:8000 (Ctrl+C to stop)
```

## What is actually hand-rolled

Everything below the application code. Specifically:

- **An incremental HTTP/1.1 parser** (`server/parser.py`) — a pure, synchronous
  state machine with no `asyncio` import and no I/O. Handles `Content-Length`
  and chunked bodies, request pipelining, and enforces size limits *during*
  accumulation rather than after buffering.
- **Request-smuggling defenses**, on a strict "reject, never resolve" posture:
  `Content-Length` together with `Transfer-Encoding` is refused outright, as are
  conflicting duplicate `Content-Length` headers, any `Transfer-Encoding` that
  isn't exactly `chunked`, and `obs-fold` header continuations. Where RFC 9112
  permits a server to either reject or normalize, Sonix rejects — two components
  disagreeing about where a request ends is the classic smuggling root cause.
- **A raw `asyncio.Protocol` server** (`server/protocol.py`) — not
  `start_server`/`StreamReader`, because `Protocol` gives direct access to
  `pause_reading()`/`resume_reading()` and write-buffer limits, which is what
  real backpressure and slow-loris defense require.
- **A write turnstile** so pipelined requests, which run as concurrent tasks,
  still have their responses written back in request order.
- **Routing** with compiled path templates, typed path parameters
  (`{id:int}`, `{name:str}`, `{rest:path}`), and a correct 404-vs-405
  distinction that accumulates an `Allow` header from every route that matched
  the path but not the method.

## The two-layer split, and why it is mechanically enforced

Sonix is deliberately two projects in one repository:

```
sonix/
├── types.py     shared ASGI aliases -- no logic, so depending on the
│                contract never means depending on the other layer
├── server/      the "uvicorn" layer: TCP, HTTP/1.1 parsing, the ASGI bridge.
│                Its job ends when bytes become (scope, receive, send).
└── app/         the "Starlette/FastAPI" layer: routing, requests, responses.
                 Depends only on (scope, receive, send) -- never on server/.
```

The interesting constraint is that **`sonix/app/**` may never import
`sonix.server`**. That is not a code-review convention; `tests/test_layering.py`
walks the AST of every module under `app/` and fails if one does.

The payoff is checked rather than asserted. CI runs a conformance suite in
**both directions**:

- **FastAPI and Starlette applications, unmodified, served by Sonix's HTTP
  server** — including request bodies, streaming responses with no
  `Content-Length`, background tasks, and a client that disconnects mid-stream.
- **A Sonix application served by uvicorn**, launched in a subprocess with an
  import string exactly as a deployment would.

Passing your own unit tests shows the code agrees with itself. Running someone
else's framework shows it agrees with the spec.

## Zero dependencies, and why that is checked, not claimed

`[project].dependencies` is empty and stays empty. `tests/test_packaging.py`
enforces it two ways: it asserts the packaging metadata declares no runtime
dependencies and no extras, and it walks every module under `src/sonix/`
asserting that nothing imports outside the standard library.

FastAPI, uvicorn, Starlette, and h11 *do* appear in this repository — in
isolated `bench` and `conformance` dependency groups, used to measure Sonix and
to cross-check it. They are never installed by a plain `uv sync`, and the test
above is what keeps that boundary honest.

## Installation

Requires Python 3.14+ and [uv](https://docs.astral.sh/uv/).

```console
$ git clone https://github.com/Pikaryu729/sonix
$ cd sonix
$ uv sync
$ uv run sonix
```

## Development

```console
$ uv run pytest              # test suite
$ uv run ruff format .       # format
$ uv run ruff check .        # lint
$ uv run ty check            # type check
```

All four run in CI on every push and pull request.

The conformance suite needs FastAPI, Starlette and uvicorn, which live in an
isolated dependency group so a plain `uv sync` never installs them:

```console
$ uv run --group conformance pytest tests/conformance
```

Without that group the directory is skipped rather than failing, so the
zero-dependency development path stays intact.

## Benchmarks

> **Status: not yet published.** The harness and methodology are specified but
> the numbers are not measured. This section will carry the results table, and
> deliberately does not carry a placeholder number in the meantime.

The plan is a comparison designed so that each cell varies one thing:

| Server | Application |
| --- | --- |
| Sonix | Sonix |
| uvicorn (`--http h11 --loop asyncio`) | FastAPI / Starlette / bare ASGI |
| uvicorn (default: httptools + uvloop) | FastAPI / Starlette / bare ASGI |
| uvicorn (`--http h11 --loop asyncio`) | **Sonix** |

That last row is the point. Holding the *application* constant across rows 1
and 4 isolates the cost of Sonix's server layer; holding the *server* constant
across rows 4 and 2 isolates the cost of its application layer. Reporting a
single "Sonix vs FastAPI" number would conflate the two.

Sonix is pure Python, so the honest expectation — stated here before the
measurement, not after — is that it lands near uvicorn's pure-Python `h11`
configuration and well behind its C-accelerated `httptools`/`uvloop` default.
Both comparisons will be published, along with p50/p99/p99.9 latencies rather
than mean throughput alone.

## Project status

Sonix follows the ten-step build order in
[`docs/architecture.md`](docs/architecture.md), which is the reference document
for the design and the reasoning behind it.

| | Step | |
| --- | --- | --- |
| ✅ | 1–2 | ASGI type contract, HTTP/1.1 parser |
| ✅ | 3 | `asyncio.Protocol` server and ASGI bridge |
| ✅ | 4–6 | Requests, responses, routing, the `Sonix` app class |
| ⬜ | 7–8 | Dependency injection, middleware and exception handling |
| ⬜ | 9 | WebSockets |
| ⬜ | 10 | Hardening pass and published benchmarks |

## Limitations and non-goals

Sonix is a learning and signal project, and a few things are deliberately out of
scope rather than merely unbuilt:

- **No HTTP/2 or TLS.** Terminate TLS at a reverse proxy.
- **No multi-worker process manager.** One event loop, one process.
- **No request-body model validation.** `await request.json()` is the body
  story; there is no pydantic equivalent and there will not be one.
- **No OpenAPI schema generation.** Large surface area, and it would say very
  little about the systems-level questions this project exists to answer.
- **No trailing-slash redirects.** `/items` and `/items/` are distinct routes.
  Implicit redirects are a known source of subtle bugs, including body loss on a
  misconfigured POST redirect.
- **Linear route matching, not a trie.** A trie wins asymptotically, but
  tracking "path matched, method didn't" through a trie walk costs real
  complexity, and at realistic route counts the parser dominates the profile.
  Starlette and FastAPI make the same choice.

## License

MIT — see [LICENSE](LICENSE).
