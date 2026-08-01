# Sonix Architecture

Sonix is an async web framework built from scratch on stdlib `asyncio`, with **zero external runtime dependencies**. The project is split into two layers:

1. **`sonix.server`** — a raw asyncio TCP server plus a hand-rolled HTTP/1.1 parser, playing the role uvicorn plays for FastAPI. Its job ends at the ASGI boundary: it turns bytes on a socket into `(scope, receive, send)` calls into an ASGI application.
2. **`sonix.app`** — the ASGI *application* framework, playing the role Starlette/FastAPI play. Routing, middleware, dependency injection, and the request/response objects all live here, and this layer only ever depends on `(scope, receive, send)` — never on `sonix.server` internals. That means a Sonix application should, in principle, run under any ASGI server, not just Sonix's own.

Directory structure enforces this boundary rather than relying only on convention: `sonix/app/**` must never import from `sonix.server`. A dedicated test (`tests/test_layering.py`) checks this mechanically.

This document is the reference for that split, the key design decisions inside each layer, and the order in which the framework will be built. No implementation exists yet — this describes what will be built.

## Module layout

```
src/sonix/
  __init__.py            # public API re-exports: Sonix, Request, Response, JSONResponse,
                          # PlainTextResponse, HTMLResponse, Depends, WebSocket, HTTPException
  types.py                # Scope, Receive, Send, ASGIApp type aliases only — no logic.
                          # Lives at the top level (sibling to server/ and app/) because it's
                          # the shared ASGI contract both layers reference; putting it inside
                          # either package would make the other import across the boundary.
  server/
    __init__.py
    parser.py             # HTTP/1.1 parser — pure, sync, no asyncio import
    protocol.py           # asyncio.Protocol, connection lifecycle, ASGI bridge, keep-alive
    websockets.py         # WS handshake (Sec-WebSocket-Accept) + frame codec — bytes-only,
                          # protocol-level, no ASGI-app concerns
  app/
    __init__.py
    requests.py           # Request object (scope + receive)
    responses.py          # Response / JSONResponse / PlainTextResponse / HTMLResponse
    routing.py            # Router/Route: path compiling, param coercion, 404/405
    middleware.py         # ASGI-onion composition + ExceptionMiddleware
    di.py                 # signature inspection, Depends, resolution plan, per-request caching
    websockets.py         # WebSocket class built purely from (scope, receive, send)
    applications.py       # Sonix app class: @app.get/@app.websocket, dispatch wiring
    exceptions.py         # HTTPException and friends
```

Tests mirror this 1:1 under `tests/`:

```
tests/
  server/
    test_parser.py
    test_protocol.py
    test_websockets.py
  app/
    test_requests.py
    test_responses.py
    test_routing.py
    test_middleware.py
    test_di.py
    test_websockets.py
    test_applications.py
    test_exceptions.py
  test_layering.py        # architecture conformance: fails if sonix/app/** imports sonix.server
```

**Aside, not addressed by this document:** `pyproject.toml` currently lists `asyncio>=4.0.0` under `[project.dependencies]`. This is a leftover mistake — `asyncio` is part of the Python 3.14 standard library — and should be removed the next time that file is touched, since the project's stated goal is zero runtime dependencies.

## Layer 1 — `server/` (the "uvicorn" layer)

### `server/parser.py` — HTTP/1.1 parser

An incremental state-machine parser (`START_LINE → HEADERS → BODY → COMPLETE`), pure and synchronous — it has no `asyncio` import and is unit-testable by feeding it raw bytes with no event loop involved.

```python
class HTTP11Parser:
    def __init__(self, *, max_header_size=..., max_headers=..., max_body_size=...): ...
    def feed_data(self, data: bytes) -> list[Event]: ...
    def feed_eof(self) -> None: ...


class HTTPParserError(Exception): ...


class MalformedRequest(HTTPParserError): ...


class RequestTooLarge(HTTPParserError): ...
```

Events are small dataclasses: `RequestHeadComplete(head)`, `BodyChunk(data, more_body)`, `RequestComplete()`.

**Resource-exhaustion defense:** header and body size limits are enforced *during accumulation*, not after the full request has been buffered. "Buffer everything, then check" is the vulnerability, not a mitigation of it.

**Request-smuggling defense — reject, never resolve:**
- `Content-Length` **and** `Transfer-Encoding` both present → reject (400), unconditionally.
- Multiple `Content-Length` headers with differing values → reject.
- `Transfer-Encoding` present but not exactly `chunked` (odd casing, extra codings, whitespace tricks) → reject.
- Header folding / `obs-fold` continuation lines → reject rather than join. RFC 9112 §5.2 permits either rejecting or replacing with a space; rejecting is the stricter choice and consistent with this project's "reject, don't resolve" posture throughout.
- Malformed request line (bad method token, missing SP, control characters, wrong token count) → 400.

**Pipelining:** a single `feed_data()` call must be able to return events for more than one complete request. Leftover bytes remain in the parser's internal buffer for the next request cycle rather than being discarded — independently testable by feeding two concatenated requests and asserting two `RequestComplete` sequences, with zero event-loop involvement.

### `server/protocol.py` — connection handling and the ASGI bridge

Built on a raw `asyncio.Protocol`/transport, **not** `asyncio.start_server`/`StreamReader`/`StreamWriter`. Streams wrap `Protocol` anyway and add buffering/locking overhead irrelevant to a `wrk` comparison against a `Protocol`-based uvicorn; more importantly, `Protocol` gives direct access to `transport.pause_reading()`/`resume_reading()` and write-buffer limits, which is what real backpressure and slow-loris defense require. The cost is more hand-rolled state (a manual "feed the parser, react to events" loop instead of `await reader.read()`), which is accepted since making the low-level mechanics legible is the point of the project.

Connection lifecycle:

1. **`connection_made(transport)`** — store the transport, instantiate a fresh `HTTP11Parser`, start a slow-loris timeout that closes the connection if a complete request head hasn't arrived within N seconds.
2. **`data_received(data)`** — feed bytes into the parser, which returns a list of events (supporting pipelining, since one `data_received` call can complete more than one request). On a head-complete event, build the ASGI `scope` and `asyncio.create_task()` the application call. On body-chunk events, push into a per-request `asyncio.Queue` that backs `receive()`. On a parser error, write the appropriate 4xx response and close — no resync attempts, no keep-alive after an error.
3. **`send()` bridge** — writes `http.response.start`/`http.response.body` to `transport.write()`, and validates that `start` always precedes `body`. This is a defensive ASGI-contract check baked into the bridge, not just an assumption about well-behaved application code.
4. **Keep-alive** — after a full request/response cycle, the transport is reused unless `Connection: close` was requested; responses to pipelined requests are written back in request order.
5. **`connection_lost(exc)`** — delivers `{"type": "http.disconnect"}` to a `receive()` call that's currently awaiting, and cancels the in-flight application task cleanly (no swallowed `CancelledError`) instead of leaking it.
6. **Concurrency** — one task per in-flight request, with state scoped to the `HTTPProtocol` instance (i.e., per-connection). No mutable state is shared across connections.

**Design principle:** `protocol.py` never independently inspects headers to decide how a request body is framed — it only acts on events that `parser.py` emits. This single-source-of-truth rule is what prevents the classic smuggling root cause: the same request being parsed two different ways by two different pieces of code.

### `server/websockets.py` — handshake and frame codec

Protocol-level only, no application-facing API:

- On seeing `Upgrade: websocket` + `Connection: Upgrade` + `Sec-WebSocket-Key` on a `GET`, compute `Sec-WebSocket-Accept = base64(sha1(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))` using stdlib `hashlib`/`base64`, write `101 Switching Protocols`, then switch the connection from the HTTP message pump to a frame-based one.
- Frame codec as pure functions on bytes — unit-testable exactly like the HTTP parser, no event loop required: `encode_frame` (server frames are always unmasked) and `decode_frame` (client frames **must** be masked; unmasked client frames are rejected outright, per RFC 6455).
- Control frames handled inline: ping → pong, close → echo-close then close the transport.
- A max frame/message size is enforced, with oversized frames rejected via close code 1009 rather than buffered without bound — the DoS defense for WebSocket connections.

## The ASGI bridge (`types.py` + bridge logic in `server/protocol.py`)

`types.py` holds only the shared contract — `Scope`, `Receive`, `Send`, `ASGIApp` type aliases — with no logic, so both layers can depend on it without depending on each other.

Concrete HTTP scope:

```python
{
    "type": "http",
    "asgi": {"version": "3.0", "spec_version": "2.3"},
    "http_version": "1.1",
    "method": "GET",
    "scheme": "http",
    "path": "/items/42",
    "raw_path": b"/items/42",
    "query_string": b"limit=10",
    "root_path": "",
    "headers": [(b"host", b"example.com"), ...],  # lowercase byte-string tuples
    "client": (host, port),
    "server": (host, port),
}
```

Sequencing guarantees are enforced defensively at the bridge, not just assumed:

- The first message `send()` receives for a request must be `http.response.start`; sending `http.response.body` first is a bridge-level error rather than being silently accepted.
- `more_body` is respected in both directions — body chunks stream through `receive()` progressively rather than requiring the bridge to fully buffer the body before invoking the application. An app-layer `await request.body()` convenience can drain the queue itself if it wants "just give me all the bytes."
- Calling `receive()` again after the body has been fully consumed and the client has disconnected must return `http.disconnect`, not hang.

## Layer 2 — `app/` (the "Starlette/FastAPI" layer)

### `app/routing.py`

A linear list of compiled route patterns, evaluated in registration order — **not a trie**. A trie wins asymptotically on lookup, but correctly tracking "the path shape matched, but the method didn't" (needed to distinguish 404 from 405) through a trie walk adds real implementation complexity, and at the route counts a project like this will realistically see, the parser/protocol layer — not routing — is where benchmark time is actually spent. This also matches what Starlette/FastAPI themselves do, which keeps a `wrk` comparison focused on the part of Sonix that's actually novel (the hand-rolled server and parser) rather than comparing two different routing data structures.

- Path templates such as `/items/{id:int}` are compiled once at registration time into a regex with named groups, plus a converter registry (`str` by default, excluding `/`; `int`, `float`, `uuid`; `path` for catch-all segments). Coercion happens *as part of matching* — the `int` converter's regex is a numeric character class, not `.+` — so "the path shape matched but the segment couldn't be coerced" is not a reachable state; a non-numeric segment simply fails to match that route and falls through.
- **Trailing slash:** strict, no implicit redirect. `/items` and `/items/` are distinct routes. Implicit redirect-on-trailing-slash (as Starlette/FastAPI default to) is a known source of subtlety, including POST body loss across a misconfigured redirect; it can be offered later as an opt-in, not as core behavior.
- **Precedence:** registration order, first match wins — explicit and legible, rather than an automatic specificity-scoring system that trades one kind of surprise for another.
- **404 vs. 405:** a full scan is required on every request, since a route can't be ruled out until both its path and method are checked. If no route matches the path shape at all, respond 404. If at least one route matches the path shape but none match the method, respond 405 with an `Allow` header accumulated from every method that *did* match the path.
- **Path traversal:** routing itself has no filesystem semantics. This is called out explicitly so that any future static-file-serving feature is responsible for resolving against a whitelisted root and rejecting `..`/encoded escapes — it must not be silently assumed to be routing's job.

### `app/middleware.py`

**ASGI-onion wrapping**: each middleware is `middleware(app) -> new_app`, where `new_app.__call__(scope, receive, send)` does pre-work, awaits the inner app (optionally wrapping `receive`/`send` to observe or transform messages), then does post-work. This is deliberately not a before/after hook list — a hook list can't express streaming interception of a response body that arrives as multiple `http.response.body` events, whereas onion wrapping is a single composition model shared with the server bridge and the router, rather than a second one to learn.

Middleware authoring shape:

```python
class SomeMiddleware:
    def __init__(self, app, **opts):
        self.app = app

    async def __call__(self, scope, receive, send): ...
```

`Sonix` wraps middlewares around the router in reverse registration order, so the first-registered middleware ends up outermost — the same ordering semantics as Starlette.

The built-in `ExceptionMiddleware` catches `HTTPException` and converts it to a `Response`, and catches unhandled exceptions and converts them to a 500 (with a debug flag to re-raise instead, for tests). This is what turns a DI failure or a handler bug into an HTTP response instead of a crashed connection.

Dependency resolution happens *inside route dispatch*, not as a separate middleware layer — it's route- and handler-signature-specific, not a cross-cutting scope-level concern.

### `app/di.py`

Handler signatures are inspected via `inspect.signature()`/`get_type_hints(include_extras=True)` **once, at route-registration time** — a resolution plan is built once per handler, not re-inspected on every request.

The canonical dependency marker is `Annotated[X, Depends(get_db)]`, with a default-value `= Depends(...)` form documented as a simpler fallback. `Annotated` requires slightly more parsing but keeps default-value slots free for actual parameter defaults, which the classic default-value-only style can't do — worth the extra parsing cost on greenfield 3.14 code.

Resolution order per parameter:

1. `Depends(...)` marker — resolved recursively, with sub-dependencies following the same rules.
2. Path parameters — taken from the dict routing has already coerced.
3. Query parameters — coerced according to the annotation (`int`/`float`/`bool`/`str`); missing with no default → 422.
4. Special types — `Request`, `WebSocket`.

**Caching/scoping:** the resolution *plan* is built once per route and cached on the handler. Per-request, a request-scoped cache (keyed by the dependency callable, held on the request/scope) ensures that a `Depends(get_db)` used by two parameters in the same request is invoked only once — mirroring FastAPI's default behavior. An explicit `use_cache=False` opt-out is a nice-to-have, not required for v1.

**Failure mode:** an unresolvable parameter — no path/query match, no default, no `Depends`, not a recognized special type — raises a `DependencyResolutionError` naming the handler and the parameter **at decoration time** (i.e., when `@app.get(...)` executes), not at request time. This is both cheaper and louder than FastAPI's own deferred-to-request-time behavior. Runtime failures inside a dependency callable itself (e.g. a database connection error) propagate as ordinary exceptions through `ExceptionMiddleware` into a 500 — they are never silently swallowed.

### `app/websockets.py`

A `WebSocket` class built purely from `(scope, receive, send)`:

```python
await ws.accept()
await ws.receive_text() / await ws.receive_bytes() / await ws.receive_json()
await ws.send_text(...) / await ws.send_bytes(...)
await ws.close(code=...)
```

Because this class only ever touches `scope`/`receive`/`send`, it is fully runtime-agnostic even though the underlying handshake and frame mechanics live in `server/websockets.py` — a direct, concrete validation of the two-layer design's central claim.

`@app.websocket("/ws")` reuses `app/routing.py`'s `Router`, matching on path **and** `scope["type"]`, so an HTTP-only route and a WebSocket-only route can share the same path without colliding.

### `app/applications.py` and the public API

What `@app.get("/items/{id}")` wires up, end to end:

1. **Registration.** `Sonix.get(path)` calls `Sonix.route(path, methods=["GET"])`, which compiles the path via `routing.compile_path`, builds a DI plan via `di.build_plan(handler)` (the handler's signature is inspected exactly once, here), and appends a `Route(pattern, converters, methods, handler, di_plan)` to the router. The decorator returns the handler unmodified — registration is its only side effect.
2. **Dispatch.** At request time, `server/protocol.py` calls `await app(scope, receive, send)` on the fully wrapped middleware onion. Innermost, `Router.__call__` — itself an ASGI app — matches the request, extracts and coerces path parameters, builds a `Request(scope, receive)`, resolves the DI plan (path/query parameters plus recursive `Depends` resolution, with per-request caching), and calls the handler: `await handler(**kwargs)` directly if it's a coroutine function, otherwise run via `asyncio.to_thread` so a synchronous handler never blocks the event loop.
3. **Response construction.** A handler may return a `Response` instance directly, or a plain value (`dict`/`list`/`str`/`None`), which dispatch wraps into a default `JSONResponse` or `PlainTextResponse`. This keeps FastAPI's "just return a dict" ergonomics without pydantic: serialization is a straightforward `json.dumps` over dict/list/dataclass/primitive values. Request-body model validation is explicitly out of scope for this document — at most a future stretch goal built on stdlib `dataclasses`.
4. **Sending.** `Response` is itself an ASGI callable — `__call__(scope, receive, send)` emits `http.response.start` then `http.response.body` — so dispatch's final step is uniformly `await response(scope, receive, send)`, whether the handler returned a `Response` directly or dispatch constructed one.

**Public API**, re-exported from `sonix/__init__.py`: `Sonix`, `Request`, `Response`, `JSONResponse`, `PlainTextResponse`, `HTMLResponse`, `Depends`, `WebSocket`, `HTTPException`. Handler authors never import `sonix.server` or `sonix.parser` directly — everything they need comes from the top-level package or `sonix.app`.

## Build order

The build order is chosen so that each step is independently testable or demoable, front-loading the highest-risk and most novel code (the parser and the raw `Protocol`-based server) while it's cheapest to isolate, and deferring the well-trodden pieces (routing, DI — which look like every other Python framework) until there's already a working round trip to validate them against.

1. **`types.py`** — shared ASGI type aliases only. Unblocks both layers against a common contract.
2. **`server/parser.py`** — pure, no `asyncio`. Demo: feed raw bytes and assert the resulting parsed events; feed a `Content-Length`/`Transfer-Encoding` conflict and other malformed input and assert rejection. Build and stress-test this in total isolation before anything depends on it.
3. **`server/protocol.py`** — wired to a trivial hardcoded ASGI app (a fixed "hello world" response). Demo: `curl` against `uv run sonix`, or a real socket round-trip test. This proves layer 1 end-to-end before layer 2 exists at all.
4. **`app/requests.py` + `app/responses.py`** — built purely against `types.Scope`/`Receive`/`Send` and tested with a fake scope/receive/send, with `server/` not involved at all. This proves layer 2's runtime-agnosticism starting from the very first module written in it.
5. **`app/routing.py`** — `Router` as an ASGI app, tested standalone against fake scopes.
6. **`app/applications.py`** (first pass, no DI or middleware yet) — `Sonix` plus `@app.get`, wired to `server/protocol.py`. This is the first real `curl`-against-a-running-server milestone, and the first point worth taking a `wrk` benchmark checkpoint.
7. **`app/di.py`** — signature inspection, plan-building, and caching, unit-tested with fake handlers and scopes, then wired into dispatch.
8. **`app/middleware.py`** (with `ExceptionMiddleware`) — onion composition tested against fake inner apps.
9. **`server/websockets.py`** + **`app/websockets.py`** — handshake and frame codec unit-tested purely on bytes (like the HTTP parser), then wired into `protocol.py`'s upgrade path and `applications.py`'s `@app.websocket`.
10. **Hardening pass** — header/body/backlog size limits, slow-loris timeouts, a `wrk` benchmark against FastAPI+uvicorn, and a pass of the findings through code review and security review against the real, now-existing code.

This yields two concrete demoable milestones along the way — step 3's raw HTTP round trip and step 6's first real `@app.get` — instead of one big-bang integration at the very end.
