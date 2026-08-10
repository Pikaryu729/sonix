# Sonix Architecture

Sonix is an async web framework built from scratch on stdlib `asyncio`, with **zero external runtime dependencies**. The project is split into two layers:

1. **`sonix.server`** — a raw asyncio TCP server plus a hand-rolled HTTP/1.1 parser, playing the role uvicorn plays for FastAPI. Its job ends at the ASGI boundary: it turns bytes on a socket into `(scope, receive, send)` calls into an ASGI application.
2. **`sonix.app`** — the ASGI *application* framework, playing the role Starlette/FastAPI play. Routing, middleware, dependency injection, and the request/response objects all live here, and this layer only ever depends on `(scope, receive, send)` — never on `sonix.server` internals. That means a Sonix application should, in principle, run under any ASGI server, not just Sonix's own.

Directory structure enforces this boundary rather than relying only on convention: `sonix/app/**` must never import from `sonix.server`. A dedicated test (`tests/test_layering.py`) checks this mechanically.

The boundary is also validated *empirically*, not just structurally. `tests/conformance/` runs unmodified FastAPI and Starlette applications on Sonix's server, and a Sonix application under uvicorn — so the claim "either half can be swapped for someone else's" is a CI job rather than an assertion in this document. Its third-party dependencies live in a `conformance` dependency group, so a plain `uv sync` never installs them and the suite skips instead of failing.

This document is the reference for that split, the key design decisions inside each layer, and the order in which the framework is built.

**Implementation status.** Steps 1–6 of the [build order](#build-order) are implemented and tested: the type contract, the HTTP/1.1 parser, the protocol/ASGI bridge, request and response objects, routing, and the `Sonix` app class with `@app.get`-style decorators. Step 7 (middleware and exceptions, swapped ahead of DI — see the [build order](#build-order)) is also done, along with two modules that were not numbered steps at all: a public server API (`Config`, `Server`, `sonix.run`) with a `module:app` CLI, and `app/lifespan.py`. Neither was optional. The server API is a prerequisite for the example application and the benchmark harness, neither of which can exist while the only way to serve an app is a private function with a hardcoded demo; lifespan fixed a bug that made a Sonix app unable to run under any other ASGI server. `uv run sonix` serves the built-in demo end-to-end over a real socket, and `uvicorn sonix._demo:app` now serves the same app. Step 8 (dependency injection) is done too. WebSockets are designed below but not yet written — that section describes intent, not existing code. Each section marks its own status.

## Module layout

Modules marked *(planned)* do not exist yet.

```
src/sonix/
  __init__.py            # public API re-exports, the `module:app` import-string
                          # resolver, and the `sonix` console-script entry point.
                          # Exports Sonix, Request, Response, JSONResponse,
                          # PlainTextResponse, HTMLResponse, Config, Server, run,
                          # Depends, HTTPException; WebSocket joins them at step 9.
  _demo.py                # the app `sonix` serves when given no target. Inside the
                          # package rather than under examples/ so a bare `sonix`
                          # works from an installed wheel with nothing on sys.path.
  types.py                # Scope, Message, Receive, Send, ASGIApp type aliases only — no
                          # logic. Lives at the top level (sibling to server/ and app/)
                          # because it's the shared ASGI contract both layers reference;
                          # putting it inside either package would make the other import
                          # across the boundary.
  server/
    __init__.py
    parser.py             # HTTP/1.1 parser — pure, sync, no asyncio import
    protocol.py           # asyncio.Protocol, connection lifecycle, ASGI bridge, keep-alive
    server.py             # Config/Server: socket binding, serve, graceful shutdown,
                          # signal handling. The server-lifecycle counterpart to
                          # protocol.py's connection lifecycle.
    websockets.py         # (planned) WS handshake (Sec-WebSocket-Accept) + frame codec —
                          # bytes-only, protocol-level, no ASGI-app concerns
  app/
    __init__.py
    requests.py           # Request object (scope + receive)
    responses.py          # Response / JSONResponse / PlainTextResponse / HTMLResponse
    routing.py            # Router/Route: path compiling, param coercion, 404/405
    applications.py       # Sonix app class: @app.get/@app.websocket, dispatch wiring
    middleware.py         # ASGI-onion composition + ExceptionMiddleware
    lifespan.py           # startup/shutdown: the async-context-manager form, the
                          # on_startup/on_shutdown sugar, and the scope["state"] merge
    di.py                 # signature inspection, Depends, resolution plan,
                          # per-request caching
    websockets.py         # (planned) WebSocket class built purely from (scope, receive, send)
    exceptions.py         # HTTPException and friends
```

Tests mirror this 1:1 under `tests/`:

```
tests/
  server/
    test_parser.py
    test_protocol.py
    test_websockets.py    # (planned)
  app/
    test_requests.py
    test_responses.py
    test_routing.py
    test_applications.py
    test_middleware.py
    test_lifespan.py
    test_di.py
    test_websockets.py    # (planned)
    test_exceptions.py
  test_types.py           # the shared ASGI aliases stay exported under their known names
  test_layering.py        # architecture conformance: fails if sonix/app/** imports sonix.server
```

`pyproject.toml` declares `dependencies = []`, and that empty list is a design constraint rather than an oversight: anything appearing there that isn't dev tooling is a bug against the project's premise.

## Layer 1 — `server/` (the "uvicorn" layer)

### `server/parser.py` — HTTP/1.1 parser *(implemented)*

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

**Errors carry their partial events.** Pipelining and error handling interact: a `feed_data()` call can complete two perfectly good requests and *then* hit malformed bytes for a third. Raising a bare exception would discard the two good requests along with the bad one, so `HTTPParserError` carries a `partial_events` list holding everything accumulated before the failure. The bridge replays those, then writes its error response — see the turnstile note under `protocol.py`, which is what makes that replay actually reach the client.

**Chunked bodies** are decoded by a nested state machine (`SIZE → DATA → DATA_CRLF → TRAILERS`) sharing the same buffer. Chunk extensions are ignored, trailer headers are discarded, and the accumulated decoded size is checked against `max_body_size` per chunk — so a chunked body can't evade the limit a `Content-Length` body is held to.

### `server/protocol.py` — connection handling and the ASGI bridge *(implemented)*

Built on a raw `asyncio.Protocol`/transport, **not** `asyncio.start_server`/`StreamReader`/`StreamWriter`. Streams wrap `Protocol` anyway and add buffering/locking overhead irrelevant to a `wrk` comparison against a `Protocol`-based uvicorn; more importantly, `Protocol` gives direct access to `transport.pause_reading()`/`resume_reading()` and write-buffer limits, which is what real backpressure and slow-loris defense require. The cost is more hand-rolled state (a manual "feed the parser, react to events" loop instead of `await reader.read()`), which is accepted since making the low-level mechanics legible is the point of the project.

Connection lifecycle:

1. **`connection_made(transport)`** — store the transport, instantiate a fresh `HTTP11Parser`, start a slow-loris timeout that closes the connection if a complete request head hasn't arrived within N seconds.
2. **`data_received(data)`** — feed bytes into the parser, which returns a list of events (supporting pipelining, since one `data_received` call can complete more than one request). On a head-complete event, build the ASGI `scope` and `asyncio.create_task()` the application call. On body-chunk events, push into a per-request `asyncio.Queue` that backs `receive()`. On a parser error, write the appropriate 4xx response and close — no resync attempts, no keep-alive after an error.
3. **`send()` bridge** — writes `http.response.start`/`http.response.body` to `transport.write()`, and validates that `start` always precedes `body`. This is a defensive ASGI-contract check baked into the bridge, not just an assumption about well-behaved application code.
4. **Keep-alive** — after a full request/response cycle, the transport is reused unless `Connection: close` was requested; responses to pipelined requests are written back in request order.
5. **`connection_lost(exc)`** — delivers `{"type": "http.disconnect"}` to a `receive()` call that's currently awaiting, and cancels the in-flight application task cleanly (no swallowed `CancelledError`) instead of leaking it.
6. **Concurrency** — one task per in-flight request, with state scoped to the `HTTPProtocol` instance (i.e., per-connection). No mutable state is shared across connections.

**Design principle:** `protocol.py` never independently inspects headers to decide how a request body is framed — it only acts on events that `parser.py` emits. This single-source-of-truth rule is what prevents the classic smuggling root cause: the same request being parsed two different ways by two different pieces of code.

Three details that only surfaced once this ran against a real socket, all of them consequences of "one task per in-flight request" meeting "one ordered byte stream":

- **The write turnstile.** Pipelined requests run as concurrent tasks, but HTTP/1.1 requires their responses on the wire in request order. On dispatch, each request takes the current turnstile event as the one it must await before writing its response head, and installs a fresh event in its place for whichever request comes next — then sets that fresh event once its own body is complete. The result is a chain of events linking the in-flight requests in arrival order. The error path waits on the same turnstile: a parser failure discovered mid-buffer must not close the transport before the good pipelined responses ahead of it have been written, or they're silently dropped against a closed transport.
- **Dangling heads are dropped, not dispatched.** A parser error's `partial_events` can end mid-request — a `RequestHeadComplete` with no matching `RequestComplete`, because body framing is exactly what failed. Dispatching that head would spawn a task whose `receive()` can never be satisfied; it would hang forever and, through the turnstile, block every response behind it including the error response. Only events belonging to fully completed requests are replayed.
- **Cancellation is deferred by one loop iteration.** `connection_lost` puts `http.disconnect` on every live receive queue and then cancels the in-flight tasks — but via `call_soon`, not directly. `put_nowait` only *schedules* the resumption of a task blocked in `queue.get()`; cancelling synchronously races it, and `Task.cancel()` on a task whose awaitable is already done falls back to `_must_cancel`, discarding the disconnect message the app was about to observe.

### `server/server.py` — server lifecycle *(implemented)*

`protocol.py` owns one *connection*; `server.py` owns the *server*: the listening
socket, the set of live connections, signal handling, and shutdown. It is the public
entry point into layer 1 — `sonix.run(app)` is a thin wrapper over
`Server(Config(app)).run()`.

`Config` is a frozen dataclass surfacing every per-connection limit `HTTPProtocol`
accepts. That parameterization already existed, but nothing forwarded any of it, so
the defaults were the only reachable values.

`Server` deliberately splits `startup()` / `serve_forever()` / `shutdown()` rather
than exposing only a blocking `run()`. Tests and the benchmark harness need to bring a
server up, drive it, and take it down without `serve_forever` owning the event loop;
`run()` is the convenience layer that adds `SIGINT`/`SIGTERM` handling on top.

**Graceful shutdown, and the ordering trap inside it.** The sequence is: close the
listening socket, set `should_exit`, close idle connections, drain busy ones against a
deadline, force-close the remainder, and *only then* `await server.wait_closed()`.

The trap is that last step's position. Since Python 3.12, `asyncio.Server.wait_closed()`
waits not just for the listener but for every existing connection to finish — so
awaiting it up front, which reads like the natural "stop accepting, then clean up",
deadlocks against exactly the connections the drain has not closed yet. The drain
deadline is bounded for a related reason: a hung handler must not be able to keep the
process alive indefinitely.

Two details follow from `ServerState` (which lives in `protocol.py`, so that module
needn't import the one that imports it):

- Connections **register themselves** in `connection_made` and deregister in
  `connection_lost`, rather than the server tracking them from outside — which would
  leak entries for connections that died without notice.
- Once `should_exit` is set, an in-flight response advertises `Connection: close`
  regardless of what `_decide_close` concluded. Telling a client keep-alive on a socket
  the server is about to drop invites it to pipeline a request into the void.

**The CLI** takes a uvicorn-style `module:app` import string. That choice matters
beyond ergonomics: it lets the benchmark harness launch Sonix and uvicorn with
structurally identical command lines, so the comparison isn't quietly measuring two
different startup paths.

### `server/websockets.py` — handshake and frame codec *(planned)*

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

### `app/requests.py` and `app/responses.py` *(implemented)*

`Request` wraps `(scope, receive)`: lazy `Headers` (a case-insensitive read-only view that lowercases both stored keys and lookup keys, so it stays correct against a hand-built scope rather than trusting the parser's normalization), `query_params`, `path_params`, and a streaming `stream()`/`body()`/`json()` trio. A client that disconnects mid-body raises `ClientDisconnect` — deliberately not an `HTTPException`, so `requests.py` stays ignorant of HTTP status codes and a future `ExceptionMiddleware` can map it.

`Response` and its `JSONResponse`/`PlainTextResponse`/`HTMLResponse` subclasses are each ASGI callables in their own right, emitting `http.response.start` then `http.response.body` from `__call__(scope, receive, send)`. That's what lets every other layer finish a request the same way — `await response(scope, receive, send)` — with no special case for who constructed the response.

### `app/routing.py` *(implemented)*

A linear list of compiled route patterns, evaluated in registration order — **not a trie**. A trie wins asymptotically on lookup, but correctly tracking "the path shape matched, but the method didn't" (needed to distinguish 404 from 405) through a trie walk adds real implementation complexity, and at the route counts a project like this will realistically see, the parser/protocol layer — not routing — is where benchmark time is actually spent. This also matches what Starlette/FastAPI themselves do, which keeps a `wrk` comparison focused on the part of Sonix that's actually novel (the hand-rolled server and parser) rather than comparing two different routing data structures.

- Path templates such as `/items/{id:int}` are compiled once at registration time into a regex with named groups, plus a converter registry (`str` by default, excluding `/`; `int`, `float`, `uuid`; `path` for catch-all segments). Coercion happens *as part of matching* — the `int` converter's regex is a numeric character class, not `.+` — so "the path shape matched but the segment couldn't be coerced" is not a reachable state; a non-numeric segment simply fails to match that route and falls through.
- **Trailing slash:** strict, no implicit redirect. `/items` and `/items/` are distinct routes. Implicit redirect-on-trailing-slash (as Starlette/FastAPI default to) is a known source of subtlety, including POST body loss across a misconfigured redirect; it can be offered later as an opt-in, not as core behavior.
- **Precedence:** registration order, first match wins — explicit and legible, rather than an automatic specificity-scoring system that trades one kind of surprise for another.
- **404 vs. 405:** a full scan is required on every request, since a route can't be ruled out until both its path and method are checked. If no route matches the path shape at all, raise `HTTPException(404)`. If at least one route matches the path shape but none match the method, raise `HTTPException(405)` with an `Allow` header accumulated from every method that *did* match the path. These are **raised, not returned** — see `app/middleware.py` below for why, and for what that costs.
- **Path traversal:** routing itself has no filesystem semantics. This is called out explicitly so that any future static-file-serving feature is responsible for resolving against a whitelisted root and rejecting `..`/encoded escapes — it must not be silently assumed to be routing's job.

### `app/lifespan.py` *(implemented)*

**Not in the original build order** — it was added because `Sonix.__call__` delegated to the router unconditionally, so a `lifespan` scope reached `Router.__call__` and died on `scope["path"]` with a bare `KeyError`. A Sonix application therefore could not run under uvicorn at all, which quietly falsified this document's central claim. `Sonix.__call__` now switches on `scope["type"]`.

The primary API is an async context manager rather than `on_startup`/`on_shutdown` lists:

```python
@contextlib.asynccontextmanager
async def lifespan(app):
    connection = sqlite3.connect(...)
    yield {"db": connection}
    connection.close()
```

It keeps setup and its matching teardown in one function and lets the resource live in a local variable between them, which paired event lists cannot express without a module-level global. The event form remains as sugar. Mixing the two is refused rather than given an arbitrary ordering.

Whatever the context manager yields is merged into `scope["state"]` **in place**, not rebound — the server holds a reference to that dict and copies it into every request scope, so replacing it would silently orphan the server's copy. Each request receives a shallow copy, per the ASGI spec: a handler may stash per-request values without leaking them into the next request, while objects the lifespan opened stay shared.

Failures are reported as `lifespan.startup.failed` / `lifespan.shutdown.failed` messages rather than raised — a server that asked for lifespan is waiting on a reply, and raising would leave it waiting forever.

**The distinction the server-side runner exists to get right** is between *"this app does not speak lifespan"* and *"this app's startup genuinely failed"*. Conflating them is a bug uvicorn shipped historically, and it is asymmetric: an app whose database connection fails must crash the server loudly, never be silently downgraded to running without a lifespan. The rule is that a protocol message always raises, while an exception escaping the app before any message means "unsupported" — tolerated under `lifespan="auto"`, fatal under `"on"`.

Detecting only the raising case is not enough. Returning early on an unrecognized scope type is the more common and more polite way for an app to say it has no lifespan, and it sends nothing at all; such an app was being reported as supported and then hung shutdown waiting for a reply that was never coming. Support is judged on whether startup was ever *acknowledged*, not on whether the app raised.

### `app/exceptions.py` *(implemented)*

`HTTPException(status_code, detail=None, headers=None)`, raised rather than returned, so any layer above routing can say "stop, answer with this status" without constructing a `Response`. `detail` defaults to the status's registered reason phrase.

`app/requests.py` deliberately still raises `ClientDisconnect` rather than an `HTTPException`, keeping that module ignorant of status codes; mapping it is the middleware's job.

### `app/middleware.py` *(implemented)*

**ASGI-onion wrapping**: each middleware is `middleware(app) -> new_app`, where `new_app.__call__(scope, receive, send)` does pre-work, awaits the inner app (optionally wrapping `receive`/`send` to observe or transform messages), then does post-work. This is deliberately not a before/after hook list — a hook list can't express streaming interception of a response body that arrives as multiple `http.response.body` events, whereas onion wrapping is a single composition model shared with the server bridge and the router, rather than a second one to learn.

Middleware authoring shape:

```python
class SomeMiddleware:
    def __init__(self, app, **opts):
        self.app = app

    async def __call__(self, scope, receive, send): ...
```

`Sonix` wraps middlewares around the router in reverse registration order, so the first-registered middleware ends up outermost. **One rule applies to both** the constructor list and successive `add_middleware()` calls — Starlette's `add_middleware` is LIFO while its constructor list is not, and a single consistent rule is easier to reason about than reproducing that split.

The stack is built **once, lazily, on the first request**, because it cannot be assembled until every route and middleware is registered. Registering middleware or an exception handler after that point raises, rather than silently having no effect.

The built-in `ExceptionMiddleware` catches `HTTPException` and converts it to a `Response`, and catches unhandled exceptions and converts them to a 500 (with a debug flag to re-raise instead, for tests). This is what turns a DI failure or a handler bug into an HTTP response instead of a crashed connection. It sits **outermost**, so it also catches failures raised by other middleware, not only by handlers.

Two details that only became apparent once it existed:

- **It wraps `send` to track whether `http.response.start` has already been emitted.** Once the status line is on the wire, an exception cannot be turned into an error response — emitting a second `http.response.start` would corrupt the stream. In that case the exception is re-raised and the server closes the connection, which is the only honest option. Getting this wrong is a classic.
- **`ClientDisconnect` is swallowed, not converted to a 500.** There is nobody left to answer, and writing to a closed transport is the only thing that could still go wrong.

Handlers may be registered against an exception class or, for `HTTPException`, against a **status code**, so a custom 404 needs no subclass; status lookup wins over class lookup.

**404 and 405 are raised, not returned.** `Router` previously constructed `PlainTextResponse` for them inline, which made them the only two statuses in the framework with no override hook. They are now `HTTPException`s converted like any other error, with the 405's `Allow` header riding along on the exception so a custom handler can still read it. The tradeoff is that `Router` is no longer a complete ASGI app on its own — it expects the `ExceptionMiddleware` that `Sonix` always wraps it in.

Dependency resolution happens *inside route dispatch*, not as a separate middleware layer — it's route- and handler-signature-specific, not a cross-cutting scope-level concern.

### `app/di.py` *(implemented)*

Handler signatures are inspected via `inspect.signature()`/`get_type_hints(include_extras=True)` **once, at route-registration time** — a resolution plan is built once per handler, not re-inspected on every request.

The canonical dependency marker is `Annotated[X, Depends(get_db)]`, with a default-value `= Depends(...)` form documented as a simpler fallback. `Annotated` requires slightly more parsing but keeps default-value slots free for actual parameter defaults, which the classic default-value-only style can't do — and, concretely, `value: int = Depends(f)` lies to a type checker about what the default is, which `Annotated` does not.

**Two orderings, and they are not the same thing.** Conflating them was the design's biggest correction.

*Classification precedence*, decided per parameter at decoration time:

1. `Depends(...)` marker — resolved recursively, with sub-dependencies following the same rules.
2. Special types — `Request` (`WebSocket` joins it at step 9).
3. Path parameters — taken from the dict routing has already coerced.
4. Query parameters — coerced according to the annotation.

*Execution order*, per request: **every scalar in the whole dependency graph is resolved first**, collecting all failures, and the 422 is raised **before any dependency callable runs**. That is what stops a malformed query string from leaving a half-opened database connection behind, and it is why a client sees every mistake in one response instead of one per round trip.

**Coercion** covers `str`, `int`, `float`, `bool`, `uuid.UUID`, plus `X | None`, defaults, and `list[X]` over repeated query parameters. `bool` uses an explicit token table (`1/true/t/yes/y/on` and their negatives) rather than Python truthiness, since `bool("false")` is `True`. A valueless `?flag` parses to `""`, which is in neither table and is therefore rejected — the same reject-never-resolve posture the HTTP parser takes.

The coercion table is keyed by *type object* and is deliberately **separate** from `routing.CONVERTERS`, which is keyed by template converter *name*. The conversion callables coincide for the four shared types and a test pins that they cannot drift, but merging the two lookups would mean every new path converter silently became a query-parameter type.

**Path parameters are injected without re-coercion** — routing already converted them. An annotation that disagrees with the template (`/items/{id:int}` with `def h(id: str)`, or `/items/{id}` with `def h(id: int)`) is a **decoration-time error in both directions**. Keeping the template as the single source of truth for path types is what preserves the routing invariant that "the path shape matched but the segment couldn't be coerced" is unreachable.

**Generator dependencies.** A `yield` dependency is wrapped as a context manager on a per-request `contextlib.AsyncExitStack`, which is entered around *sending the response as well as* calling the handler — closing a connection before the rows it produced have been serialized would be a subtle bug. Teardown is LIFO, so a transaction opened inside a connection closes before it. Standard `contextlib` semantics apply and are pinned by a test: cleanup that must run on error needs `try`/`finally`, because a bare `yield` has the exception thrown in at the yield point.

Sync dependency callables run via `asyncio.to_thread`. Sync *generator* dependencies are entered and exited on the event-loop thread instead — splitting the two halves across worker threads would break any resource with thread affinity, and `sqlite3` connections default to `check_same_thread=True`.

**Caching/scoping:** the resolution *plan* is built once per route and closed over by the endpoint. Per-request, a cache keyed by the dependency callable ensures a `Depends(get_db)` used by two parameters is invoked once; `use_cache=False` opts out. The cache is created per request, so nothing leaks between them.

**Where the plan lives — a correction to this document.** Earlier drafts hung the plan off `Route.di_plan`. That field is now **deleted**. By the time a `Route` exists the handler has already been wrapped into an ASGI app closing over its own plan, which is where the signature was known in the first place; honouring a plan inside `Router.__call__` would require routing to build a `Request` and coerce a response, i.e. to absorb the endpoint wrapper and stop being a generic ASGI mounter.

**Fast paths.** The plan precomputes `needs_teardown` and `is_trivial`. Without them every request would allocate an `AsyncExitStack` with nothing to close, and a handler taking only a `Request` — the pre-DI convention, and the shape of the benchmark's own handlers — would pay for machinery it does not use. `is_trivial` dispatches straight through.

**Failure mode:** a parameter with an unsupported annotation and no `Depends`, a variadic or positional-only parameter, an annotation disagreeing with the path template, two `Depends` on one parameter, or a dependency cycle — all raise `DependencyResolutionError` naming the handler and parameter **at decoration time** (when `@app.get(...)` executes), not at request time. Note the failure condition is *unsupported annotation*, not "no query parameter matched": every parameter name is a potential query parameter, so whether one will match is unknowable until a request arrives. Runtime failures inside a dependency callable propagate as ordinary exceptions through `ExceptionMiddleware` into a 500.

**A Python limitation worth knowing.** Under `from __future__ import annotations` (PEP 563) an annotation is a string that `get_type_hints` evaluates against the defining module's globals, so `Annotated[X, Depends(f)]` where `f` is a *local* cannot be resolved — the name lives nowhere the evaluator can see. Module-level dependencies, which is what applications write, are unaffected. Sonix reports this as a `DependencyResolutionError` naming the handler rather than letting a bare `NameError` escape at import time.

### `app/websockets.py` *(planned)*

A `WebSocket` class built purely from `(scope, receive, send)`:

```python
await ws.accept()
await ws.receive_text() / await ws.receive_bytes() / await ws.receive_json()
await ws.send_text(...) / await ws.send_bytes(...)
await ws.close(code=...)
```

Because this class only ever touches `scope`/`receive`/`send`, it is fully runtime-agnostic even though the underlying handshake and frame mechanics live in `server/websockets.py` — a direct, concrete validation of the two-layer design's central claim.

`@app.websocket("/ws")` reuses `app/routing.py`'s `Router`, matching on path **and** `scope["type"]`, so an HTTP-only route and a WebSocket-only route can share the same path without colliding.

### `app/applications.py` and the public API *(implemented, first pass)*

All four stages below exist today. A handler may declare whatever parameters it needs, and the one-argument `def handler(request: Request)` convention that predates DI keeps working as a degenerate plan -- every test written against it passed unedited when DI landed, which is the compatibility guarantee stated as a fact rather than an intention.

What `@app.get("/items/{id}")` wires up, end to end:

1. **Registration.** `Sonix.get(path)` calls `Sonix.route(path, methods=["GET"])`, which compiles the path via `routing.compile_path`, builds a DI plan via `di.build_plan(handler)` (the handler's signature is inspected exactly once, here), wraps the handler in an endpoint closing over that plan, and appends a `Route(pattern, converters, methods, handler)` to the router. The decorator returns the handler unmodified — registration is its only side effect.
2. **Dispatch.** At request time, `server/protocol.py` calls `await app(scope, receive, send)` on the fully wrapped middleware onion. Innermost, `Router.__call__` — itself an ASGI app — matches the request and extracts and coerces path parameters into the scope, then awaits the endpoint. The endpoint builds a `Request(scope, receive)`, resolves its DI plan (path/query parameters plus recursive `Depends` resolution, with per-request caching), and calls the handler: `await handler(**kwargs)` directly if it's a coroutine function, otherwise run via `asyncio.to_thread` so a synchronous handler never blocks the event loop.
3. **Response construction.** A handler may return a `Response` instance directly, or a plain value (`dict`/`list`/`str`/`None`), which dispatch wraps into a default `JSONResponse` or `PlainTextResponse`. This keeps FastAPI's "just return a dict" ergonomics without pydantic: serialization is a straightforward `json.dumps` over dict/list/dataclass/primitive values. Request-body model validation is explicitly out of scope for this document — at most a future stretch goal built on stdlib `dataclasses`.
4. **Sending.** `Response` is itself an ASGI callable — `__call__(scope, receive, send)` emits `http.response.start` then `http.response.body` — so dispatch's final step is uniformly `await response(scope, receive, send)`, whether the handler returned a `Response` directly or dispatch constructed one.

**Public API**, re-exported from `sonix/__init__.py`: `Sonix`, `Request`, `Response`, `JSONResponse`, `PlainTextResponse`, `HTMLResponse`, `Depends`, `WebSocket`, `HTTPException`. Handler authors never import `sonix.server` or `sonix.parser` directly — everything they need comes from the top-level package or `sonix.app`.

## Build order

The build order is chosen so that each step is independently testable or demoable, front-loading the highest-risk and most novel code (the parser and the raw `Protocol`-based server) while it's cheapest to isolate, and deferring the well-trodden pieces (routing, DI — which look like every other Python framework) until there's already a working round trip to validate them against.

1. ✅ **`types.py`** — shared ASGI type aliases only. Unblocks both layers against a common contract.
2. ✅ **`server/parser.py`** — pure, no `asyncio`. Demo: feed raw bytes and assert the resulting parsed events; feed a `Content-Length`/`Transfer-Encoding` conflict and other malformed input and assert rejection. Build and stress-test this in total isolation before anything depends on it.
3. ✅ **`server/protocol.py`** — wired to a trivial hardcoded ASGI app (a fixed "hello world" response). Demo: `curl` against `uv run sonix`, or a real socket round-trip test. This proves layer 1 end-to-end before layer 2 exists at all.
4. ✅ **`app/requests.py` + `app/responses.py`** — built purely against `types.Scope`/`Receive`/`Send` and tested with a fake scope/receive/send, with `server/` not involved at all. This proves layer 2's runtime-agnosticism starting from the very first module written in it.
5. ✅ **`app/routing.py`** — `Router` as an ASGI app, tested standalone against fake scopes.
6. ✅ **`app/applications.py`** (first pass, no DI or middleware yet) — `Sonix` plus `@app.get`, wired to `server/protocol.py`. This is the first real `curl`-against-a-running-server milestone, and the first point worth taking a `wrk` benchmark checkpoint.
7. ✅ **`app/middleware.py`** (with `ExceptionMiddleware` and `app/exceptions.py`) — onion composition tested against fake inner apps. **Swapped with DI from this document's original order.** DI's failure modes — a missing query parameter becoming a 422, a dependency callable raising — have nowhere to go without an exception layer, so building DI first would mean building it against a hole. `app/lifespan.py` landed alongside, since both required `Sonix.__call__` to switch on `scope["type"]` rather than delegate blindly.
8. ✅ **`app/di.py`** — signature inspection, plan-building, and caching, unit-tested with fake handlers and scopes, then wired into dispatch. Two additions beyond the original design: generator (`yield`) dependencies with an `AsyncExitStack`, without which `Depends(get_db)` is a demo rather than a usable feature; and precomputed fast-path flags on the plan, without which step 10's benchmark would have measured an `AsyncExitStack` allocated per request for handlers that need none.
9. ⬜ **`server/websockets.py`** + **`app/websockets.py`** — handshake and frame codec unit-tested purely on bytes (like the HTTP parser), then wired into `protocol.py`'s upgrade path and `applications.py`'s `@app.websocket`.
10. ⬜ **Hardening pass** — header/body/backlog size limits, slow-loris timeouts, a `wrk` benchmark against FastAPI+uvicorn, and a pass of the findings through code review and security review against the real, now-existing code.

This yields two concrete demoable milestones along the way — step 3's raw HTTP round trip and step 6's first real `@app.get` — instead of one big-bang integration at the very end. Both have now landed: `uv run sonix` serves a demo app with `@app.get("/")` and `@app.get("/items/{item_id:int}")` over a real socket.

Note that several step-10 hardening items arrived early, because the protocol layer couldn't be written correctly without them: header/body size limits, the slow-loris head timeout, and read/write backpressure watermarks are already in place. What remains for step 10 is the benchmark and the review passes.
