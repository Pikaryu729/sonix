# Autobahn|Testsuite

The standard RFC 6455 conformance suite: roughly 500 cases covering framing,
fragmentation, UTF-8 handling, close codes, and deliberate protocol abuse.
It drives Sonix's WebSocket implementation from the outside, which is the
only way to find the rules a hand-rolled codec got wrong rather than the
ones it thought to test.

It runs as a **blocking CI job**, not as part of `uv run pytest`: the suite
ships as a Docker image and takes a few minutes.

## Running it locally

Needs Docker.

```console
$ uv run python tests/autobahn/echo_server.py 9001 &
$ cd tests/autobahn
$ docker run --rm \
      -v "$PWD/fuzzingclient.json:/config/fuzzingclient.json" \
      -v "$PWD/reports:/reports" \
      --network host \
      crossbario/autobahn-testsuite \
      wstest --mode fuzzingclient --spec /config/fuzzingclient.json
$ uv run python check_report.py reports/servers/index.json
```

`reports/servers/index.html` is the human-readable version.

`--network host` is what lets the container reach `ws://127.0.0.1:9001`. It
works on Linux; on Docker Desktop, point the `url` in `fuzzingclient.json` at
`ws://host.docker.internal:9001` and drop the flag instead.

## What is excluded, and why

`fuzzingclient.json` excludes sections **12** and **13**, which test
`permessage-deflate`. Sonix negotiates no extensions at all and rejects any
frame with an RSV bit set, so those cases are not a partial implementation to
be measured -- they are a documented non-goal.

`check_report.py` accepts `NON-STRICT` alongside `OK`.

## Current result

**301 cases, 0 failing, 11 non-strict** (run 31429614696). The 11 fall into
exactly two classes, both understood:

**Fail-fast UTF-8 — cases 6.4.1 through 6.4.4.** UTF-8 is validated on the
reassembled message, not mid-fragment. The suite considers detecting an
invalid sequence as early as possible to be strict; validating per fragment
instead would reject legitimate traffic where a multi-byte character straddles
a fragment boundary, so the check happens once the message is whole.

**A valid message lost to the close that follows it — cases 3.2, 3.3, 4.1.3,
4.1.4, 4.2.3, 4.2.4 and 5.15.** Each sends a good message *and then* a
protocol violation, and expects the echo of the good message before the
connection fails. Sonix's codec does surface the earlier message
(`WebSocketProtocolError.partial_events` exists precisely for this), and the
bridge does queue it for the application — but it then writes the close frame
and closes the transport in the same event-loop callback, before the
application task has run. So the echo never reaches the wire.

That is a real, if minor, defect rather than a stylistic difference: a message
the client legitimately sent is dropped. It is RFC-permitted, which is why the
suite grades it non-strict rather than failed. Fixing it properly means
deferring both the close frame and the transport close until the application
task has drained — a change to the connection teardown path, and therefore a
hardening-pass item rather than something to bolt on here.

`INFORMATIONAL` is accepted for section 9, where the suite measures
throughput rather than asserting behaviour.

`UNIMPLEMENTED` is treated as a failure. A case the server never answered is
not a case that passed.

## Server settings that are not defaults

`echo_server.py` raises `websocket_max_message_size` to 20 MiB and disables
keepalive pings. Section 9 sends messages up to 16 MiB and the right answer
to those is an echo rather than a 1009; section 2 asserts on exact ping/pong
exchanges, and an unsolicited server ping arriving mid-case would be a real
disagreement about what the case measured.
