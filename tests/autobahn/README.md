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

`check_report.py` accepts `NON-STRICT` alongside `OK`. There is one class of
those and it is deliberate: **UTF-8 is validated on the reassembled message,
not fail-fast mid-fragment** (cases 6.4.x). The suite considers detecting an
invalid sequence as early as possible to be strict behaviour; validating per
fragment instead would reject legitimate traffic where a multi-byte character
straddles a fragment boundary, so the check happens once the message is
whole. Fail-fast incremental validation is a hardening item, not a bug.

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
