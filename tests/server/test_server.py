"""Server lifecycle: binding, real round trips, and graceful shutdown."""

from __future__ import annotations

import asyncio
import os
import signal

import pytest

from sonix import DEFAULT_TARGET, _parse_args, import_from_string, main
from sonix.server.protocol import HTTPProtocol
from sonix.server.server import DEFAULT_HOST, DEFAULT_PORT, Config, Server
from sonix.types import Receive, Scope, Send


async def hello_app(scope: Scope, receive: Receive, send: Send) -> None:
    body = b"hello"
    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-length", str(len(body)).encode())],
        }
    )
    await send({"type": "http.response.body", "body": body, "more_body": False})


def make_slow_app(release: asyncio.Event, started: asyncio.Event):
    async def slow_app(scope: Scope, receive: Receive, send: Send) -> None:
        started.set()
        await release.wait()
        body = b"done"
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-length", str(len(body)).encode())],
            }
        )
        await send({"type": "http.response.body", "body": body, "more_body": False})

    return slow_app


async def running(config: Config):
    server = Server(config)
    await server.startup()
    host, port = server.sockets[0].getsockname()[:2]
    return server, host, port


async def request(host, port, raw=b"GET / HTTP/1.1\r\nHost: e.com\r\n\r\n"):
    reader, writer = await asyncio.open_connection(host, port)
    writer.write(raw)
    await writer.drain()
    data = await reader.read(4096)
    writer.close()
    await writer.wait_closed()
    return data


class TestConfig:
    def test_protocol_factory_forwards_every_tuning_parameter(self):
        # Before Config existed, HTTPProtocol was fully parameterized but
        # __init__.py forwarded none of it, so the defaults were the only
        # reachable values. This is the regression guard for that.
        config = Config(
            hello_app,
            max_header_size=1234,
            max_headers=7,
            max_body_size=4321,
            head_timeout=2.5,
            body_pause_watermark=3,
            body_resume_watermark=1,
        )
        protocol = config.protocol_factory(Server(config).state)()

        assert isinstance(protocol, HTTPProtocol)
        assert protocol._max_header_size == 1234
        assert protocol._max_headers == 7
        assert protocol._max_body_size == 4321
        assert protocol._head_timeout == 2.5
        assert protocol._body_pause_watermark == 3
        assert protocol._body_resume_watermark == 1

    def test_defaults_match_protocol_defaults(self):
        protocol = Config(hello_app).protocol_factory(Server(Config(hello_app)).state)()
        bare = HTTPProtocol(hello_app)
        assert protocol._max_header_size == bare._max_header_size
        assert protocol._head_timeout == bare._head_timeout


class TestServing:
    async def test_real_round_trip_on_an_ephemeral_port(self):
        server, host, port = await running(Config(hello_app, host="127.0.0.1", port=0))
        try:
            data = await request(host, port)
            assert data.startswith(b"HTTP/1.1 200 OK")
            assert data.endswith(b"hello")
        finally:
            await server.shutdown()

    async def test_serve_forever_before_startup_raises(self):
        server = Server(Config(hello_app))
        with pytest.raises(RuntimeError, match="startup"):
            await server.serve_forever()

    async def test_sockets_is_empty_before_startup(self):
        assert Server(Config(hello_app)).sockets == []

    async def test_connections_are_registered_and_deregistered(self):
        server, host, port = await running(Config(hello_app, port=0))
        try:
            reader, writer = await asyncio.open_connection(host, port)
            writer.write(b"GET / HTTP/1.1\r\nHost: e.com\r\n\r\n")
            await writer.drain()
            await reader.read(4096)
            assert len(server.state.connections) == 1

            writer.close()
            await writer.wait_closed()
            await asyncio.sleep(0.05)
            assert server.state.connections == set()
        finally:
            await server.shutdown()


class TestGracefulShutdown:
    async def test_inflight_request_is_allowed_to_finish(self):
        release, started = asyncio.Event(), asyncio.Event()
        config = Config(make_slow_app(release, started), port=0, shutdown_timeout=5.0)
        server, host, port = await running(config)

        reader, writer = await asyncio.open_connection(host, port)
        writer.write(b"GET / HTTP/1.1\r\nHost: e.com\r\n\r\n")
        await writer.drain()
        await asyncio.wait_for(started.wait(), timeout=1)

        shutdown = asyncio.ensure_future(server.shutdown())
        await asyncio.sleep(0.05)
        assert not shutdown.done(), "shutdown must wait for the in-flight request"

        release.set()
        await asyncio.wait_for(shutdown, timeout=2)

        data = await reader.read(4096)
        assert b"done" in data
        writer.close()
        await writer.wait_closed()

    async def test_inflight_response_advertises_connection_close(self):
        # The client must not be told keep-alive on a socket about to be
        # dropped, or it may pipeline a request into the void.
        release, started = asyncio.Event(), asyncio.Event()
        config = Config(make_slow_app(release, started), port=0, shutdown_timeout=5.0)
        server, host, port = await running(config)

        reader, writer = await asyncio.open_connection(host, port)
        writer.write(b"GET / HTTP/1.1\r\nHost: e.com\r\n\r\n")
        await writer.drain()
        await asyncio.wait_for(started.wait(), timeout=1)

        shutdown = asyncio.ensure_future(server.shutdown())
        await asyncio.sleep(0.05)
        release.set()
        await asyncio.wait_for(shutdown, timeout=2)

        data = await reader.read(4096)
        assert b"connection: close" in data.lower()
        writer.close()
        await writer.wait_closed()

    async def test_idle_keep_alive_connection_is_closed_immediately(self):
        server, host, port = await running(Config(hello_app, port=0))
        reader, writer = await asyncio.open_connection(host, port)
        writer.write(b"GET / HTTP/1.1\r\nHost: e.com\r\n\r\n")
        await writer.drain()
        await reader.read(4096)

        # The connection is idle, so shutdown should not wait out its
        # timeout -- it should close it and return promptly.
        await asyncio.wait_for(server.shutdown(), timeout=1)
        assert server.state.connections == set()
        writer.close()
        await writer.wait_closed()

    async def test_hung_handler_does_not_block_shutdown_past_the_deadline(self):
        never = asyncio.Event()
        started = asyncio.Event()
        config = Config(make_slow_app(never, started), port=0, shutdown_timeout=0.05)
        server, host, port = await running(config)

        _reader, writer = await asyncio.open_connection(host, port)
        writer.write(b"GET / HTTP/1.1\r\nHost: e.com\r\n\r\n")
        await writer.drain()
        await asyncio.wait_for(started.wait(), timeout=1)

        # Bounded, not unbounded: a hung handler must not keep the process up.
        await asyncio.wait_for(server.shutdown(), timeout=2)
        writer.close()
        await writer.wait_closed()

    async def test_shutdown_stops_accepting_new_connections(self):
        server, host, port = await running(Config(hello_app, port=0))
        await server.shutdown()
        with pytest.raises((ConnectionRefusedError, OSError)):
            await asyncio.wait_for(asyncio.open_connection(host, port), timeout=1)

    async def test_shutdown_is_idempotent(self):
        server, _host, _port = await running(Config(hello_app, port=0))
        await server.shutdown()
        await server.shutdown()


class TestRunLoop:
    async def test_run_serves_then_shuts_down_on_a_signal(self):
        config = Config(hello_app, port=0, shutdown_timeout=1.0)
        server = Server(config)
        running_task = asyncio.ensure_future(server.run())

        # run() owns startup(), so wait for the socket to appear.
        for _ in range(200):
            if server.sockets:
                break
            await asyncio.sleep(0.01)
        host, port = server.sockets[0].getsockname()[:2]

        assert (await request(host, port)).startswith(b"HTTP/1.1 200 OK")

        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.wait_for(running_task, timeout=5)

        # The handler must be removed again, or a later SIGTERM in the same
        # process would still be routed at this server's cancelled task.
        assert server.state.connections == set()

    async def test_run_reraises_startup_failure(self):
        # A TEST-NET-3 address (RFC 5737) is not assigned to this host, so the
        # bind fails. Deliberately not "a privileged port": that test passes
        # only for non-root users, and silently serves forever under root.
        server = Server(Config(hello_app, host="203.0.113.1", port=0))
        # No match=: the errno text for an unassignable address differs across
        # platforms and libc versions, and asserting on it would make this
        # test brittle for no gain. That the bind fails is the whole claim.
        with pytest.raises(OSError):  # noqa: PT011
            await asyncio.wait_for(server.run(), timeout=5)


class TestCLI:
    def test_default_target_is_the_builtin_demo(self):
        args = _parse_args([])
        assert args.target == DEFAULT_TARGET
        assert args.host == DEFAULT_HOST
        assert args.port == DEFAULT_PORT

    def test_explicit_host_and_port_parsed(self):
        args = _parse_args(["mod:app", "--host", "0.0.0.0", "--port", "9999"])
        assert (args.target, args.host, args.port) == ("mod:app", "0.0.0.0", 9999)

    def test_main_reports_a_bad_target_and_exits_nonzero(self, capsys):
        assert main(["not-a-valid-target"]) == 2
        assert "sonix:" in capsys.readouterr().err

    def test_main_does_not_start_a_server_for_a_bad_target(self, capsys):
        assert main(["sonix._demo:missing"]) == 2
        assert "no attribute" in capsys.readouterr().err


class TestImportFromString:
    def test_resolves_a_real_target(self):
        app = import_from_string("sonix._demo:app")
        assert callable(app)

    @pytest.mark.parametrize(
        "target",
        ["no_colon", ":app", "module:", ""],
    )
    def test_malformed_target_rejected(self, target):
        with pytest.raises(ValueError, match="invalid target"):
            import_from_string(target)

    def test_missing_module_reports_the_module(self):
        with pytest.raises(ValueError, match="could not import module"):
            import_from_string("sonix.definitely_not_here:app")

    def test_missing_attribute_reports_the_attribute(self):
        with pytest.raises(ValueError, match="no attribute"):
            import_from_string("sonix._demo:not_an_app")

    def test_non_callable_target_rejected(self):
        with pytest.raises(ValueError, match="not callable"):
            import_from_string("sonix:DEFAULT_TARGET")
