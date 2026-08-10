"""Shared plumbing for the cross-implementation conformance tests.

Imported as ``conformance.support`` -- ``tests/`` is on sys.path via the
pythonpath setting in pyproject.toml, and this package has an __init__.py.
Deliberately not conftest.py: pytest imports conftest under its own machinery,
and a test module importing it by name is not a supported path.
"""

from __future__ import annotations

import asyncio
import contextlib
import socket
import sys

from sonix.server.server import Config, Server


def free_port() -> int:
    """Ask the kernel for an unused port, then release it.

    Inherently racy, but it is the only option for a subprocess that takes a
    port on its command line rather than binding one itself.
    """
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@contextlib.asynccontextmanager
async def serve_on_sonix(app):
    """Run any ASGI app on Sonix's own server and yield its base URL."""
    server = Server(Config(app, host="127.0.0.1", port=0))
    await server.startup()
    serving = asyncio.ensure_future(server.serve_forever())
    try:
        host, port = server.sockets[0].getsockname()[:2]
        yield f"http://{host}:{port}"
    finally:
        serving.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await serving
        await server.shutdown()


async def wait_until_serving(port: int, process) -> None:
    """Poll until ``port`` accepts a connection, or the process dies.

    Deliberately takes no timeout parameter -- the caller bounds this with
    asyncio.timeout(), which is the composable way to do it and avoids two
    competing deadlines. Exiting early when the process dies matters more than
    the deadline anyway: it turns "startup timed out" into the subprocess's
    actual error output.
    """
    while True:
        if process.poll() is not None:
            output = process.stdout.read() if process.stdout else ""
            raise RuntimeError(
                f"server exited early with code {process.returncode}\n{output}"
            )
        try:
            _reader, writer = await asyncio.open_connection("127.0.0.1", port)
        except OSError:
            await asyncio.sleep(0.05)
            continue
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()
        return


UVICORN_CMD = [sys.executable, "-m", "uvicorn"]
