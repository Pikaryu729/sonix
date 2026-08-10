"""A Sonix echo server for the Autobahn|Testsuite fuzzing client.

Run as a module, not through the `sonix` CLI, because the settings below are
not defaults and matter for the suite:

* ``websocket_max_message_size`` is raised well past the default. Section 9
  of the suite ("limits/performance") sends messages up to 16 MiB, and the
  correct answer to those is an echo, not a 1009.
* ``ws_ping_interval=None`` disables our keepalive. Section 2 asserts on
  exact ping/pong exchanges, and an unsolicited server ping arriving
  mid-case would be a real disagreement about what the test measured.

The handler echoes text as text and binary as binary. Getting that backwards
turns most of section 1 into failures, since a text frame carrying invalid
UTF-8 must be rejected and a binary frame carrying the same bytes must not.
"""

from __future__ import annotations

import sys

from sonix import Sonix, run
from sonix.app.websockets import WebSocket, WebSocketDisconnect

# 20 MiB: section 9's largest case is 16 MiB, and the limit is checked against
# a frame's declared length before any payload is buffered.
MAX_MESSAGE_SIZE = 20 * 1024 * 1024

app = Sonix()


@app.websocket("/")
async def echo(websocket: WebSocket) -> None:
    await websocket.accept()
    try:
        while True:
            message = await websocket.receive()
            if message["type"] == "websocket.disconnect":
                return
            if message.get("text") is not None:
                await websocket.send_text(message["text"])
            else:
                await websocket.send_bytes(message["bytes"])
    except WebSocketDisconnect:
        return


def main() -> None:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9001
    run(
        app,
        # Bound broadly on purpose: the suite runs in a container and has
        # to reach this from outside the loopback interface.
        host="0.0.0.0",
        port=port,
        websocket_max_message_size=MAX_MESSAGE_SIZE,
        ws_ping_interval=None,
        ws_ping_timeout=None,
    )


if __name__ == "__main__":
    main()
