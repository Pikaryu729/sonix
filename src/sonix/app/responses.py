"""Response / JSONResponse / PlainTextResponse / HTMLResponse.

Response is itself a valid ASGI app: __call__(scope, receive, send) emits
http.response.start then http.response.body. This is what lets dispatch
(a later milestone) treat "handler returned a Response" and "handler
returned a dict that dispatch wrapped into one" identically.
"""

from __future__ import annotations

import dataclasses
import json
from typing import Any

from sonix.types import Receive, Scope, Send

HeaderTypes = dict[str, str] | list[tuple[str, str]] | None


def _json_default(obj: Any) -> Any:
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return dataclasses.asdict(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


class Response:
    media_type: str | None = None

    def __init__(
        self,
        content: Any = None,
        status_code: int = 200,
        headers: HeaderTypes = None,
        media_type: str | None = None,
    ) -> None:
        self.status_code = status_code
        self.media_type = media_type if media_type is not None else self.media_type
        self.body = self.render(content)
        self.headers = self._build_headers(headers)

    def render(self, content: Any) -> bytes:
        if content is None:
            return b""
        if isinstance(content, bytes):
            return content
        if isinstance(content, str):
            return content.encode("utf-8")
        raise TypeError(
            f"{type(self).__name__} cannot render content of type "
            f"{type(content).__name__}; pass str/bytes, or use a subclass "
            "like JSONResponse"
        )

    def _build_headers(self, headers: HeaderTypes) -> list[tuple[bytes, bytes]]:
        # A list, not a dict keyed by name -- a caller must be able to set
        # the same header name more than once (e.g. multiple Set-Cookie
        # values), which a dict would silently collapse to the last one.
        encoded: list[tuple[bytes, bytes]] = []
        if headers:
            items = headers.items() if isinstance(headers, dict) else headers
            for name, value in items:
                encoded.append(
                    (name.lower().encode("latin-1"), value.encode("latin-1"))
                )

        # Caller-supplied content-type wins over the class/instance
        # media_type default -- explicit beats implicit.
        has_content_type = any(name == b"content-type" for name, _ in encoded)
        if not has_content_type and self.media_type is not None:
            content_type = self.media_type
            if content_type.startswith("text/") or content_type == "application/json":
                content_type += "; charset=utf-8"
            encoded.append((b"content-type", content_type.encode("latin-1")))

        # content-length is always derived from the real rendered body and
        # always overrides whatever the caller passed -- a caller-supplied
        # value could never be trusted to match self.body, and
        # protocol.py's keep-alive logic (_decide_close) depends on this
        # header being present and correct. No Transfer-Encoding is ever
        # emitted here -- no chunked support exists anywhere yet.
        encoded = [
            (name, value) for name, value in encoded if name != b"content-length"
        ]
        encoded.append((b"content-length", str(len(self.body)).encode("latin-1")))

        return encoded

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": self.status_code,
                "headers": self.headers,
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": self.body,
                "more_body": False,
            }
        )


class JSONResponse(Response):
    media_type = "application/json"

    def render(self, content: Any) -> bytes:
        return json.dumps(
            content,
            default=_json_default,
            separators=(",", ":"),
        ).encode("utf-8")


class PlainTextResponse(Response):
    media_type = "text/plain"


class HTMLResponse(Response):
    media_type = "text/html"
