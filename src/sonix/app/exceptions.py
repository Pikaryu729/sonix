"""Exceptions that the middleware layer converts into HTTP responses.

These live in the app layer and carry no knowledge of how a response
reaches a socket -- raising HTTPException is how any layer above routing
says "stop, and answer with this status" without constructing a Response
itself.

Note what is deliberately *not* here: app/requests.py raises ClientDisconnect
rather than an HTTPException, so that module stays ignorant of HTTP status
codes entirely. ExceptionMiddleware is what maps it.
"""

from __future__ import annotations

import http


def status_phrase(status_code: int) -> str:
    """The registered reason phrase for a status, or "Error" if unknown."""
    try:
        return http.HTTPStatus(status_code).phrase
    except ValueError:
        return "Error"


class HTTPException(Exception):
    """An HTTP error response, raised rather than returned.

    ``detail`` defaults to the status's reason phrase, so
    ``HTTPException(404)`` is already a complete, sensible response.
    """

    def __init__(
        self,
        status_code: int,
        detail: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self.detail = detail if detail is not None else status_phrase(status_code)
        self.headers = headers
        super().__init__(f"{status_code}: {self.detail}")

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(status_code={self.status_code!r}, "
            f"detail={self.detail!r})"
        )


class ValidationError(HTTPException):
    """A 422 carrying one entry per parameter that failed.

    A plain HTTPException would flatten this to a single string, but a client
    fixing a request wants every problem at once rather than discovering them
    one round trip at a time. Each entry is
    ``{"loc": [source, name], "msg": ..., "type": ...}`` -- the same shape
    FastAPI uses, which is worth matching for no reason other than that
    consumers already know how to read it.

    Subclassing HTTPException is what makes this work with the existing
    machinery: ExceptionMiddleware walks the MRO, so a registered
    ValidationError handler wins, and `debug=True` (which re-raises only
    non-HTTPException errors) correctly still converts it -- a 422 is a
    client mistake, not a bug to surface.
    """

    def __init__(self, errors: list[dict]) -> None:
        self.errors = list(errors)
        # str detail is still populated so that anything reading a plain
        # HTTPException -- a custom handler, a log line -- gets something
        # useful rather than an empty string.
        summary = "; ".join(
            f"{'.'.join(str(part) for part in error['loc'])}: {error['msg']}"
            for error in self.errors
        )
        super().__init__(422, summary or status_phrase(422))
