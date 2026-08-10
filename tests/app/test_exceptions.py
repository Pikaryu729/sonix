"""HTTPException: the raise-don't-return path to an error response."""

from __future__ import annotations

from sonix.app.exceptions import HTTPException, status_phrase


class TestStatusPhrase:
    def test_known_status(self):
        assert status_phrase(404) == "Not Found"
        assert status_phrase(500) == "Internal Server Error"

    def test_unknown_status_does_not_raise(self):
        # A caller is allowed to invent a status; it must not crash the
        # error path, which is the one path that has to keep working.
        assert status_phrase(799) == "Error"


class TestHTTPException:
    def test_detail_defaults_to_the_reason_phrase(self):
        assert HTTPException(404).detail == "Not Found"

    def test_explicit_detail_wins(self):
        assert HTTPException(404, "no such room").detail == "no such room"

    def test_empty_detail_is_preserved_not_replaced(self):
        # "" is a deliberate choice of empty body, distinct from "unset".
        assert HTTPException(404, "").detail == ""

    def test_headers_default_to_none(self):
        assert HTTPException(404).headers is None

    def test_headers_round_trip(self):
        exc = HTTPException(405, headers={"allow": "GET"})
        assert exc.headers == {"allow": "GET"}

    def test_is_an_exception_and_carries_a_readable_message(self):
        exc = HTTPException(418, "nope")
        assert isinstance(exc, Exception)
        assert "418" in str(exc)
        assert "nope" in str(exc)

    def test_repr_names_status_and_detail(self):
        assert "404" in repr(HTTPException(404))
        assert "Not Found" in repr(HTTPException(404))
