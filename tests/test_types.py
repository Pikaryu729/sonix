from sonix import types


def test_aliases_are_exported():
    assert hasattr(types, "Scope")
    assert hasattr(types, "Message")
    assert hasattr(types, "Receive")
    assert hasattr(types, "Send")
    assert hasattr(types, "ASGIApp")
