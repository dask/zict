from collections import UserDict

from zict.common import ZictBase


def test_close_on_del():
    closed = False

    class D(ZictBase, UserDict):
        def close(self):
            nonlocal closed
            closed = True

    d = D()
    del d
    assert closed


def test_context():
    closed = False

    class D(ZictBase, UserDict):
        def close(self):
            nonlocal closed
            closed = True

    d = D()
    with d as d2:
        assert d2 is d
    assert closed


def test_update():
    items = []

    class D(ZictBase, UserDict):
        def _do_update(self, items_):
            nonlocal items
            items = items_

    d = D()
    d.update({"x": 1})
    assert list(items) == [("x", 1)]
    d.update(iter([("x", 2)]))
    assert list(items) == [("x", 2)]
    d.update({"x": 3}, y=4)
    assert list(items) == [("x", 3), ("y", 4)]
    d.update(x=5)
    assert list(items) == [("x", 5)]

    # Special kwargs can't overwrite positional-only parameters
    d.update(self=1, other=2)
    assert list(items) == [("self", 1), ("other", 2)]
