from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from zict import LRU
from zict.tests import utils_test


def test_simple():
    d = {}
    lru = LRU(2, d)

    lru["x"] = 1
    lru["y"] = 2

    assert lru["x"] == 1
    assert lru["y"] == 2
    assert d == {"x": 1, "y": 2}

    lru["z"] = 3
    assert len(d) == 2
    assert len(lru) == 2
    assert "z" in d
    assert "z" in lru
    assert "x" not in d
    assert "y" in d

    del lru["y"]
    assert "y" not in d
    assert "y" not in lru

    lru["a"] = 5
    assert set(lru.keys()) == {"z", "a"}


def test_str():
    d = {}
    lru = LRU(2, d)

    lru["x"] = 1
    lru["y"] = 2

    assert str(lru.total_weight) in str(lru)
    assert str(lru.total_weight) in repr(lru)
    assert str(lru.n) in str(lru)
    assert str(lru.n) in repr(lru)
    assert "dict" in str(lru)
    assert "dict" in repr(lru)


def test_mapping():
    """
    Test mapping interface for LRU().
    """
    d = {}
    # 100 is more than the max length when running check_mapping()
    lru = LRU(100, d)
    utils_test.check_mapping(lru)
    utils_test.check_closing(lru)


def test_overwrite():
    d = {}
    lru = LRU(2, d)

    lru["x"] = 1
    lru["y"] = 2
    lru["y"] = 3

    assert set(lru) == {"x", "y"}

    lru.update({"y": 4})

    assert set(lru) == {"x", "y"}


def test_callbacks():
    count = [0]

    def cb(k, v):
        count[0] += 1

    L = []
    d = {}
    lru = LRU(2, d, on_evict=[lambda k, v: L.append((k, v)), cb])

    lru["x"] = 1
    lru["y"] = 2
    lru["z"] = 3

    assert L == [("x", 1)]
    assert count[0] == len(L)


def test_cb_exception_keep_on_lru():
    class MyError(Exception):
        pass

    def cb(k, v):
        raise MyError()

    a = []
    b = []
    d = {}
    lru = LRU(
        2,
        d,
        on_evict=[
            lambda k, v: a.append((k, v)),
            cb,
            lambda k, v: b.append((k, v)),
        ],
    )

    lru["x"] = 1
    lru["y"] = 2

    with pytest.raises(MyError):
        lru["z"] = 3

    # exception was raised in a later callback
    assert a == [("x", 1)]
    # tried to evict and raised exception
    assert b == []
    assert lru.total_weight == 3
    assert lru.weights == {"x": 1, "y": 1, "z": 1}

    assert set(lru) == {"x", "y", "z"}

    assert lru.d == {"x": 1, "y": 2, "z": 3}
    assert list(lru.order) == ["x", "y", "z"]


def test_cb_exception_keep_on_lru_weights():
    class MyError(Exception):
        pass

    def cb(k, v):
        if v >= 3:
            raise MyError()

    a = []
    b = []
    d = {}
    lru = LRU(
        2,
        d,
        on_evict=[
            lambda k, v: a.append((k, v)),
            cb,
            lambda k, v: b.append((k, v)),
        ],
        weight=lambda k, v: v,
    )

    lru["x"] = 1

    with pytest.raises(MyError):
        # value is individually heavier than n
        lru["y"] = 3

    # exception was raised in a later callback
    assert a == [("y", 3), ("x", 1)]
    # tried to to evict x and succeeded
    assert b == [("x", 1)]
    assert lru.total_weight == 3
    assert set(lru) == {"y"}

    assert lru.d == {"y": 3}
    assert list(lru.order) == ["y"]

    with pytest.raises(MyError):
        # value is individually heavier than n
        lru["z"] = 4

    # exception was raised in a later callback
    assert a == [
        ("y", 3),
        ("x", 1),
        ("z", 4),
        ("y", 3),
    ]  # try to evict z and then y again
    # tried to evict and raised exception
    assert b == [("x", 1)]
    assert lru.total_weight == 7
    assert set(lru) == {"y", "z"}

    assert lru.d == {"y": 3, "z": 4}
    assert list(lru.order) == ["y", "z"]


def test_weight():
    d = {}
    weight = lambda k, v: v
    lru = LRU(10, d, weight=weight)

    lru["x"] = 5
    assert lru.total_weight == 5

    lru["y"] = 4
    assert lru.total_weight == 9

    lru["z"] = 3
    assert d == {"y": 4, "z": 3}
    assert lru.total_weight == 7

    del lru["z"]
    assert lru.total_weight == 4

    lru["a"] = 10000
    assert "a" not in lru
    assert d == {"y": 4}


def test_explicit_evict():
    d = {}
    lru = LRU(10, d)

    lru["x"] = 1
    lru["y"] = 2

    assert set(d) == {"x", "y"}

    k, v, w = lru.evict()
    assert set(d) == {"y"}
    assert k == "x"
    assert v == 1
    assert w == 1


def test_init_not_empty():
    lru1 = LRU(100, {}, weight=lambda k, v: v * 2)
    lru1[1] = 10
    lru1[2] = 20
    lru1[3] = 30
    lru2 = LRU(100, {1: 10, 2: 20, 3: 30}, weight=lambda k, v: v * 2)
    assert lru1.d == lru2.d == {2: 20, 3: 30}
    assert lru1.weights == lru2.weights == {2: 40, 3: 60}
    assert lru1.total_weight == lru2.total_weight == 100
    assert list(lru1.order) == list(lru2.order) == [2, 3]


def test_getitem_is_threasafe():
    """Note: even if you maliciously tamper with LRU.__getitem__ to make it
    thread-unsafe, this test fails only ~20% of the times on a 12-CPU desktop.
    """
    lru = LRU(100, {})
    lru["x"] = 1

    def f(_):
        barrier.wait()
        for _ in range(5_000_000):
            assert lru["x"] == 1

    barrier = Barrier(2)
    with ThreadPoolExecutor(2) as ex:
        for _ in ex.map(f, range(2)):
            pass
