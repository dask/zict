import inspect
import threading
from collections.abc import MutableMapping
from concurrent.futures import ThreadPoolExecutor

import pytest

from zict import LRU


def test_removal_signatures():
    for method in ("pop", "popitem"):
        assert inspect.signature(getattr(LRU, method)) == inspect.signature(
            getattr(MutableMapping, method)
        )


def assert_consistent(lru):
    assert set(lru.d) == set(lru.weights) == set(lru.order)
    assert lru.total_weight == sum(lru.weights.values())
    assert set(lru.heavy) == {k for k, w in lru.weights.items() if w > lru.n}


@pytest.mark.parametrize("method", ["pop", "popitem"])
@pytest.mark.parametrize("mutation", ["replace", "pop"])
def test_removal_read_delete_atomic(monkeypatch, method, mutation):
    """Attempt a competing operation in the gap after the value is read."""
    lru = LRU(100, {"x": 1}, weight=lambda k, v: v)
    original_getitem = LRU.__getitem__
    owner = threading.get_ident()
    attempted = []

    def compete():
        acquired = lru.lock.acquire(blocking=False)
        try:
            if acquired:
                if mutation == "replace":
                    lru["x"] = 2
                else:
                    assert lru.pop("x") == 1
            return acquired
        finally:
            if acquired:
                lru.lock.release()

    with ThreadPoolExecutor(1) as executor:

        def getitem(self, key):
            value = original_getitem(self, key)
            if threading.get_ident() == owner and not attempted:
                attempted.append(executor.submit(compete).result(timeout=5))
            return value

        monkeypatch.setattr(LRU, "__getitem__", getitem)
        result = lru.pop("x") if method == "pop" else lru.popitem()

    assert result == (1 if method == "pop" else ("x", 1))
    assert attempted == [False]
    # Once the atomic removal finishes, the competing operation can proceed.
    if mutation == "replace":
        lru["x"] = 2
        assert lru["x"] == 2
    else:
        with pytest.raises(KeyError):
            lru.pop("x")
    assert_consistent(lru)


def test_popitem_key_selection_atomic(monkeypatch):
    lru = LRU(100, {"x": 1, "y": 2})
    original_iter = LRU.__iter__
    attempted = []

    def compete():
        acquired = lru.lock.acquire(blocking=False)
        try:
            if acquired:
                del lru["x"]
            return acquired
        finally:
            if acquired:
                lru.lock.release()

    with ThreadPoolExecutor(1) as executor:

        def iterate(self):
            keys = list(original_iter(self))
            attempted.append(executor.submit(compete).result(timeout=5))
            return iter(keys)

        monkeypatch.setattr(LRU, "__iter__", iterate)
        assert lru.popitem() == ("x", 1)

    assert attempted == [False]
    assert lru.d == {"y": 2}
    assert_consistent(lru)


def test_removal_mapping_semantics(is_locked):
    lru = LRU(2, {}, weight=lambda k, v: v)
    default = object()
    assert lru.pop("missing", default) is default
    assert lru.pop("missing", None) is None
    assert lru.pop("missing", default=False) is False
    assert lru.pop(key="missing", default=default) is default
    with pytest.raises(TypeError):
        lru.pop("missing", 1, 2)
    with pytest.raises(TypeError):
        lru.pop("missing", unexpected=1)
    with pytest.raises(KeyError, match="missing"):
        lru.pop("missing")
    with pytest.raises(KeyError):
        lru.popitem()
    assert not is_locked(lru)
    lru.set_noevict("heavy", 3)
    lru.set_noevict("last", 1)
    lru["heavy"]  # Recency does not change backing-map iteration order.
    assert lru.popitem() == ("heavy", 3)
    assert_consistent(lru)
    assert lru.pop("last") == 1
    assert_consistent(lru)


@pytest.mark.parametrize("method", ["pop", "popitem"])
def test_removal_cancels_pending_eviction(method, check_thread_leaks, is_locked):
    started = threading.Event()
    finish = threading.Event()
    cancelled = []

    def evict(key, value):
        started.set()
        assert finish.wait(timeout=5)

    def cancel(key, value):
        cancelled.append((key, value))

    lru = LRU(100, {"x": 1}, on_evict=evict, on_cancel_evict=cancel)
    with ThreadPoolExecutor(1) as executor:
        future = executor.submit(lru.evict)
        try:
            assert started.wait(timeout=5)
            assert not is_locked(lru)
            result = lru.pop("x") if method == "pop" else lru.popitem()
            assert result == (1 if method == "pop" else ("x", 1))
            assert not is_locked(lru)
            # An unrelated key still progresses while the callback is blocked.
            lru["y"] = 2
            assert lru["y"] == 2
        finally:
            finish.set()
        assert future.result(timeout=5) == (None, None, 0)
    assert cancelled == [("x", 1)]
    assert not lru._cancel_evict
    assert_consistent(lru)


@pytest.mark.parametrize("method", ["pop", "popitem"])
@pytest.mark.parametrize("failure", ["read", "delete"])
def test_removal_failure_releases_lock(method, failure, is_locked):
    class FailingDict(dict):
        def __getitem__(self, key):
            if failure == "read":
                raise OSError("read failed")
            return super().__getitem__(key)

        def __delitem__(self, key):
            if failure == "delete":
                raise OSError("delete failed")
            super().__delitem__(key)

    lru = LRU(100, FailingDict(x=1))
    with pytest.raises(OSError, match=f"{failure} failed"):
        if method == "pop":
            lru.pop("x", None)
        else:
            lru.popitem()
    assert not is_locked(lru)
    assert lru.d == {"x": 1}
    assert_consistent(lru)
