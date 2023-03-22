from __future__ import annotations

from collections.abc import (
    Callable,
    ItemsView,
    Iterator,
    KeysView,
    MutableMapping,
    ValuesView,
)

from zict.common import KT, VT, NoDefault, ZictBase, close, flush, nodefault
from zict.utils import Accumulator, InsertionSortedSet


class LRU(ZictBase[KT, VT]):
    """Evict Least Recently Used Elements.

    Parameters
    ----------
    n: int or float
        Number of elements to keep, or total weight if ``weight`` is used.
    d: MutableMapping
        Dict-like in which to hold elements. There are no expectations on its internal
        ordering. Iteration on the LRU follows the order of the underlying mapping.
    on_evict: list of callables
        Function:: k, v -> action to call on key value pairs prior to eviction
        If an exception occurs during an on_evict callback (e.g a callback tried
        storing to disk and raised a disk full error) the key will remain in the LRU.
    weight: callable
        Function:: k, v -> number to determine the size of keeping the item in
        the mapping.  Defaults to ``(k, v) -> 1``

    Notes
    -----
    Most methods are thread-safe if the same methods on ``d`` are thread-safe.
    ``__setitem__``, ``__delitem__``, :meth:`evict`, and
    :meth:`evict_until_below_capacity` also require all callables in ``on_evict`` to be
    thread-safe and should not be called from different threads for the same
    key. It's OK to set/delete different keys from different threads, it's OK to set a
    key in a thread and read it from many other threads, but it's not OK to set/delete
    the same key from different threads at the same time.

    Examples
    --------
    >>> lru = LRU(2, {}, on_evict=lambda k, v: print("Lost", k, v))
    >>> lru['x'] = 1
    >>> lru['y'] = 2
    >>> lru['z'] = 3
    Lost x 1
    """

    d: MutableMapping[KT, VT]
    order: InsertionSortedSet[KT]
    heavy: InsertionSortedSet[KT]
    on_evict: list[Callable[[KT, VT], None]]
    weight: Callable[[KT, VT], float]
    n: float
    weights: dict[KT, float]
    closed: bool
    total_weight: Accumulator

    def __init__(
        self,
        n: float,
        d: MutableMapping[KT, VT],
        on_evict: Callable[[KT, VT], None]
        | list[Callable[[KT, VT], None]]
        | None = None,
        weight: Callable[[KT, VT], float] = lambda k, v: 1,
    ):
        self.d = d
        self.n = n
        if callable(on_evict):
            on_evict = [on_evict]
        self.on_evict = on_evict or []
        self.weight = weight
        self.weights = {k: weight(k, v) for k, v in d.items()}
        self.total_weight = Accumulator(sum(self.weights.values()))
        self.order = InsertionSortedSet(d)
        self.heavy = InsertionSortedSet(k for k, v in self.weights.items() if v >= n)
        self.closed = False

    def __getitem__(self, key: KT) -> VT:
        result = self.d[key]
        # Don't use .remove() to prevent race condition which can happen during
        # multithreaded access
        self.order.discard(key)
        self.order.add(key)
        return result

    def __setitem__(self, key: KT, value: VT) -> None:
        self.set_noevict(key, value)
        try:
            self.evict_until_below_capacity()
        except Exception:
            if self.weights[key] > self.n and key not in self.heavy:
                # weight(value) > n and evicting the key we just inserted failed.
                # Evict the rest of the LRU instead.
                try:
                    while len(self.d) > 1:
                        self.evict()
                except Exception:
                    pass
            raise

    def set_noevict(self, key: KT, value: VT) -> None:
        """Variant of ``__setitem__`` that does not evict if the total weight exceeds n.
        Unlike ``__setitem__``, this method does not depend on the ``on_evict``
        functions to be thread-safe for its own thread-safety. It also is not prone to
        re-raising exceptions from the ``on_evict`` callbacks.
        """
        try:
            del self[key]
        except KeyError:
            pass

        weight = self.weight(key, value)
        self.d[key] = value
        self.order.add(key)
        if weight > self.n:
            self.heavy.add(key)  # Mark this key to be evicted first
        self.weights[key] = weight
        self.total_weight += weight

    def evict_until_below_capacity(self) -> None:
        """Evict key/value pairs until the total weight falls below n"""
        while self.total_weight > self.n and not self.closed:
            self.evict()

    def evict(self, key: KT | NoDefault = nodefault) -> tuple[KT, VT, float]:
        """Evict least recently used key, or least recently inserted key with individual
        weight > n, if any. You may also evict a specific key.

        This is typically called from internal use, but can be externally
        triggered as well.

        Returns
        -------
        Tuple of (key, value, weight)
        """
        # For the purpose of multithreaded access, it's important that the value remains
        # in self.d until all callbacks are successful.
        # When this is used inside a Buffer, there must never be a moment when the key
        # is neither in fast nor in slow.
        if key is nodefault:
            while True:
                try:
                    key = next(iter(self.heavy or self.order))
                    value = self.d[key]
                    break
                except StopIteration:
                    raise KeyError("evict(): dictionary is empty")
                except (KeyError, RuntimeError):  # pragma: nocover
                    pass  # Race condition caused by multithreading
        else:
            value = self.d[key]

        # If we are evicting a heavy key we just inserted and one of the callbacks
        # fails, put it at the bottom of the LRU instead of the top. This way lighter
        # keys will have a chance to be evicted first and make space.
        self.heavy.discard(key)

        # This may raise; e.g. if a callback tries storing to a full disk
        for cb in self.on_evict:
            cb(key, value)

        self.d.pop(key, None)  # type: ignore[arg-type]
        self.order.discard(key)
        weight = self.weights.pop(key)
        self.total_weight -= weight

        return key, value, weight

    def __delitem__(self, key: KT) -> None:
        del self.d[key]
        self.order.discard(key)
        self.heavy.discard(key)
        self.total_weight -= self.weights.pop(key)

    def keys(self) -> KeysView[KT]:
        return self.d.keys()

    def values(self) -> ValuesView[VT]:
        return self.d.values()

    def items(self) -> ItemsView[KT, VT]:
        return self.d.items()

    def __len__(self) -> int:
        return len(self.d)

    def __iter__(self) -> Iterator[KT]:
        return iter(self.d)

    def __contains__(self, key: object) -> bool:
        return key in self.d

    def __str__(self) -> str:
        sub = str(self.d) if not isinstance(self.d, dict) else "dict"
        return f"<LRU: {self.total_weight}/{self.n} on {sub}>"

    __repr__ = __str__

    def flush(self) -> None:
        flush(self.d)

    def close(self) -> None:
        self.closed = True
        close(self.d)
