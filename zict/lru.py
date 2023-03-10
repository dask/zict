from __future__ import annotations

from collections.abc import (
    Callable,
    ItemsView,
    Iterator,
    KeysView,
    MutableMapping,
    ValuesView,
)

from zict.common import KT, VT, ZictBase, close, flush


class LRU(ZictBase[KT, VT]):
    """Evict Least Recently Used Elements.

    Parameters
    ----------
    n: int or float
        Number of elements to keep, or total weight if weight= is used
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
    All methods except ``__setitem__`` and :meth:`evict` are thread-safe if the same
    methods on ``d`` are thread-safe.

    Examples
    --------
    >>> lru = LRU(2, {}, on_evict=lambda k, v: print("Lost", k, v))
    >>> lru['x'] = 1
    >>> lru['y'] = 2
    >>> lru['z'] = 3
    Lost x 1
    """

    d: MutableMapping[KT, VT]
    order: dict[KT, None]  # This is used as an insertion-sorted set
    on_evict: list[Callable[[KT, VT], None]]
    weight: Callable[[KT, VT], float]
    n: float
    total_weight: float
    weights: dict[KT, float]

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
        self.order = dict.fromkeys(d)
        if callable(on_evict):
            on_evict = [on_evict]
        self.on_evict = on_evict or []
        self.weight = weight
        self.weights = {k: weight(k, v) for k, v in d.items()}
        self.total_weight = sum(self.weights.values())
        while self.total_weight > n:
            self.evict()

    def __getitem__(self, key: KT) -> VT:
        result = self.d[key]
        try:
            del self.order[key]
        except KeyError:
            # Race condition which can happen during multithreaded access
            pass  # pragma: nocover
        self.order[key] = None
        return result

    def __setitem__(self, key: KT, value: VT) -> None:
        try:
            del self[key]
        except KeyError:
            pass

        weight = self.weight(key, value)

        def set_() -> None:
            self.d[key] = value
            self.order[key] = None
            self.weights[key] = weight
            self.total_weight += weight
            # Evicting the last key/value pair is guaranteed to fail, so don't try.
            # This is because it is always the last one inserted by virtue of this
            # being an LRU, which in turn means we reached this point because
            # weight > self.n and a callback raised exception (e.g. disk full).
            while self.total_weight > self.n and len(self.d) > 1:
                self.evict()

        if weight <= self.n:
            set_()
        else:
            try:
                for cb in self.on_evict:
                    cb(key, value)
            except Exception:
                # e.g. if a callback tried storing to disk and raised a disk full error
                set_()
                raise

    def evict(self) -> tuple[KT, VT, float]:
        """Evict least recently used key

        This is typically called from internal use, but can be externally
        triggered as well.

        Returns
        -------
        Tuple of (key, value, weight)
        """
        try:
            key = next(iter(self.order))
        except StopIteration:
            raise KeyError("evict(): dictionary is empty")
        value = self.d.pop(key)

        try:
            for cb in self.on_evict:
                cb(key, value)
        except Exception:
            # e.g. if a callback tried storing to disk and raised a disk full error
            self.d[key] = value
            raise

        del self.order[key]
        weight = self.weights.pop(key)
        self.total_weight -= weight
        return key, value, weight

    def __delitem__(self, key: KT) -> None:
        del self.d[key]
        try:
            del self.order[key]
        except KeyError:
            # Race condition which can happen during multithreaded access
            pass  # pragma: nocover

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
        close(self.d)
