from __future__ import annotations

from collections.abc import Callable, Iterator, MutableMapping

from zict.common import KT, VT, ZictBase, close, discard, flush, locked
from zict.lru import LRU
from zict.utils import InsertionSortedSet


class Buffer(ZictBase[KT, VT]):
    """Buffer one dictionary on top of another

    This creates a MutableMapping by combining two MutableMappings, one that
    feeds into the other when it overflows, based on an LRU mechanism.  When
    the first evicts elements these get placed into the second. When an item
    is retrieved from the second it is placed back into the first.

    Parameters
    ----------
    fast: MutableMapping
    slow: MutableMapping
    n: float
        Number of elements to keep, or total weight if ``weight`` is used.
    weight: f(k, v) -> float, optional
        Weight of each key/value pair (default: 1)
    fast_to_slow_callbacks: list of callables
        These functions run every time data moves from the fast to the slow
        mapping. They take two arguments, a key and a value.
        If an exception occurs during a fast_to_slow_callbacks (e.g a callback tried
        storing to disk and raised a disk full error) the key will remain in the LRU.
    slow_to_fast_callbacks: list of callables
        These functions run every time data moves form the slow to the fast mapping.
    keep_slow: bool, optional
        If False (default), delete key/value pairs in slow when they are moved back to
        fast.
        If True, keep them in slow until deleted; this will avoid repeating the fast to
        slow transition when they are evicted again, but at the cost of duplication.

    Notes
    -----
    If you call methods of this class from multiple threads, access will be fast as long
    as all methods of ``fast``, plus ``slow.__contains__`` and ``slow.__delitem__``, are
    fast. ``slow.__getitem__``, ``slow.__setitem__`` and callbacks are not protected
    by locks.

    Examples
    --------
    >>> fast = {}
    >>> slow = Func(dumps, loads, File('storage/'))  # doctest: +SKIP
    >>> def weight(k, v):
    ...     return sys.getsizeof(v)
    >>> buff = Buffer(fast, slow, 1e8, weight=weight)  # doctest: +SKIP

    See Also
    --------
    LRU
    """

    fast: LRU[KT, VT]
    slow: MutableMapping[KT, VT]
    weight: Callable[[KT, VT], float]
    fast_to_slow_callbacks: list[Callable[[KT, VT], None]]
    slow_to_fast_callbacks: list[Callable[[KT, VT], None]]
    keep_slow: bool
    _cancel_restore: dict[KT, bool]
    _keys: InsertionSortedSet[KT]

    def __init__(
        self,
        fast: MutableMapping[KT, VT],
        slow: MutableMapping[KT, VT],
        n: float,
        weight: Callable[[KT, VT], float] = lambda k, v: 1,
        fast_to_slow_callbacks: Callable[[KT, VT], None]
        | list[Callable[[KT, VT], None]]
        | None = None,
        slow_to_fast_callbacks: Callable[[KT, VT], None]
        | list[Callable[[KT, VT], None]]
        | None = None,
        keep_slow: bool = False,
    ):
        super().__init__()
        self.fast = LRU(
            n,
            fast,
            weight=weight,
            on_evict=[self.fast_to_slow],
            on_cancel_evict=[self._cancel_evict],
        )
        self.slow = slow
        self.weight = weight
        if callable(fast_to_slow_callbacks):
            fast_to_slow_callbacks = [fast_to_slow_callbacks]
        if callable(slow_to_fast_callbacks):
            slow_to_fast_callbacks = [slow_to_fast_callbacks]
        self.fast_to_slow_callbacks = fast_to_slow_callbacks or []
        self.slow_to_fast_callbacks = slow_to_fast_callbacks or []
        self.keep_slow = keep_slow
        self._cancel_restore = {}
        self._keys = InsertionSortedSet((*self.fast, *self.slow))

    @property
    def n(self) -> float:
        """Maximum weight in the fast mapping before eviction happens.
        Can be updated; this won't trigger eviction by itself; you should call
        :meth:`evict_until_below_target` afterwards.

        See also
        --------
        offset
        evict_until_below_target
        LRU.n
        LRU.offset
        """
        return self.fast.n

    @n.setter
    def n(self, value: float) -> None:
        self.fast.n = value

    @property
    def offset(self) -> float:
        """Offset to add to the total weight in the fast buffer to determine when
        eviction happens. Note that increasing offset is not the same as decreasing n,
        as the latter also changes what keys qualify as "heavy" and should not be stored
        in fast.

        Always starts at zero and can be updated; this won't trigger eviction by itself;
        you should call :meth:`evict_until_below_target` afterwards.

        See also
        --------
        n
        evict_until_below_target
        LRU.n
        LRU.offset
        """
        return self.fast.offset

    @offset.setter
    def offset(self, value: float) -> None:
        self.fast.offset = value

    def fast_to_slow(self, key: KT, value: VT) -> None:
        if self.keep_slow and key in self.slow:
            return

        self.slow[key] = value
        try:
            for cb in self.fast_to_slow_callbacks:
                cb(key, value)
        # LRU catches exception, raises and makes sure keys are not lost and located in
        # fast.
        except Exception:
            del self.slow[key]
            raise

    def slow_to_fast(self, key: KT) -> VT:
        self._cancel_restore[key] = False
        try:
            with self.unlock():
                value = self.slow[key]
            if self._cancel_restore[key]:
                raise KeyError(key)
        finally:
            del self._cancel_restore[key]

        # Avoid useless movement for heavy values
        w = self.weight(key, value)
        if w <= self.n:
            # Multithreaded edge case:
            # - Thread 1 starts slow_to_fast(x) and puts it at the top of fast
            # - This causes the eviction of older key(s)
            # - While thread 1 is evicting older keys, thread 2 is loading fast with
            #   set_noevict()
            # - By the time the eviction of the older key(s) is done, there is
            #   enough weight in fast that thread 1 will spill x
            # - If the below code was just `self.fast[key] = value; del
            #   self.slow[key]` now the key would be in neither slow nor fast!
            self.fast.set_noevict(key, value)
            if not self.keep_slow:
                del self.slow[key]

        with self.unlock():
            self.fast.evict_until_below_target()
            for cb in self.slow_to_fast_callbacks:
                cb(key, value)

        return value

    @locked
    def __getitem__(self, key: KT) -> VT:
        if key not in self._keys:
            raise KeyError(key)
        try:
            return self.fast[key]
        except KeyError:
            return self.slow_to_fast(key)

    def __setitem__(self, key: KT, value: VT) -> None:
        self.set_noevict(key, value)
        try:
            self.fast.evict_until_below_target()
        except Exception:
            self.fast._setitem_exception(key)
            raise

    @locked
    def set_noevict(self, key: KT, value: VT) -> None:
        """Variant of ``__setitem__`` that does not move keys from fast to slow if the
        total weight exceeds n
        """
        discard(self.slow, key)
        if key in self._cancel_restore:
            self._cancel_restore[key] = True
        self.fast.set_noevict(key, value)
        self._keys.add(key)

    def evict_until_below_target(self, n: float | None = None) -> None:
        """Wrapper around :meth:`zict.LRU.evict_until_below_target`.
        Presented here to allow easier overriding.
        """
        self.fast.evict_until_below_target(n)

    @locked
    def __delitem__(self, key: KT) -> None:
        self._keys.remove(key)
        if key in self._cancel_restore:
            self._cancel_restore[key] = True
        discard(self.fast, key)
        discard(self.slow, key)

    @locked
    def _cancel_evict(self, key: KT, value: VT) -> None:
        discard(self.slow, key)

    def __len__(self) -> int:
        return len(self._keys)

    def __iter__(self) -> Iterator[KT]:
        return iter(self._keys)

    def __contains__(self, key: object) -> bool:
        return key in self._keys

    @locked
    def __str__(self) -> str:
        s = f"Buffer<fast: {len(self.fast)}, slow: {len(self.slow)}"
        if self.keep_slow:
            ndup = len(self.fast) + len(self.slow) - len(self._keys)
            s += f", unique: {len(self._keys)}, duplicates: {ndup}"
        return s + ">"

    __repr__ = __str__

    def flush(self) -> None:
        flush(self.fast, self.slow)

    def close(self) -> None:
        close(self.fast, self.slow)
