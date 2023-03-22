from __future__ import annotations

import platform
import sys
import threading
from collections import defaultdict
from collections.abc import Iterable, Iterator
from numbers import Number
from typing import MutableSet  # TODO import from collections.abc (needs Python >=3.9)

from zict.common import T


class InsertionSortedSet(MutableSet[T]):
    """A set-like that retains insertion order, like a dict. Thread-safe.

    Equality does not compare order or class, but only compares against the contents of
    any other set-like, coherently with dict and the AbstractSet design.
    """

    _d: dict[T, None]
    __slots__ = ("_d",)

    def __init__(self, other: Iterable[T] = ()) -> None:
        self._d = dict.fromkeys(other)

    def __contains__(self, item: object) -> bool:
        return item in self._d

    def __iter__(self) -> Iterator[T]:
        return iter(self._d)

    def __len__(self) -> int:
        return len(self._d)

    def add(self, value: T) -> None:
        """Add element to the set. If the element is already in the set, retain original
        insertion order.
        """
        self._d[value] = None

    def discard(self, value: T) -> None:
        # Don't trust the thread-safety of self._d.pop(value, None)
        try:
            del self._d[value]
        except KeyError:
            pass

    def remove(self, value: T) -> None:
        del self._d[value]

    def popleft(self) -> T:
        """Pop the oldest-inserted key from the set"""
        while True:
            try:
                value = next(iter(self._d))
                del self._d[value]
                return value
            except StopIteration:
                raise KeyError("pop from an empty set")
            except (KeyError, RuntimeError):
                # Multithreaded race condition
                continue

    def popright(self) -> T:
        """Pop the latest-inserted key from the set"""
        return self._d.popitem()[0]

    pop = popright

    def clear(self) -> None:
        self._d.clear()


ATOMIC_INT_IADD = (
    platform.python_implementation() == "CPython" and sys.version_info >= (3, 10)
)


class Accumulator(Number):
    """A lockless thread-safe accumulator"""

    _values: defaultdict[int, float]
    __slots__ = ("_values",)

    def __new__(cls, value: float = 0) -> Accumulator:
        if ATOMIC_INT_IADD:
            # int.__iadd__ and float.__iadd__ are GIL-atomic starting from CPython 3.10.
            # We can get rid of the whole class and just use them instead.
            # This is an implementation detail.
            return value  # type: ignore[return-value]

        self = object.__new__(cls)
        # Don't return float unless you actually added floats.
        # This behaviour is consistent with sum().
        self._values = defaultdict(int)
        self._values[threading.get_ident()] = value
        return self

    def _value(self) -> float:
        """Return accumulator total across all threads.
        The return type is float if any float elements were added, otherwise it's int.
        """
        while True:
            try:
                return sum(self._values.values())
            except RuntimeError:  # dictionary changed size during iteration
                pass  # pragma: nocover

    def __iadd__(self, other: float) -> Accumulator:
        self._values[threading.get_ident()] += other
        return self

    def __isub__(self, other: float) -> Accumulator:
        self._values[threading.get_ident()] -= other
        return self

    # Trivial wrappers around self._value().
    # Since they are magic methods, they can't be implemented with __getattr__
    # or with accessor classes.

    def __repr__(self) -> str:
        return repr(self._value())

    def __int__(self) -> int:
        return int(self._value())

    def __float__(self) -> float:
        return float(self._value())

    def __eq__(self, other: object) -> bool:
        return self._value() == other

    def __gt__(self, other: float) -> bool:
        return self._value() > other

    def __ge__(self, other: float) -> bool:
        return self._value() >= other

    def __lt__(self, other: float) -> bool:
        return self._value() < other

    def __le__(self, other: float) -> bool:
        return self._value() <= other

    def __add__(self, other: float) -> float:
        return self._value() + other

    def __sub__(self, other: float) -> float:
        return self._value() - other

    def __mul__(self, other: float) -> float:
        return self._value() * other

    def __truediv__(self, other: float) -> float:
        return self._value() / other

    def __hash__(self) -> int:
        return hash(self._value())
