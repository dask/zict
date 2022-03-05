from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator, Mapping, MutableMapping
from itertools import chain
from typing import Generic, TypeVar

from .common import KT, VT, ZictBase, close

MKT = TypeVar("MKT")


class Sieve(ZictBase[KT, VT], Generic[KT, VT, MKT]):
    """Store values in different mappings based on a selector's
    output.

    This creates a MutableMapping combining several underlying
    MutableMappings for storage.  Items are dispatched based on
    a selector function provided by the user.

    Parameters
    ----------
    mappings: dict of {mapping key: MutableMapping}
    selector: callable (key, value) -> mapping key

    Examples
    --------
    >>> small = {}
    >>> large = DataBase()                        # doctest: +SKIP
    >>> mappings = {True: small, False: large}    # doctest: +SKIP
    >>> def is_small(key, value):                 # doctest: +SKIP
            return sys.getsizeof(value) < 10000
    >>> d = Sieve(mappings, is_small)             # doctest: +SKIP

    See Also
    --------
    Buffer
    """

    mappings: Mapping[MKT, MutableMapping[KT, VT]]
    selector: Callable[[KT, VT], MKT]
    key_to_mapping: dict[KT, MutableMapping[KT, VT]]

    def __init__(
        self,
        mappings: Mapping[MKT, MutableMapping[KT, VT]],
        selector: Callable[[KT, VT], MKT],
    ):
        self.mappings = mappings
        # FIXME https://github.com/python/mypy/issues/708
        self.selector = selector  # type: ignore
        self.key_to_mapping = {}

    def __getitem__(self, key: KT) -> VT:
        return self.key_to_mapping[key][key]

    def __setitem__(self, key: KT, value: VT) -> None:
        old_mapping = self.key_to_mapping.get(key)
        mkey = self.selector(key, value)  # type: ignore
        mapping = self.mappings[mkey]
        if old_mapping is not None and old_mapping is not mapping:
            del old_mapping[key]
        mapping[key] = value
        self.key_to_mapping[key] = mapping

    def __delitem__(self, key: KT) -> None:
        del self.key_to_mapping.pop(key)[key]

    def _do_update(self, items: Iterable[tuple[KT, VT]]) -> None:
        # Optimized update() implementation issuing a single update()
        # call per underlying mapping.
        updates = defaultdict(list)
        mapping_ids = {id(m): m for m in self.mappings.values()}

        for key, value in items:
            old_mapping = self.key_to_mapping.get(key)
            mkey = self.selector(key, value)  # type: ignore
            mapping = self.mappings[mkey]
            if old_mapping is not None and old_mapping is not mapping:
                del old_mapping[key]
            # Can't hash a mutable mapping, so use its id() instead
            updates[id(mapping)].append((key, value))

        for mid, mitems in updates.items():
            mapping = mapping_ids[mid]
            mapping.update(mitems)
            for key, _ in mitems:
                self.key_to_mapping[key] = mapping

    def keys(self) -> Iterator[KT]:
        return chain.from_iterable(self.mappings.values())

    def values(self) -> Iterator[VT]:
        return chain.from_iterable(m.values() for m in self.mappings.values())

    def items(self) -> Iterator[tuple[KT, VT]]:
        return chain.from_iterable(m.items() for m in self.mappings.values())

    def __len__(self) -> int:
        return sum(map(len, self.mappings.values()))

    def __iter__(self) -> Iterator[KT]:
        return iter(self.keys())

    def __contains__(self, key: object) -> bool:
        return key in self.key_to_mapping

    def __str__(self) -> str:
        return f"Sieve<{self.mappings}>"

    __repr__ = __str__

    def flush(self) -> None:
        for m in self.mappings.values():
            if hasattr(m, "flush"):
                m.flush()  # type: ignore

    def close(self) -> None:
        for m in self.mappings.values():
            close(m)
