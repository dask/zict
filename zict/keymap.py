from __future__ import annotations

from collections.abc import Callable, Iterator, MutableMapping
from typing import Generic, TypeVar

from zict.common import KT, VT, ZictBase, close, discard, flush, locked

JT = TypeVar("JT")


class KeyMap(ZictBase[KT, VT], Generic[KT, JT, VT]):
    """Translate the keys of a MutableMapping with a pair of input/output functions

    Parameters
    ----------
    fn: callable
        Function to call on a key of the KeyMap to transform it to a key of the wrapped
        mapping. It must be pure (if called twice on the same key it must return
        the same result) and it must not generate collisions. In other words,
        ``fn(a) == fn(b) iff a == b``.

    d: MutableMapping
        Wrapped mapping

    See Also
    --------
    Func

    Examples
    --------
    Use any python object as keys of a File, instead of just strings, as long as their
    str representation is unique:

    >>> from zict import File
    >>> z = KeyMap(str, File("myfile"))  # doctest: +SKIP
    >>> z[1] = 10  # doctest: +SKIP
    """

    fn: Callable[[KT], JT]
    d: MutableMapping[JT, VT]
    keymap: dict[KT, JT]

    def __init__(self, fn: Callable[[KT], JT], d: MutableMapping[JT, VT]):
        super().__init__()
        self.fn = fn
        self.d = d
        self.keymap = {}

    @locked
    def __setitem__(self, key: KT, value: VT) -> None:
        j = self.fn(key)
        self.keymap[key] = j
        with self.unlock():
            self.d[j] = value
        if key not in self.keymap:
            # Race condition with __delitem__
            discard(self.d, j)

    def __getitem__(self, key: KT) -> VT:
        j = self.keymap[key]
        return self.d[j]

    @locked
    def __delitem__(self, key: KT) -> None:
        j = self.keymap.pop(key)
        del self.d[j]

    def __contains__(self, key: object) -> bool:
        return key in self.keymap

    def __iter__(self) -> Iterator[KT]:
        return iter(self.keymap)

    def __len__(self) -> int:
        return len(self.keymap)

    def flush(self) -> None:
        flush(self.d)

    def close(self) -> None:
        close(self.d)
