from __future__ import annotations

import sys
from collections.abc import Iterable, Iterator
from multiprocessing.managers import SharedMemoryManager
from multiprocessing.shared_memory import SharedMemory

from zict.common import ZictBase


class SMDB(ZictBase[str, bytes]):
    """Mutable Mapping interface to a SharedMemoryManager.

    Keys must be strings, values must be bytes

    Parameters
    ----------
    directory: string

    Examples
    --------
    >>> z = SMDB('/tmp/somedir/')  # doctest: +SKIP
    >>> z['x'] = b'123'  # doctest: +SKIP
    >>> z['x']  # doctest: +SKIP
    b'123'
    """

    def __init__(self, directory: str):
        self.shmm = SharedMemoryManager(directory)
        self.shmm.start()
        self.mapping = dict()

    def __getitem__(self, key: str) -> SharedMemory:
        return self.mapping[key]

    def __setitem__(self, key: str, value: bytes) -> None:
        with self.db.begin(write=True) as txn:
            txn.put(_encode_key(key), value)

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        with self.db.begin() as txn:
            return txn.cursor().set_key(_encode_key(key))

    # FIXME dictionary views https://github.com/dask/zict/issues/61
    def items(self) -> Iterator[tuple[str, bytes]]:  # type: ignore
        cursor = self.db.begin().cursor()
        return ((_decode_key(k), v) for k, v in cursor.iternext(keys=True, values=True))

    def keys(self) -> Iterator[str]:  # type: ignore
        cursor = self.db.begin().cursor()
        return (_decode_key(k) for k in cursor.iternext(keys=True, values=False))

    def values(self) -> Iterator[bytes]:  # type: ignore
        cursor = self.db.begin().cursor()
        return cursor.iternext(keys=False, values=True)

    def _do_update(self, items: Iterable[tuple[str, bytes]]) -> None:
        # Optimized version of update() using a single putmulti() call.
        items_enc = [(_encode_key(k), v) for k, v in items]
        with self.db.begin(write=True) as txn:
            consumed, added = txn.cursor().putmulti(items_enc)
            assert consumed == added == len(items_enc)

    def __iter__(self) -> Iterator[str]:
        return self.keys()

    def __delitem__(self, key: str) -> None:
        with self.db.begin(write=True) as txn:
            if not txn.delete(_encode_key(key)):
                raise KeyError(key)

    def __len__(self) -> int:
        return self.db.stat()["entries"]

    def close(self) -> None:
        self.shmm.shutdown()
