from __future__ import annotations

import mmap
import os
import pathlib
from collections.abc import Iterator
from urllib.parse import quote, unquote

from zict.common import ZictBase


def _safe_key(key: str) -> str:
    """
    Escape key so as to be usable on all filesystems.
    """
    # Even directory separators are unsafe.
    return quote(key, safe="")


def _unsafe_key(key: str) -> str:
    """
    Undo the escaping done by _safe_key().
    """
    return unquote(key)


class File(ZictBase[str, bytes]):
    """Mutable Mapping interface to a directory

    Keys must be strings, values must be buffers

    Note this shouldn't be used for interprocess persistence, as keys
    are cached in memory.

    Parameters
    ----------
    directory: str
        Directory to write to. If it already exists, existing files will be imported as
        mapping elements. If it doesn't exists, it will be created.
    memmap: bool (optional)
        If True, use `mmap` for reading. Defaults to False.

    Examples
    --------
    >>> z = File('myfile')  # doctest: +SKIP
    >>> z['x'] = b'123'  # doctest: +SKIP
    >>> z['x']  # doctest: +SKIP
    b'123'

    Also supports writing lists of bytes objects

    >>> z['y'] = [b'123', b'4567']  # doctest: +SKIP
    >>> z['y']  # doctest: +SKIP
    b'1234567'

    Or anything that can be used with file.write, like a memoryview

    >>> z['data'] = np.ones(5).data  # doctest: +SKIP
    """

    directory: str
    memmap: bool
    _keys: set[str]

    def __init__(self, directory: str | pathlib.Path, memmap: bool = False):
        self.directory = str(directory)
        self.memmap = memmap
        self._keys = set()
        if not os.path.exists(self.directory):
            os.makedirs(self.directory, exist_ok=True)
        else:
            for n in os.listdir(self.directory):
                self._keys.add(_unsafe_key(n))

    def __str__(self) -> str:
        return f"<File: {self.directory}, {len(self)} elements>"

    __repr__ = __str__

    def __getitem__(self, key: str) -> bytearray | memoryview:
        if key not in self._keys:
            raise KeyError(key)
        fn = os.path.join(self.directory, _safe_key(key))

        # distributed.protocol.numpy.deserialize_numpy_ndarray makes sure that, if the
        # numpy array was writeable before serialization, remains writeable afterwards.
        # If it receives a read-only buffer (e.g. from fh.read() or from a mmap to a
        # read-only file descriptor), it performs an expensive memcpy.
        # Note that this is a dask-specific feature; vanilla pickle.loads will instead
        # return an array with flags.writeable=False.
        if self.memmap:
            with open(fn, "r+b") as fh:
                return memoryview(mmap.mmap(fh.fileno(), 0))
        else:
            with open(fn, "rb") as fh:
                size = os.fstat(fh.fileno()).st_size
                buf = bytearray(size)
                nread = fh.readinto(buf)
                assert nread == size
                return buf

    def __setitem__(
        self,
        key: str,
        value: bytes
        | bytearray
        | memoryview
        | list[bytes | bytearray | memoryview]
        | tuple[bytes | bytearray | memoryview, ...],
    ) -> None:
        fn = os.path.join(self.directory, _safe_key(key))
        with open(fn, "wb") as fh:
            if isinstance(value, (tuple, list)):
                fh.writelines(value)
            else:
                fh.write(value)
        self._keys.add(key)

    def __contains__(self, key: object) -> bool:
        return key in self._keys

    # FIXME dictionary views https://github.com/dask/zict/issues/61
    def keys(self) -> set[str]:  # type: ignore
        return self._keys

    def __iter__(self) -> Iterator[str]:
        return iter(self._keys)

    def __delitem__(self, key: str) -> None:
        if key not in self._keys:
            raise KeyError(key)
        os.remove(os.path.join(self.directory, _safe_key(key)))
        self._keys.remove(key)

    def __len__(self) -> int:
        return len(self._keys)
