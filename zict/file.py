from __future__ import annotations

import mmap
import os
import pathlib
from collections.abc import Iterator, KeysView
from urllib.parse import quote, unquote

from zict.common import ZictBase


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

    Notes
    -----
    This class is fully thread-safe, with only one caveat: you can't have two calls to
    ``__setitem__``, on the same key, at the same time from two different threads.

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
    filenames: dict[str, str]
    _inc: int

    def __init__(self, directory: str | pathlib.Path, memmap: bool = False):
        self.directory = str(directory)
        self.memmap = memmap
        self.filenames = {}
        self._inc = 0

        if not os.path.exists(self.directory):
            os.makedirs(self.directory, exist_ok=True)
        else:
            for fn in os.listdir(self.directory):
                self.filenames[self._unsafe_key(fn)] = fn
                self._inc += 1

    def _safe_key(self, key: str) -> str:
        """Escape key so that it is usable on all filesystems.

        Append to the filenames a unique suffix that changes every time this method is
        called. This prevents race conditions when another thread accesses the same
        key, e.g. ``__setitem__`` on one thread and ``__getitem__`` on another.
        """
        # `#` is escaped by quote and is supported by most file systems
        key = quote(key, safe="") + f"#{self._inc}"
        self._inc += 1
        return key

    @staticmethod
    def _unsafe_key(key: str) -> str:
        """Undo the escaping done by _safe_key()"""
        key = key.split("#")[0]
        return unquote(key)

    def __str__(self) -> str:
        return f"<File: {self.directory}, {len(self)} elements>"

    __repr__ = __str__

    def __getitem__(self, key: str) -> bytearray | memoryview:
        fn = os.path.join(self.directory, self.filenames[key])

        # distributed.protocol.numpy.deserialize_numpy_ndarray makes sure that, if the
        # numpy array was writeable before serialization, remains writeable afterwards.
        # If it receives a read-only buffer (e.g. from fh.read() or from a mmap to a
        # read-only file descriptor), it performs an expensive memcpy.
        # Note that this is a dask-specific feature; vanilla pickle.loads will instead
        # return an array with flags.writeable=False.

        try:
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

        except FileNotFoundError:  # pragma: nocover
            raise KeyError(key)  # Race condition with __setitem__ or __delitem__

    def __setitem__(
        self,
        key: str,
        value: bytes
        | bytearray
        | memoryview
        | list[bytes | bytearray | memoryview]
        | tuple[bytes | bytearray | memoryview, ...],
    ) -> None:
        try:
            del self[key]
        except KeyError:
            pass

        fn = self._safe_key(key)
        with open(os.path.join(self.directory, fn), "wb") as fh:
            if isinstance(value, (tuple, list)):
                fh.writelines(value)
            else:
                fh.write(value)
        self.filenames[key] = fn

    def __contains__(self, key: object) -> bool:
        return key in self.filenames

    def keys(self) -> KeysView[str]:
        return self.filenames.keys()

    def __iter__(self) -> Iterator[str]:
        return iter(self.filenames)

    def __delitem__(self, key: str) -> None:
        fn = self.filenames.pop(key)
        try:
            os.remove(os.path.join(self.directory, fn))
        except FileNotFoundError:  # pragma: nocover
            raise KeyError(key)  # Race condition with __setitem__ or __delitem__

    def __len__(self) -> int:
        return len(self.filenames)
