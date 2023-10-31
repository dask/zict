from __future__ import annotations

import secrets
import sys
from collections.abc import Iterator, KeysView
from typing import Any
from urllib.parse import quote, unquote

from zict.common import ZictBase

if sys.platform == "linux":
    from zict.shared_memory._linux import _delitem, _export, _getitem, _import, _setitem
elif sys.platform == "win32":
    from zict.shared_memory._windows import (
        _delitem,
        _export,
        _getitem,
        _import,
        _setitem,
    )


class SharedMemory(ZictBase[str, memoryview]):
    """Mutable Mapping interface to shared memory.

    **Supported OSs:** Linux, Windows

    Keys must be strings, values must be buffers.
    Keys are stored in private memory, and other SharedMemory objects by default won't
    see them - even in case of key collision, the two pieces of data remain separate.

    In order to share the same buffer, one SharedMemory object must call
    :meth:`export` and the other :meth:`import_`.

    **Resources usage**

    On Linux, you will hold 1 file descriptor open for every key in the SharedMemory
    mapping, plus 1 file descriptor for every returned memoryview that is referenced
    somewhere else. Please ensure that your ``ulimit`` is high enough to cope with this.

    If you expect to call ``__getitem__`` multiple times on the same key while the
    return value from the previous call is still in use, you should wrap this mapping in
    a :class:`~zict.Cache`:

    >>> import zict
    >>> shm = zict.Cache(
    ...     zict.SharedMemory(),
    ...     zict.WeakValueMapping(),
    ...     update_on_set=False,
    ... ) # doctest: +SKIP

    The above will cap the amount of open file descriptors per key to 2.

    **Lifecycle**

    Memory is released when all the SharedMemory objects that were sharing the key have
    deleted it *and* the buffer returned by ``__getitem__`` is no longer referenced
    anywhere else.
    Process termination, including ungraceful termination (SIGKILL, SIGSEGV), also
    releases the memory; in other words you don't risk leaking memory to the
    OS if all processes that were sharing it crash or are killed.

    Examples
    --------
    In process 1:

    >>> import pickle, numpy, zict  # doctest: +SKIP
    >>> shm = zict.SharedMemory()  # doctest: +SKIP
    >>> a = numpy.random.random(2**27)  # 1 GiB  # doctest: +SKIP
    >>> buffers = []  # doctest: +SKIP
    >>> pik = pickle.dumps(a, protocol=5, buffer_callback=buffers.append)
    ... # doctest: +SKIP
    >>> # This deep-copies the buffer, resulting in 1 GiB private + 1 GiB shared memory.
    >>> shm["a"] = buffers  # doctest: +SKIP
    >>> # Release private memory, leaving only the shared memory allocated
    >>> del a, buffers  # doctest: +SKIP
    >>> # Recreate array from shared memory. This requires no extra memory.
    >>> a = pickle.loads(pik, buffers=[shm["a"]])  # doctest: +SKIP
    >>> # Send trivially-sized metadata (<1 kiB) to the peer process somehow.
    >>> send_to_process_2((pik, shm.export("a")))  # doctest: +SKIP

    In process 2:

    >>> import pickle, zict  # doctest: +SKIP
    >>> shm = zict.SharedMemory()  # doctest: +SKIP
    >>> pik, metadata = receive_from_process_1()  # doctest: +SKIP
    >>> key = shm.import_(metadata)  # returns "a"  # doctest: +SKIP
    >>> a = pickle.loads(pik, buffers=[shm[key]])  # doctest: +SKIP

    Now process 1 and 2 hold a reference to the same memory; in-place changes on one
    process are reflected onto the other. The shared memory is released after you delete
    the key and dereference the buffer returned by ``__getitem__`` on *both* processes:

    >>> del shm["a"]  # doctest: +SKIP
    >>> del a  # doctest: +SKIP

    or alternatively when both processes are terminated.

    **Implementation notes**

    This mapping uses OS-specific shared memory, which

    1. can be shared among already existing processes, e.g. unlike ``mmap(fd=-1)``, and
    2. is automatically cleaned up by the OS in case of ungraceful process termination,
       e.g. unlike ``shm_open`` (which is used by :mod:`multiprocessing.shared_memory`
       on all POSIX OS'es)

    It is implemented on top of ``memfd_create`` on Linux and ``CreateFileMapping`` on
    Windows. Notably, there is no POSIX equivalent for these API calls, as it only
    implements ``shm_open`` which would inevitably cause memory leaks in case of
    ungraceful process termination.
    """

    # {key: (unique safe key, implementation-specific data)}
    _data: dict[str, tuple[str, Any]]

    def __init__(self):  # type: ignore[no-untyped-def]
        if sys.platform not in ("linux", "win32"):
            raise NotImplementedError(
                "SharedMemory is only available on Linux and Windows"
            )

        self._data = {}

    def __str__(self) -> str:
        return f"<SharedMemory: {len(self)} elements>"

    __repr__ = __str__

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

        if not isinstance(value, (tuple, list)):
            value = [value]
        safe_key = quote(key, safe="") + "#" + secrets.token_bytes(8).hex()
        impl_data = _setitem(safe_key, value)
        self._data[key] = safe_key, impl_data

    def __getitem__(self, key: str) -> memoryview:
        _, impl_data = self._data[key]
        return _getitem(impl_data)

    def __delitem__(self, key: str) -> None:
        _, impl_data = self._data.pop(key)
        _delitem(impl_data)

    def __del__(self) -> None:
        try:
            data_values = self._data.values()
        except Exception:
            # Interpreter shutdown
            return  # pragma: nocover

        for _, impl_data in data_values:
            try:
                _delitem(impl_data)
            except Exception:
                pass  # pragma: nocover

    def close(self) -> None:
        # Implements ZictBase.close(). Also triggered by __exit__.
        self.clear()

    def __contains__(self, key: object) -> bool:
        return key in self._data

    def keys(self) -> KeysView[str]:
        return self._data.keys()

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def export(self, key: str) -> tuple:
        """Export metadata for a key, which can be fed into :meth:`import_` on
        another process.

        Returns
        -------
        Opaque metadata object (implementation-specific) to be passed to
        :meth:`import_`. It is serializable with JSON, YAML, and msgpack.

        See Also
        --------
        import_
        """
        return _export(*self._data[key])

    def import_(self, metadata: tuple | list) -> str:
        """Import a key from another process, starting to share the memory area.

        You should treat parameters as implementation details and just unpack the tuple
        that was generated by :meth:`export`.

        Returns
        -------
        Key that was just added to the mapping

        Raises
        ------
        FileNotFoundError
            Either the key or the whole SharedMemory object were deleted on the process
            where you ran :meth:`export`, or the process was terminated.

        Notes
        -----
        On Windows, this method will raise FileNotFoundError if the key has been deleted
        from the other SharedMemory mapping *and* it is no longer referenced anywhere.
        On Linux, this method will raise as soon as the key is deleted from the other
        SharedMemory mapping, even if it's still referenced.

        e.g. this code is not portable, as it will work on Windows but not on Linux:

        >>> buf = shm["x"] = buf  # doctest: +SKIP
        >>> meta = shm.export("x")  # doctest: +SKIP
        >>> del shm["x"]  # doctest: +SKIP

        See Also
        --------
        export
        """
        safe_key = metadata[0]
        key = unquote(safe_key.split("#")[0])

        try:
            del self[key]
        except KeyError:
            pass

        try:
            impl_data = _import(*metadata)
        except OSError:
            raise FileNotFoundError(f"Peer process no longer holds the key: {key!r}")
        self._data[key] = safe_key, impl_data
        return key
