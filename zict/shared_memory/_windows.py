"""Windows implementation of :class:`zict.SharedMemory`.

Conveniently, :class:`multiprocessing.shared_memory.SharedMemory` already wraps around
the Windows API we want to use, so this is implemented as a hack on top of it.
"""
from __future__ import annotations

import mmap
import multiprocessing.shared_memory
from collections.abc import Collection
from typing import cast


class _PySharedMemoryNoClose(multiprocessing.shared_memory.SharedMemory):
    def __del__(self) -> None:
        pass


def _setitem(
    safe_key: str, value: Collection[bytes | bytearray | memoryview]
) -> memoryview:
    nbytes = sum(v.nbytes if isinstance(v, memoryview) else len(v) for v in value)
    shm = _PySharedMemoryNoClose(safe_key, create=True, size=nbytes)
    mm = cast(mmap.mmap, shm.buf.obj)
    for v in value:
        mm.write(v)
    # This dereferences shm; if we hadn't overridden the __del__ method, it would cause
    # it to automatically close the memory map and deallocate the shared memory.
    return shm.buf


def _getitem(mm: memoryview) -> memoryview:
    # Nothing to do. This is just for compatibility with the Linux implementation, which
    # instead creates a memory map on the fly.
    return mm


def _delitem(mm: memoryview) -> None:
    # Nothing to do. The shared memory is released as soon as the last memory map
    # referencing it is destroyed.
    pass


def _export(safe_key: str, mm: memoryview) -> tuple:
    return (safe_key,)


def _import(safe_key: str) -> memoryview:
    # Raises OSError in case of invalid key
    shm = _PySharedMemoryNoClose(safe_key)
    return shm.buf
