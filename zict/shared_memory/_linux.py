"""Linux implementation of :class:`zict.SharedMemory`.

Wraps around glibc ``memfd_create``.
"""
from __future__ import annotations

import ctypes
import mmap
import os
from collections.abc import Iterable

_memfd_create = None


def _setitem(safe_key: str, value: Iterable[bytes | bytearray | memoryview]) -> int:
    global _memfd_create
    if _memfd_create is None:
        libc = ctypes.CDLL("libc.so.6")
        _memfd_create = libc.memfd_create

    fd = _memfd_create(safe_key.encode("ascii"), 0)
    if fd == -1:
        raise OSError("Call to memfd_create failed")  # pragma: nocover

    with os.fdopen(fd, "wb", closefd=False) as fh:
        fh.writelines(value)

    return fd


def _getitem(fd: int) -> memoryview:
    # This opens a second fd for as long as the memory map is referenced.
    # Sadly there does not seem a way to extract the fd from the mmap, so we have to
    # keep the original fd open for the purpose of exporting.
    return memoryview(mmap.mmap(fd, 0))


def _delitem(fd: int) -> None:
    # Close the original fd. There may be other fd's still open if the shared memory is
    # referenced somewhere else.
    # This is also called by SharedMemory.__del__.
    os.close(fd)


def _export(safe_key: str, fd: int) -> tuple:
    return safe_key, os.getpid(), fd


def _import(safe_key: str, pid: int, fd: int) -> int:
    # if fd has been closed, raise FileNotFoundError
    # if fd has been closed and reopened to something else, this may also raise a
    # generic OSError, e.g. if this is now a socket
    new_fd = os.open(f"/proc/{pid}/fd/{fd}", os.O_RDWR)

    expect = f"/memfd:{safe_key} (deleted)"
    actual = os.readlink(f"/proc/{os.getpid()}/fd/{new_fd}")
    if actual != expect:
        # fd has been closed and reopened to something else
        os.close(new_fd)
        raise OSError()

    return new_fd
