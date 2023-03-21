import os
import pathlib
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from zict import File
from zict.tests import utils_test


def test_mapping(tmpdir):
    """
    Test mapping interface for File().
    """
    z = File(tmpdir)
    utils_test.check_mapping(z)


@pytest.mark.parametrize("dirtype", [str, pathlib.Path, lambda x: x])
def test_implementation(tmpdir, dirtype):
    z = File(dirtype(tmpdir))
    assert not z

    z["x"] = b"123"
    assert os.listdir(tmpdir) == ["x#0"]
    with open(tmpdir / "x#0", "rb") as f:
        assert f.read() == b"123"

    assert "x" in z
    out = z["x"]
    assert isinstance(out, bytearray)
    assert out == b"123"


def test_memmap_implementation(tmpdir):
    z = File(tmpdir, memmap=True)
    assert not z

    mv = memoryview(b"123")
    assert "x" not in z
    z["x"] = mv
    assert os.listdir(tmpdir) == ["x#0"]
    assert "x" in z
    mv2 = z["x"]
    assert mv2 == b"123"
    # Buffer is writeable
    mv2[0] = mv2[1]
    assert mv2 == b"223"


def test_str(tmpdir):
    z = File(tmpdir)
    assert str(z) == repr(z) == f"<File: {tmpdir}, 0 elements>"


def test_setitem_typeerror(tmpdir):
    z = File(tmpdir)
    with pytest.raises(TypeError):
        z["x"] = 123


def test_contextmanager(tmpdir):
    with File(tmpdir) as z:
        z["x"] = b"123"

    with open(tmpdir / "x#0", "rb") as fh:
        assert fh.read() == b"123"


def test_delitem(tmpdir):
    z = File(tmpdir)

    z["x"] = b"123"
    assert os.listdir(tmpdir) == ["x#0"]
    del z["x"]
    assert os.listdir(tmpdir) == []
    # File name is never repeated
    z["x"] = b"123"
    assert os.listdir(tmpdir) == ["x#1"]
    # __setitem__ deletes the previous file
    z["x"] = b"123"
    assert os.listdir(tmpdir) == ["x#2"]


def test_missing_key(tmpdir):
    z = File(tmpdir)

    with pytest.raises(KeyError):
        z["x"]


def test_arbitrary_chars(tmpdir):
    z = File(tmpdir)

    # Avoid hitting the Windows max filename length
    chunk = 16
    for i in range(1, 128, chunk):
        key = "".join(["foo_"] + [chr(i) for i in range(i, min(128, i + chunk))])
        with pytest.raises(KeyError):
            z[key]
        z[key] = b"foo"
        assert z[key] == b"foo"
        assert list(z) == [key]
        assert list(z.keys()) == [key]
        assert list(z.items()) == [(key, b"foo")]
        assert list(z.values()) == [b"foo"]

        zz = File(tmpdir)
        assert zz[key] == b"foo"
        assert list(zz) == [key]
        assert list(zz.keys()) == [key]
        assert list(zz.items()) == [(key, b"foo")]
        assert list(zz.values()) == [b"foo"]
        del zz

        del z[key]
        with pytest.raises(KeyError):
            z[key]


def test_write_list_of_bytes(tmpdir):
    z = File(tmpdir)

    z["x"] = [b"123", b"4567"]
    assert z["x"] == b"1234567"


def test_different_keys_threadsafe(tmpdir):
    """File is fully thread-safe as long as different threads operate on different keys"""
    z = File(tmpdir)
    barrier = Barrier(2)

    def worker(key, start):
        barrier.wait()
        t1 = time.perf_counter() + 2
        i = start
        while time.perf_counter() < t1:
            payload = str(i).encode("ascii")
            z[key] = payload
            assert z[key] == payload
            del z[key]

            assert key not in z
            with pytest.raises(KeyError):
                _ = z[key]
            with pytest.raises(KeyError):
                del z[key]

            i += 2
        return i // 2

    with ThreadPoolExecutor(2) as ex:
        f1 = ex.submit(worker, "x", 0)
        f2 = ex.submit(worker, "y", 1)
        assert f1.result() > 100
        assert f2.result() > 100

    assert not z
