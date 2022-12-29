import os
import pathlib

import pytest

from zict.file import File
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
    assert os.listdir(tmpdir) == ["x"]
    with open(tmpdir / "x", "rb") as f:
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
    assert os.listdir(tmpdir) == ["x"]
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

    with open(tmpdir / "x", "rb") as fh:
        assert fh.read() == b"123"


def test_delitem(tmpdir):
    z = File(tmpdir)

    z["x"] = b"123"
    assert os.listdir(tmpdir) == ["x"]
    del z["x"]
    assert os.listdir(tmpdir) == []


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
