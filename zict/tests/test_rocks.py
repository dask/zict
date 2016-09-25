from collections import MutableMapping

import pytest

from zict import RocksDB
from zict.utils import tmpfile


def test_simple(tmpfile):
    z = RocksDB(tmpfile)
    assert isinstance(z, MutableMapping)
    assert not z

    assert list(z) == list(z.keys()) == []
    assert list(z.values()) == []
    assert list(z.items()) == []

    z[b'x'] = b'123'
    assert list(z) == list(z.keys()) == [b'x']
    assert list(z.values()) == [b'123']
    assert list(z.items()) == [(b'x', b'123')]
    assert z[b'x'] == b'123'

    z[b'y'] = b'456'
    assert z[b'y'] == b'456'


def test_setitem_typeerror(tmpfile):
    z = RocksDB(tmpfile)
    with pytest.raises(TypeError):
        z['x'] = 123


def test_missing_key(tmpfile):
    z = RocksDB(tmpfile)

    with pytest.raises(KeyError):
        z[b'x']


def test_bytearray(tmpfile):
    data = bytearray(b'123')
    z = RocksDB(tmpfile)
    z[b'x'] = data
    assert z[b'x'] == b'123'
