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

    z['x'] = b'123'
    assert list(z) == list(z.keys()) == ['x']
    assert list(z.values()) == [b'123']
    assert list(z.items()) == [('x', b'123')]
    assert z['x'] == b'123'

    z['y'] = b'456'
    assert z['y'] == b'456'


def test_setitem_typeerror(tmpfile):
    z = RocksDB(tmpfile)
    with pytest.raises(TypeError):
        z['x'] = 123


def test_missing_key(tmpfile):
    z = RocksDB(tmpfile)

    with pytest.raises(KeyError):
        z['x']


def test_bytearray(tmpfile):
    data = bytearray(b'123')
    z = RocksDB(tmpfile)
    z['x'] = data
    assert z['x'] == b'123'
