from collections import MutableMapping
import os
import shutil

import pytest

from zict.file import File

@pytest.yield_fixture
def fn():
    filename = '.tmp'
    if os.path.exists(filename):
        shutil.rmtree(filename)

    yield filename

    if os.path.exists(filename):
        shutil.rmtree(filename)


def test_simple(fn):
    z = File(fn)
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

    assert os.listdir(fn) == ['x']
    with open(os.path.join(fn, 'x'), 'rb') as f:
        assert f.read() == b'123'

    z['y'] = b'456'
    assert z['y'] == b'456'


def test_setitem_typeerror(fn):
    z = File(fn)
    with pytest.raises(TypeError):
        z['x'] = 123


def test_contextmanager(fn):
    with File(fn) as z:
        z['x'] = b'123'

    with open(os.path.join(fn, 'x'), 'rb') as f:
        assert f.read() == b'123'


def test_delitem(fn):
    z = File(fn)

    z['x'] = b'123'
    assert os.path.exists(os.path.join(z.directory, 'x'))
    del z['x']
    assert not os.path.exists(os.path.join(z.directory, 'x'))


def test_missing_key(fn):
    z = File(fn)

    with pytest.raises(KeyError):
        z['x']
