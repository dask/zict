from __future__ import absolute_import, division, print_function

import os
import shutil

import pytest

from zict.file import File
from . import utils_test


@pytest.yield_fixture
def fn():
    filename = '.tmp'
    if os.path.exists(filename):
        shutil.rmtree(filename)

    yield filename

    if os.path.exists(filename):
        shutil.rmtree(filename)


def test_mapping(fn):
    """
    Test mapping interface for File().
    """
    z = File(fn)
    utils_test.check_mapping(z)


def test_implementation(fn):
    z = File(fn)
    assert not z

    z['x'] = b'123'
    assert os.listdir(fn) == ['x']
    with open(os.path.join(fn, 'x'), 'rb') as f:
        assert f.read() == b'123'


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
