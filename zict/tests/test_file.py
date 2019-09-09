from __future__ import absolute_import, division, print_function

import os
import shutil

import pytest

from zict.file import File, _WriteAheadLog
from . import utils_test


@pytest.yield_fixture
def fn():
    filename = '.tmp'
    if os.path.exists(filename):
        if not os.path.isdir(filename):
            os.remove(filename)
        else:
            shutil.rmtree(filename)

    yield filename

    if os.path.exists(filename):
        if not os.path.isdir(filename):
            os.remove(filename)
        else:
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

    assert 'x' in z


def test_str(fn):
    z = File(fn)
    assert fn in str(z)
    assert fn in repr(z)
    assert z.mode in str(z)
    assert z.mode in repr(z)


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


def test_arbitrary_chars(fn):
    z = File(fn)

    # Avoid hitting the Windows max filename length
    chunk = 16
    for i in range(1, 128, chunk):
        key = ''.join(['foo_'] + [chr(i) for i in range(i, min(128, i + chunk))])
        with pytest.raises(KeyError):
            z[key]
        z[key] = b'foo'
        assert z[key] == b'foo'
        assert list(z) == [key]
        assert list(z.keys()) == [key]
        assert list(z.items()) == [(key, b'foo')]
        assert list(z.values()) == [b'foo']

        zz = File(fn)
        assert zz[key] == b'foo'
        assert list(zz) == [key]
        assert list(zz.keys()) == [key]
        assert list(zz.items()) == [(key, b'foo')]
        assert list(zz.values()) == [b'foo']
        del zz

        del z[key]
        with pytest.raises(KeyError):
            z[key]


def test_write_list_of_bytes(fn):
    z = File(fn)

    z['x'] = [b'123', b'4567']
    assert z['x'] == b'1234567'


def test_item_with_very_long_name_can_be_read_and_deleted_and_restored(fn):
    z = File(fn)
    long_key1 = 'a' + 'a'.join(str(i) for i in range(500))
    long_key2 = 'b' + 'a'.join(str(i) for i in range(500))
    z[long_key1] = b'key1'
    z[long_key2] = b'key2'
    assert z[long_key1] == b'key1'
    assert z[long_key2] == b'key2'
    z2 = File(fn)
    assert z2[long_key1] == b'key1'
    assert z2[long_key2] == b'key2'
    del z2[long_key1]
    z3 = File(fn)
    assert long_key1 not in z3
    assert z3[long_key2] == b'key2'


def test_write_ahead_log_can_record_keys_and_replay_them_back(fn):
    file_path = fn
    wal = _WriteAheadLog(file_path)
    expected = [
        ('key1', 'val1', 'a'),
        ('key2', 'val2', 'a'),
        ('key3', 'val3', 'd')
    ]
    for key, val, action in expected:
        wal.write_key_value_and_action(key, val, action)

    vals = wal.get_all_pairs()
    assert expected == vals


def test_write_ahead_log_can_read_keys_from_file_writen_by_another_instance(fn):
    file_path = fn
    wal = _WriteAheadLog(file_path)
    expected = [
        ('key1', 'val1', 'a'),
        ('keyxxxx2', 'valllll2', 'a'),
        ('key3', 'val3', 'd')
    ]
    for key, val, action in expected:
        wal.write_key_value_and_action(key, val, action)

    wal2 = _WriteAheadLog(file_path)

    vals = wal2.get_all_pairs()
    assert expected == vals