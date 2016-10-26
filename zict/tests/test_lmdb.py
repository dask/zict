from collections import MutableMapping
import os
import shutil
import tempfile

import pytest

from zict.lmdb import LMDB


@pytest.yield_fixture
def fn():
    dirname = tempfile.mkdtemp(prefix='test_lmdb-')
    try:
        yield dirname
    finally:
        if os.path.exists(dirname):
            shutil.rmtree(dirname)


def test_mapping(fn):
    """
    Test mapping interface for LMDB().
    """
    z = LMDB(fn)
    assert isinstance(z, MutableMapping)
    assert not z

    assert list(z) == list(z.keys()) == []
    assert list(z.values()) == []
    assert list(z.items()) == []
    assert len(z) == 0

    z['abc'] = b'456'
    z['xyz'] = b'123'
    assert len(z) == 2
    assert z['abc'] == b'456'

    items = list(z.items())
    assert set(items) == {('abc', b'456'), ('xyz', b'123')}
    assert list(z.keys()) == [k for k, v in items]
    assert list(z.values()) == [v for k, v in items]
    assert list(z) == [k for k, v in items]

    assert 'abc' in z
    assert 'xyz' in z
    assert 'def' not in z

    with pytest.raises(KeyError):
        z['def']

    z.update(xyz=b'707', uvw=b'000')
    assert set(z.items()) == {('abc', b'456'), ('xyz', b'707'), ('uvw', b'000')}
    z.update([('xyz', b'654'), ('uvw', b'999')])
    assert set(z.items()) == {('abc', b'456'), ('xyz', b'654'), ('uvw', b'999')}
    z.update({'xyz': b'321'})
    assert set(z.items()) == {('abc', b'456'), ('xyz', b'321'), ('uvw', b'999')}

    del z['abc']
    with pytest.raises(KeyError):
        z['abc']
    with pytest.raises(KeyError):
        del z['abc']
    assert 'abc' not in z
    assert set(z) == {'uvw', 'xyz'}
    assert len(z) == 2

    z['def'] = b'\x00\xff'
    assert len(z) == 3
    assert z['def'] == b'\x00\xff'
    assert 'def' in z


def test_reuse(fn):
    """
    Test persistence of a LMDB() mapping.
    """
    with LMDB(fn) as z:
        assert len(z) == 0
        z['abc'] = b'123'

    with LMDB(fn) as z:
        assert len(z) == 1
        assert z['abc'] == b'123'


def test_implementation_details(fn):
    with LMDB(fn) as z:
        assert os.path.isdir(fn)
