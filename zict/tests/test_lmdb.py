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

    del z['abc']
    with pytest.raises(KeyError):
        z['abc']
    assert 'abc' not in z
    assert list(z) == ['xyz']
    assert len(z) == 1

    z['def'] = b'\x00\xff'
    assert len(z) == 2
    assert z['def'] == b'\x00\xff'
    assert 'def' in z

    assert set(z.values()) == {b'123', b'\x00\xff'}
