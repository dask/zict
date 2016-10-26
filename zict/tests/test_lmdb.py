from __future__ import absolute_import, division, print_function

import os
import shutil
import tempfile

import pytest

from zict.lmdb import LMDB
from . import common


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
    common.check_mapping(z)


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
