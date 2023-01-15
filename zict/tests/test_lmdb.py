import gc
import os
import pathlib
import sys

import pytest

from zict import LMDB
from zict.tests import utils_test

pytest.importorskip("lmdb")


@pytest.mark.parametrize("dirtype", [str, pathlib.Path, lambda x: x])
def test_dirtypes(tmpdir, dirtype):
    z = LMDB(tmpdir)
    z["x"] = b"123"
    assert z["x"] == b"123"
    del z["x"]


def test_mapping(tmpdir):
    """
    Test mapping interface for LMDB().
    """
    z = LMDB(tmpdir)
    utils_test.check_mapping(z)


def test_reuse(tmpdir):
    """
    Test persistence of a LMDB() mapping.
    """
    with LMDB(tmpdir) as z:
        assert len(z) == 0
        z["abc"] = b"123"

    with LMDB(tmpdir) as z:
        assert len(z) == 1
        assert z["abc"] == b"123"


def test_creates_dir(tmpdir):
    with LMDB(tmpdir):
        assert os.path.isdir(tmpdir)


@pytest.mark.skipif(sys.platform == "win32", reason="requires psutil.Process.num_fds")
def test_file_descriptors_dont_leak(tmpdir):
    psutil = pytest.importorskip("psutil")
    proc = psutil.Process()
    before = proc.num_fds()

    z = LMDB(tmpdir)
    del z
    gc.collect()

    assert proc.num_fds() == before

    z = LMDB(tmpdir)
    z.close()

    assert proc.num_fds() == before

    with LMDB(tmpdir) as z:
        pass

    assert proc.num_fds() == before


def test_map_size(tmpdir):
    import lmdb

    z = LMDB(tmpdir, map_size=2**20)
    z["x"] = b"x" * 2**19
    with pytest.raises(lmdb.MapFullError):
        z["y"] = b"x" * 2**20
