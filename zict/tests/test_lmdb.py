import gc
import os
import pathlib
import sys

import pytest

from zict import LMDB
from zict.tests import utils_test

pytest.importorskip("lmdb")


@pytest.mark.parametrize("dirtype", [str, pathlib.Path, lambda x: x])
def test_dirtypes(tmp_path, dirtype):
    z = LMDB(tmp_path)
    z["x"] = b"123"
    assert z["x"] == b"123"
    del z["x"]


def test_mapping(tmp_path):
    """
    Test mapping interface for LMDB().
    """
    z = LMDB(tmp_path)
    utils_test.check_mapping(z)


def test_reuse(tmp_path):
    """
    Test persistence of a LMDB() mapping.
    """
    with LMDB(tmp_path) as z:
        assert len(z) == 0
        z["abc"] = b"123"

    with LMDB(tmp_path) as z:
        assert len(z) == 1
        assert z["abc"] == b"123"


def test_creates_dir(tmp_path):
    with LMDB(tmp_path):
        assert os.path.isdir(tmp_path)


@pytest.mark.skipif(sys.platform == "win32", reason="requires psutil.Process.num_fds")
def test_file_descriptors_dont_leak(tmp_path):
    psutil = pytest.importorskip("psutil")
    proc = psutil.Process()
    before = proc.num_fds()

    z = LMDB(tmp_path)
    del z
    gc.collect()

    assert proc.num_fds() == before

    z = LMDB(tmp_path)
    z.close()

    assert proc.num_fds() == before

    with LMDB(tmp_path) as z:
        pass

    assert proc.num_fds() == before


def test_map_size(tmp_path):
    import lmdb

    z = LMDB(tmp_path, map_size=2**20)
    z["x"] = b"x" * 2**19
    with pytest.raises(lmdb.MapFullError):
        z["y"] = b"x" * 2**20
