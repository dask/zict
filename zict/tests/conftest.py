import gc
import sys

import pytest

try:
    import psutil
except ImportError:
    psutil = None  # type: ignore


@pytest.fixture
def check_fd_leaks():
    if sys.platform == "win32" or psutil is None:
        yield
    else:
        proc = psutil.Process()
        before = proc.num_fds()
        yield
        gc.collect()
        assert proc.num_fds() == before
