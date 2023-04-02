from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest


@pytest.fixture
def is_locked():
    """Callable that returns True if the parameter zict mapping has its RLock engaged"""
    with ThreadPoolExecutor(1) as ex:

        def __is_locked(d):
            out = d.lock.acquire(blocking=False)
            if out:
                d.lock.release()
            return not out

        def _is_locked(d):
            return ex.submit(__is_locked, d).result()

        yield _is_locked
