from __future__ import annotations

import asyncio
import contextvars
from collections.abc import Callable, Collection
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from typing import Any, Literal

from zict.buffer import Buffer
from zict.common import KT, VT, T, locked


class AsyncBuffer(Buffer[KT, VT]):
    """Extension of :class:`~zict.Buffer` that allows offloading all reads and writes
    from/to slow to a separate worker thread.

    This requires ``fast`` to be fully thread-safe (e.g. a plain dict).

    ``slow.__setitem__`` and ``slow.__getitem__`` will be called from the offloaded
    thread, while all of its other methods (including, notably for the purpose of
    thread-safety consideration, ``__contains__`` and ``__delitem__``) will be called
    from the main thread.
    """

    executor: ThreadPoolExecutor | None
    futures: set[asyncio.Future]
    evicting: float | None

    @wraps(Buffer.__init__)
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.executor = None
        self.futures = set()
        self.evicting = None

    def close(self) -> None:
        # Call LRU.close(), which stops LRU.evict_until_below_target() halfway through
        super().close()
        if self.executor is not None:
            # TODO just use shutdown(cancel_futures=True) (requires Python >=3.9)
            for future in self.futures:
                future.cancel()
            self.executor.shutdown(wait=True)
            self.executor = None

    def _offload(self, func: Callable[..., T], *args: Any) -> asyncio.Future[T]:
        if self.executor is None:
            self.executor = ThreadPoolExecutor(
                1, thread_name_prefix="zict.Buffer offloader"
            )

        loop = asyncio.get_running_loop()
        context = contextvars.copy_context()
        future = loop.run_in_executor(self.executor, context.run, func, *args)
        self.futures.add(future)
        future.add_done_callback(self.futures.remove)
        return future  # type: ignore[return-value]

    # Return an asyncio.Future, instead of just writing it as an async function, to make
    # it easier for overriders to tell apart the use case when all keys were already
    # in fast
    @locked
    def async_get(
        self, keys: Collection[KT], missing: Literal["raise", "omit"] = "raise"
    ) -> asyncio.Future[dict[KT, VT]]:
        """Fetch one or more key/value pairs. If not all keys are available in fast,
        offload to a worker thread moving keys from slow to fast, as well as possibly
        moving older keys from fast to slow.

        Parameters
        ----------
        keys:
            collection of zero or more keys to get
        missing: raise or omit, optional
            raise (default)
                If any key is missing, raise KeyError.
            omit
                If a key is missing, return a dict with less keys than those requested.

        Notes
        -----
        All keys may be present when you call ``async_get``, but ``__delitem__`` may be
        called on one of them before the actual data is fetched. ``__setitem__`` also
        internally calls ``__delitem__`` in a non-atomic way, so you may get
        ``KeyError`` when updating a value too.
        """
        # This block avoids spawning a thread if keys are missing from both fast and
        # slow. It is otherwise just a performance optimization.
        if missing == "omit":
            keys = [key for key in keys if key in self]
        elif missing == "raise":
            for key in keys:
                if key not in self:
                    raise KeyError(key)
        else:
            raise ValueError(f"missing: expected raise or omit; got {missing}")
        # End performance optimization

        try:
            # Do not pull keys towards the top of the LRU unless they are all available.
            # This matters when there is a very long queue of async_get futures.
            d = self.fast.get_all_or_nothing(keys)
        except KeyError:
            pass
        else:
            f: asyncio.Future[dict[KT, VT]] = asyncio.Future()
            f.set_result(d)
            return f

        def _async_get() -> dict[KT, VT]:
            d = {}
            for k in keys:
                if self.fast.closed:
                    raise asyncio.CancelledError()
                try:
                    # This can cause keys to be restored and older keys to be evicted
                    d[k] = self[k]
                except KeyError:
                    # Race condition: key was there when async_get was called, but got
                    # deleted afterwards.
                    if missing == "raise":
                        raise
            return d

        return self._offload(_async_get)

    def __setitem__(self, key: KT, value: VT) -> None:
        """Immediately set a key in fast. If this causes the total weight to exceed n,
        asynchronously start moving keys from fast to slow in a worker thread.
        """
        self.set_noevict(key, value)
        self.async_evict_until_below_target()

    @locked
    def async_evict_until_below_target(self, n: float | None = None) -> None:
        """If the total weight exceeds n, asynchronously start moving keys from fast to
        slow in a worker thread.
        """
        if n is None:
            n = self.n
        n = max(0.0, n)
        weight = self.fast.total_weight
        if self.evicting is not None:
            weight = min(weight, self.evicting)
        if weight <= n:
            return

        self.evicting = n

        def done(_: asyncio.Future) -> None:
            self.evicting = None

        # Note: this can get cancelled by LRU.close(), which in turn is
        # triggered by Buffer.close()
        future = self._offload(self.evict_until_below_target, n)
        future.add_done_callback(done)
