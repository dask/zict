from collections import MutableMapping

from toolz import concat

from .lru import LRU


class Buffer(MutableMapping):
    """ Buffer one dictionary on top of another

    This creates a MutableMapping by combining two others.  The first mutable
    mapping must evict elements at a certain point and accept an ``on_evict``
    callback.  The ``LRU`` mapping within this library satisfies this.

    Parameters
    ----------
    fast: MutableMapping
    slow: MutableMapping

    Examples
    --------
    >>> fast = LRU(2, dict(), on_evict=lambda k, v: print("Lost", k, v))
    >>> slow = dict()  # usually some on-disk structure
    >>> buff = Buffer(fast, slow)
    """
    def __init__(self, fast, slow, n, weight=lambda k, v: 1):
        self.fast = LRU(n, fast, weight=weight, on_evict=self.fast_to_slow)
        self.slow = slow

    def fast_to_slow(self, key, value):
        self.slow[key] = value

    def slow_to_fast(self, key):
        self.fast[key] = self.slow.pop(key)

    def __getitem__(self, key):
        if key in self.fast:
            return self.fast[key]
        elif key in self.slow:
            self.slow_to_fast(key)
            return self.fast[key]
        else:
            raise KeyError(key)

    def __setitem__(self, key, value):
        self.fast[key] = value

    def __delitem__(self, key):
        if key in self.fast:
            del self.fast[key]
        elif key in self.slow:
            del self.slow[key]
        else:
            raise KeyError(key)

    def keys(self):
        return concat([self.fast.keys(), self.slow.keys()])

    def values(self):
        return concat([self.fast.values(), self.slow.values()])

    def items(self):
        return concat([self.fast.items(), self.slow.items()])

    def __len__(self):
        return len(self.fast) + len(self.slow)

    def __iter__(self):
        return concat([iter(self.fast), iter(self.slow)])

    def __contains__(self, key):
        return key in self.fast or key in self.slow

    def __str__(self):
        return 'Buffer<%s, %s>' % (str(self.fast), str(self.slow))

    __repr__ = __str__

    def flush(self):
        self.fast.flush()
        self.slow.flush()
