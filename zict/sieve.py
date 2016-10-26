from __future__ import absolute_import, division, print_function

from collections import MutableMapping
from itertools import chain
import sys


class Sieve(MutableMapping):
    """ Buffer one dictionary on top of another

    This creates a MutableMapping by combining two MutableMappings, one that
    feeds into the other when it overflows, based on an LRU mechanism.  When
    the first evicts elements these get placed into the second.  When an item
    is retrieved from the second it is placed back into the first.

    Parameters
    ----------
    fast: MutableMapping
    slow: MutableMapping

    Examples
    --------
    >>> fast = dict()
    >>> slow = Func(dumps, loads, File('storage/'))  # doctest: +SKIP
    >>> def weight(k, v):
    ...     return sys.getsizeof(v)
    >>> buff = Buffer(fast, slow, 1e8, weight=weight)  # doctest: +SKIP

    See Also
    --------
    LRU
    """
    def __init__(self, small, large, threshold):
        self.small = small
        self.large = large
        self.threshold = threshold

    def __getitem__(self, key):
        if key in self.small:
            return self.small[key]
        else:
            return self.large[key]

    def __setitem__(self, key, value):
        if sys.getsizeof(value) < self.threshold:
            self.small[key] = value
        else:
            self.large[key] = value

    def __delitem__(self, key):
        if key in self.small:
            del self.small[key]
        else:
            del self.large[key]

    def keys(self):
        return chain(self.small.keys(), self.large.keys())

    def values(self):
        return chain(self.small.values(), self.large.values())

    def items(self):
        return chain(self.small.items(), self.large.items())

    def __len__(self):
        return len(self.small) + len(self.large)

    def __iter__(self):
        return chain(iter(self.small), iter(self.large))

    def __contains__(self, key):
        return key in self.small or key in self.large

    def __str__(self):
        return 'Sieve<%s, %s>' % (str(self.small), str(self.large))

    __repr__ = __str__

    def flush(self):
        self.small.flush()
        self.large.flush()
