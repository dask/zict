from __future__ import absolute_import, division, print_function

from collections import MutableMapping
from itertools import chain
import sys


class Sieve(MutableMapping):
    """ Store values in different mappings based on their sizes.

    This creates a MutableMapping by combining two MutableMappings,
    one that stores values strictly smaller (in bytes) than a given
    threshold, the other storing the other values.

    Only the size of values, not keys, is considered, since keys are
    likely kept in memory for bookkeeping purposes anyway.

    Parameters
    ----------
    small: MutableMapping
    large: MutableMapping
    threshold: int
        The number of bytes below which values are stored in the *small*
        mapping.

    Examples
    --------
    >>> small = {}
    >>> large = Func(dumps, loads, File('storage/'))  # doctest: +SKIP
    >>> store = Sieve(small, large, threshold=1024 ** 2)  # doctest: +SKIP

    See Also
    --------
    Buffer
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
            if key in self.large:
                del self.large[key]
            self.small[key] = value
        else:
            if key in self.small:
                del self.small[key]
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
