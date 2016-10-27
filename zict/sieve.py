from __future__ import absolute_import, division, print_function

from collections import MutableMapping, defaultdict
from itertools import chain
import sys


class Sieve(MutableMapping):
    """ Store values in different mappings based on a selector's
    output.

    This creates a MutableMapping combining several underlying
    MutableMappings for storage.  Items are dispatcher based on
    a selector function provided by the user.

    Parameters
    ----------
    mappings: MutableMapping
    selector: callable (key, value) -> mapping key

    Examples
    --------
    >>> small = {}
    >>> large = DataBase()  # doctest: +SKIP
    >>> mappings = {True: small, False: large}
    >>> def selector(key, value):
            return sys.getsizeof(value) > 10000
    >>> d = Sieve(mappings, selector)

    See Also
    --------
    Buffer
    """
    def __init__(self, mappings, selector):
        self.mappings = mappings
        self.selector = selector
        self.key_to_mapping = {}

    def __getitem__(self, key):
        return self.key_to_mapping[key][key]

    def __setitem__(self, key, value):
        old_mapping = self.key_to_mapping.get(key)
        mapping = self.mappings[self.selector(key, value)]
        if old_mapping is not None and old_mapping is not mapping:
            del old_mapping[key]
        mapping[key] = value
        self.key_to_mapping[key] = mapping

    def __delitem__(self, key):
        del self.key_to_mapping.pop(key)[key]

    def keys(self):
        return chain.from_iterable(self.mappings.values())

    def values(self):
        return chain.from_iterable(m.values() for m in self.mappings.values())

    def items(self):
        return chain.from_iterable(m.items() for m in self.mappings.values())

    def __len__(self):
        return sum(map(len, self.mappings.values()))

    __iter__ = keys

    def __contains__(self, key):
        return key in self.key_to_mapping

    def __str__(self):
        return 'Sieve<%s>' % (str(self.mappings),)

    __repr__ = __str__

    def flush(self):
        for m in self.mappings.values():
            m.flush()
