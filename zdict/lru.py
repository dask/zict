from heapdict import heapdict
from collections import MutableMapping


def do_nothing(k, v):
    pass


class LRU(MutableMapping):
    """ Evict Least Recently Used Elements

    Parameters
    ----------
    n: int
        Number of elements to keep
    d: MutableMapping
        Dictionary in which to hold elements
    on_evict: callable
        Function:: k, v -> action to call on key value pairs prior to eviction
    weight: callable

    Examples
    --------

    >>> lru = LRU(2, dict(), on_evict=lambda k, v: print("Lost", k, v))
    >>> lru['x'] = 1
    >>> lru['y'] = 2
    >>> lru['z'] = 3
    Lost x 1
    """
    def __init__(self, n, d, on_evict=do_nothing):
        self.d = d
        self.n = n
        self.heap = heapdict()
        self.i = 0
        self.on_evict = on_evict

    def __getitem__(self, key):
        result = self.d[key]
        self.i += 1
        self.heap[key] = self.i
        return result

    def __setitem__(self, key, value):
        self.d[key] = value
        self.i += 1
        self.heap[key] = self.i
        if len(self.heap) > self.n:
            k, v = self.heap.popitem()
            self.on_evict(k, v)
            del self.d[k]

    def __delitem__(self, key):
        del self.d[key]
        del self.heap[key]

    def keys(self):
        return self.d.keys()

    def values(self):
        return self.d.values()

    def items(self):
        return self.d.items()

    def __len__(self):
        return len(self.d)

    def __iter__(self):
        return iter(self.d)

    def __contains__(self, key):
        if key in self.d:
            self.i += 1
            self.heap[key] = self.i
            return True
        else:
            return False

    def __str__(self):
        return 'LRU: %s' % str(self.d)

    def __repr__(self):
        return 'LRU: %s' % repr(self.d)

    def flush(self):
        self.d.flush()
