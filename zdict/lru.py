from heapdict import heapdict
from collections import MutableMapping


class LRU(MutableMapping):
    def __init__(self, n, d):
        self.d = d
        self.n = n
        self.heap = heapdict()
        self.i = 0

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
            k, _ = self.heap.popitem()
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
