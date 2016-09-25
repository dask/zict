from __future__ import absolute_import, division, print_function

from collections import MutableMapping
import sys
import rocksdb


class RocksDB(MutableMapping):
    """ Mutable Mapping interface to a RocksDB file

    Keys must be bytestrings, values must be bytestrings

    Parameters
    ----------
    filename: string
    opts: ``rocksdb.Options``
    read_only: bool

    Examples
    --------
    >>> z = RocksDB('myfile.db')  # doctest: +SKIP
    >>> z[b'x'] = b'123'  # doctest: +SKIP
    >>> z[b'x']  # doctest: +SKIP
    b'123'
    """
    def __init__(self, filename, opts=None, read_only=False):
        opts = opts or rocksdb.Options(create_if_missing=True)
        self.filename = filename
        self.db = rocksdb.DB(filename, opts, read_only=read_only)

    def __getitem__(self, key):
        result = self.db.get(key)
        if result is None:
            raise KeyError(key)
        else:
            return result

    def __setitem__(self, key, value):
        self.db.put(key, to_bytes(value))

    def keys(self):
        it = self.db.iterkeys()
        it.seek_to_first()
        return it

    def values(self):
        it = self.db.itervalues()
        it.seek_to_first()
        return it

    def items(self):
        it = self.db.iteritems()
        it.seek_to_first()
        return it

    def __len__(self):
        return sum(1 for key in self)

    def __iter__(self):
        return self.keys()

    def __delitem__(self, key):
        raise self.db.delete(key)


def to_bytes(x):
    if isinstance(x, bytearray):
        return bytes(x)
    return x
