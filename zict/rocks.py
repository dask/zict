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
    >>> z['x'] = b'123'  # doctest: +SKIP
    >>> z['x']  # doctest: +SKIP
    b'123'
    """
    def __init__(self, filename, opts=None, read_only=False, string_keys=True):
        opts = opts or rocksdb.Options(create_if_missing=True)
        self.filename = filename
        self.db = rocksdb.DB(filename, opts, read_only=read_only)
        self.string_keys = string_keys

    def __getitem__(self, key):
        result = self.db.get(to_key(key))
        if result is None:
            raise KeyError(key)
        else:
            return result

    def __setitem__(self, key, value):
        self.db.put(to_key(key), to_bytes(value))

    def keys(self):
        it = self.db.iterkeys()
        it.seek_to_first()
        if self.string_keys:
            return (b.decode() for b in it)
        else:
            return it

    def values(self):
        it = self.db.itervalues()
        it.seek_to_first()
        return it

    def items(self):
        it = self.db.iteritems()
        it.seek_to_first()
        if self.string_keys:
            return ((k.decode(), v) for k, v in it)
        else:
            return it

    def __len__(self):
        return sum(1 for key in self)

    def __contains__(self, key):
        return self[key] is not None

    def __iter__(self):
        return self.keys()

    def __delitem__(self, key):
        self.db.delete(to_key(key))


def to_bytes(x):
    if isinstance(x, bytearray):
        return bytes(x)
    return x


def to_key(key):
    try:
        return key.encode()
    except AttributeError:
        return key
