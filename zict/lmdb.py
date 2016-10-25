from __future__ import absolute_import, division, print_function

from collections import MutableMapping
import os
import sys


if sys.version_info >= (3,):
    def _encode_key(key):
        return key.encode('latin1')

    def _decode_key(key):
        return key.decode('latin1')

else:
    def _encode_key(key):
        return key

    def _decode_key(key):
        return key


class LMDB(MutableMapping):
    """ Mutable Mapping interface to a LMDB database.

    Keys must be strings, values must be bytes

    Parameters
    ----------
    directory: string

    Examples
    --------
    >>> z = LMDB('/tmp/somedir/')  # doctest: +SKIP
    >>> z['x'] = b'123'  # doctest: +SKIP
    >>> z['x']  # doctest: +SKIP
    b'123'
    """
    def __init__(self, directory):
        import lmdb
        # map_size is the maximum database size but shouldn't fill up the
        # virtual address space
        map_size = (1 << 40 if sys.maxsize >= 2**32 else 1 << 28)
        # writemap requires sparse file support otherwise the whole
        # `map_size` may be reserved up front on disk
        writemap = sys.platform.startswith('linux')
        self.db = lmdb.open(directory,
                            subdir=True,
                            map_size=map_size,
                            sync=False,
                            writemap=writemap,
                            )

    def __getitem__(self, key):
        with self.db.begin() as txn:
            value = txn.get(_encode_key(key))
        if value is None:
            raise KeyError(key)
        return value

    def __setitem__(self, key, value):
        with self.db.begin(write=True) as txn:
            txn.put(_encode_key(key), value)

    def __contains__(self, key):
        with self.db.begin() as txn:
            return txn.cursor().set_key(_encode_key(key))

    def items(self):
        cursor = self.db.begin().cursor()
        return ((_decode_key(k), v)
                for k, v in cursor.iternext(keys=True, values=True))

    def keys(self):
        cursor = self.db.begin().cursor()
        return (_decode_key(k)
                for k in cursor.iternext(keys=True, values=False))

    def values(self):
        cursor = self.db.begin().cursor()
        return cursor.iternext(keys=False, values=True)

    def __iter__(self):
        return self.keys()

    def __delitem__(self, key):
        with self.db.begin(write=True) as txn:
            txn.delete(_encode_key(key))

    def __len__(self):
        return self.db.stat()['entries']

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass
