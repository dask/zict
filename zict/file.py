import mmap
import os
from urllib.parse import quote, unquote

from .common import ZictBase


def _safe_key(key):
    """
    Escape key so as to be usable on all filesystems.
    """
    # Even directory separators are unsafe.
    return quote(key, safe="")


def _unsafe_key(key):
    """
    Undo the escaping done by _safe_key().
    """
    return unquote(key)


class File(ZictBase):
    """Mutable Mapping interface to a directory

    Keys must be strings, values must be bytes

    Note this shouldn't be used for interprocess persistence, as keys
    are cached in memory.

    Parameters
    ----------
    directory: string
    mode: string, ('r', 'w', 'a'), defaults to 'a'

    Examples
    --------
    >>> z = File('myfile')  # doctest: +SKIP
    >>> z['x'] = b'123'  # doctest: +SKIP
    >>> z['x']  # doctest: +SKIP
    b'123'

    Also supports writing lists of bytes objects

    >>> z['y'] = [b'123', b'4567']  # doctest: +SKIP
    >>> z['y']  # doctest: +SKIP
    b'1234567'

    Or anything that can be used with file.write, like a memoryview

    >>> z['data'] = np.ones(5).data  # doctest: +SKIP
    """

    def __init__(self, directory, mode="a", memmap=False):
        self.directory = directory
        self.mode = mode
        self.memmap = memmap
        self._keys = set()
        if not os.path.exists(self.directory):
            os.makedirs(self.directory, exist_ok=True)
        else:
            for n in os.listdir(self.directory):
                self._keys.add(_unsafe_key(n))

    def __str__(self):
        return '<File: %s, mode="%s", %d elements>' % (
            self.directory,
            self.mode,
            len(self),
        )

    __repr__ = __str__

    def __getitem__(self, key):
        if key not in self._keys:
            raise KeyError(key)
        fn = os.path.join(self.directory, _safe_key(key))
        with open(fn, "rb") as fh:
            if self.memmap:
                return memoryview(mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ))
            else:
                return fh.read()

    def __setitem__(self, key, value):
        fn = os.path.join(self.directory, _safe_key(key))
        with open(fn, "wb") as fh:
            if isinstance(value, (tuple, list)):
                fh.writelines(value)
            else:
                fh.write(value)
        self._keys.add(key)

    def __contains__(self, key):
        return key in self._keys

    def keys(self):
        return iter(self._keys)

    __iter__ = keys

    def __delitem__(self, key):
        if key not in self._keys:
            raise KeyError(key)
        os.remove(os.path.join(self.directory, _safe_key(key)))
        self._keys.remove(key)

    def __len__(self):
        return len(self._keys)
