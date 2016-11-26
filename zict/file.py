from __future__ import absolute_import, division, print_function

import errno
import os
try:
    from urllib.parse import quote
except ImportError:
    from urllib import quote

from .common import ZictBase


def _safe_key(key):
    """
    Escape key so as to be usable on all filesystems.
    """
    # Even directory separators are unsafe.
    return quote(key, safe='')


class File(ZictBase):
    """ Mutable Mapping interface to a directory

    Keys must be strings, values must be bytes

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
    """
    def __init__(self, directory, mode='a'):
        self.directory = directory
        self.mode = mode
        if not os.path.exists(self.directory):
            os.mkdir(self.directory)

    def __str__(self):
        return '<File: %s, mode="%s">' % (self.directory, self.mode)

    __repr__ = __str__

    def __getitem__(self, key):
        try:
            with open(os.path.join(self.directory, _safe_key(key)), 'rb') as f:
                result = f.read()
        except EnvironmentError as e:
            if e.args[0] != errno.ENOENT:
                raise
            raise KeyError(key)
        return result

    def __setitem__(self, key, value):
        with open(os.path.join(self.directory, _safe_key(key)), 'wb') as f:
            f.write(value)

    def __contains__(self, key):
        return isinstance(key, str) and os.path.exists(os.path.join(self.directory, _safe_key(key)))

    def keys(self):
        return iter(os.listdir(self.directory))

    def __iter__(self):
        return self.keys()

    def __delitem__(self, key):
        try:
            os.remove(os.path.join(self.directory, _safe_key(key)))
        except EnvironmentError as e:
            if e.args[0] != errno.ENOENT:
                raise
            raise KeyError(key)

    def __len__(self):
        return sum(1 for _ in self.keys())
