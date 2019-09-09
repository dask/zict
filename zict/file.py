from __future__ import absolute_import, division, print_function

import errno
import os
import pickle
import uuid

try:
    from urllib.parse import quote, unquote
except ImportError:
    from urllib import quote, unquote

from .common import ZictBase


def _get_uniq_name():
    uniq_str = str(uuid.uuid4())
    uniq_str = 'f_' + uniq_str.replace('-', '')
    return uniq_str


def _safe_key(key):
    """
    Escape key so as to be usable on all filesystems.
    """
    # Even directory separators are unsafe.
    return quote(key, safe='')


def _unsafe_key(key):
    """
    Undo the escaping done by _safe_key().
    """
    return unquote(key)


class _WriteAheadLog:
    """
    Write ahead log implementation for storing key value pairs to the file
    Appends to the file key value and action as pickled tuples and reply them back when requested
    :param log_path: file path to the log file
    """

    def __init__(self, log_path):
        self.log_path = log_path

    def write_key_value_and_action(self, key, value, action):
        """
        Record key value pair and action to the log file
        :param key: key to record
        :param value: value to record
        :param action: action assigned to key value pair
        :return: None
        """
        with open(self.log_path, 'ab') as f:
            b = pickle.dumps((key, value, action))
            bytes_len = len(b)
            f.write(bytes_len.to_bytes(4, 'big'))
            f.write(b)

    def get_all_pairs(self):
        """
        Get all tuples with key value and action from the log
        :return: list of tuples with key value and action
        """
        if not os.path.exists(self.log_path):
            return []
        result = []
        with open(self.log_path, 'rb') as f:
            while True:
                bytes_len_b = f.read(4)
                if not bytes_len_b:
                    break
                bytes_len = int.from_bytes(bytes_len_b, 'big')
                payload = f.read(bytes_len)
                key_val_pair = pickle.loads(payload)
                result.append(key_val_pair)

        return result


_WAL_NAME = 'f_a3d4e639575448efa18ed45bdbf5882a.bin'


#After that create a package and create a merge request
class File(ZictBase):
    """ Mutable Mapping interface to a directory

    Keys must be strings, values must be bytes

    Note this shouldn't be used for interprocess persistence, as keys
    are cached in memory.k

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
    def __init__(self, directory, mode='a'):
        self.directory = directory
        self.mode = mode
        self._keys = set()
        self._long_key_to_val_map = {}
        self._wal = _WriteAheadLog(os.path.join(directory, _WAL_NAME))
        if not os.path.exists(self.directory):
            os.mkdir(self.directory)
        else:
            for n in os.listdir(self.directory):
                if n != _WAL_NAME:
                    self._keys.add(_unsafe_key(n))

        for key, value, action in self._wal.get_all_pairs():
            if action == 'a':
                self._long_key_to_val_map[key] = value
            else:
                del self._long_key_to_val_map[key]

        for k in self._long_key_to_val_map.keys():
            self._keys.add(k)

    def __str__(self):
        return '<File: %s, mode="%s", %d elements>' % (self.directory, self.mode, len(self))

    __repr__ = __str__

    def __getitem__(self, key):
        if key not in self._keys:
            raise KeyError(key)
        if key in self._long_key_to_val_map:
            key = self._long_key_to_val_map[key]
        with open(os.path.join(self.directory, _safe_key(key)), 'rb') as f:
            return f.read()

    def __setitem__(self, key, value):
        original_key = key
        if len(_safe_key(key)) > 250:
            short_key = _get_uniq_name()
            self._long_key_to_val_map[key] = short_key
            self._wal.write_key_value_and_action(key, short_key, 'a')
            key = short_key

        with open(os.path.join(self.directory, _safe_key(key)), 'wb') as f:
            if isinstance(value, (tuple, list)):
                for v in value:
                    f.write(v)
            else:
                f.write(value)
        self._keys.add(original_key)

    def __contains__(self, key):
        return key in self._keys

    def keys(self):
        return iter(self._keys)

    __iter__ = keys

    def __delitem__(self, key):
        if key not in self._keys:
            raise KeyError(key)
        original_key = key
        if len(_safe_key(key)) > 250:
            short_key = self._long_key_to_val_map[key]
            self._wal.write_key_value_and_action(key, short_key, 'd')
            key = short_key

        os.remove(os.path.join(self.directory, _safe_key(key)))
        self._keys.remove(original_key)

    def __len__(self):
        return len(self._keys)
