from collections import MutableMapping
import os


class File(MutableMapping):
    """ Mutable Mapping interface to a directory

    Keys must be strings, values must be bytes

    Parameters
    ----------
    directory: string
    mode: string, ('r', 'w', 'a'), defaults to 'a'

    Examples
    --------
    >>> z = File('myfile')
    >>> z['x'] = b'123'
    >>> z['x']
    b'123'
    """
    def __init__(self, directory, mode='a'):
        self.directory = directory
        self.mode = mode
        if not os.path.exists(self.directory):
            os.mkdir(self.directory)

    def __getitem__(self, key):
        try:
            with open(os.path.join(self.directory, key), 'rb') as f:
                result = f.read()
        except (IOError, OSError):
            raise KeyError(key)
        return result

    def __setitem__(self, key, value):
        if not isinstance(value, bytes):
            raise TypeError("Value must be of type bytes")
        with open(os.path.join(self.directory, key), 'wb') as f:
            f.write(value)

    def keys(self):
        return iter(os.listdir(self.directory))

    def __iter__(self):
        return self.keys()

    def __delitem__(self, key):
        os.remove(os.path.join(self.directory, key))

    def __len__(self):
        return sum(1 for _ in self.keys())

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass
