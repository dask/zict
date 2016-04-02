from zict import Func
from collections import MutableMapping

def inc(x):
    return x + 1

def dec(x):
    return x - 1

def test_simple():
    d = dict()
    f = Func(inc, dec, d)
    assert isinstance(f, MutableMapping)
    f['x'] = 10
    assert f['x'] == 10
    assert d['x'] == 11

    assert 'x' in f
    assert list(f) == ['x']
    assert list(f.values()) == [10]
    assert list(f.items()) == [('x', 10)]

    assert all(s in str(f) for s in ['inc', 'dec', 'x'])
    assert all(s in repr(f) for s in ['inc', 'dec', 'x'])

    del f['x']
    assert 'x' not in d
