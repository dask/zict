from __future__ import absolute_import, division, print_function

from zict import Func
from . import utils_test


def inc(x):
    return x + 1

def dec(x):
    return x - 1

def rotl(x):
    return x[1:] + x[:1]

def rotr(x):
    return x[-1:] + x[:-1]


def test_simple():
    d = dict()
    f = Func(inc, dec, d)
    f['x'] = 10
    assert f['x'] == 10
    assert d['x'] == 11

    assert 'x' in f
    assert list(f) == ['x']
    assert list(f.values()) == [10]
    assert list(f.items()) == [('x', 10)]

    assert all(s in str(f) for s in ['inc', 'dec', 'x', 'Func'])
    assert all(s in repr(f) for s in ['inc', 'dec', 'x', 'Func'])

    del f['x']
    assert 'x' not in d


def test_mapping():
    """
    Test mapping interface for Func().
    """
    d = {}
    z = Func(rotl, rotr, d)
    utils_test.check_mapping(z)
    utils_test.check_closing(z)
