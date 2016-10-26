from __future__ import absolute_import, division, print_function

from zict import Buffer
from . import utils_test


def test_simple():
    a = dict()
    b = dict()
    buff = Buffer(a, b, n=10, weight=lambda k, v: v)

    buff['x'] = 1
    buff['y'] = 2

    assert buff['x'] == 1
    assert buff['y'] == 2
    assert a == {'x': 1, 'y': 2}
    assert buff.fast.total_weight == 3

    buff['z'] = 8
    assert a == {'y': 2, 'z': 8}
    assert b == {'x': 1}

    assert buff['x'] == 1
    assert a == {'x': 1, 'z': 8}
    assert b == {'y': 2}

    assert 'x' in buff
    assert 'y' in buff
    assert 'missing' not in buff

    del buff['z']
    assert a == {'x': 1}
    assert b == {'y': 2}

    del buff['y']
    assert a == {'x': 1}
    assert b == {}

    assert 'y' not in buff

    buff['a'] = 5
    assert set(buff) == set(buff.keys()) == {'a', 'x'}

    fast_keys = set(buff.fast)
    buff['b'] = 1000
    assert 'b' in buff.slow
    assert set(buff.fast) == fast_keys


def test_mapping():
    """
    Test mapping interface for Buffer().
    """
    a = {}
    b = {}
    buff = Buffer(a, b, n=2)
    utils_test.check_mapping(buff)
