from __future__ import absolute_import, division, print_function

from zict import LRU
from . import utils_test


def test_simple():
    d = dict()
    lru = LRU(2, d)

    lru['x'] = 1
    lru['y'] = 2

    assert lru['x'] == 1
    assert lru['y'] == 2
    assert d == {'x': 1, 'y': 2}

    lru['z'] = 3
    assert len(d) == 2
    assert len(lru) == 2
    assert 'z' in d
    assert 'z' in lru
    assert 'x' not in d
    assert 'y' in d

    del lru['y']
    assert 'y' not in d
    assert 'y' not in lru

    lru['a'] = 5
    assert set(lru.keys()) == set(['z', 'a'])


def test_str():
    d = dict()
    lru = LRU(2, d)

    lru['x'] = 1
    lru['y'] = 2

    assert str(lru.total_weight) in str(lru)
    assert str(lru.total_weight) in repr(lru)
    assert str(lru.n) in str(lru)
    assert str(lru.n) in repr(lru)
    assert 'dict' in str(lru)
    assert 'dict' in repr(lru)


def test_mapping():
    """
    Test mapping interface for LRU().
    """
    d = {}
    # 100 is more than the max length when running check_mapping()
    lru = LRU(100, d)
    utils_test.check_mapping(lru)
    utils_test.check_closing(lru)


def test_overwrite():
    d = dict()
    lru = LRU(2, d)

    lru['x'] = 1
    lru['y'] = 2
    lru['y'] = 3

    assert set(lru) == {'x', 'y'}

    lru.update({'y': 4})

    assert set(lru) == {'x', 'y'}


def test_callbacks():
    count = [0]
    def cb(k, v):
        count[0] += 1

    L = list()
    d = dict()
    lru = LRU(2, d, on_evict=[lambda k, v: L.append((k, v)), cb])

    lru['x'] = 1
    lru['y'] = 2
    lru['z'] = 3

    assert L == [('x', 1)]
    assert count[0] == len(L)


def test_weight():
    d = dict()
    weight = lambda k, v: v
    lru = LRU(10, d, weight=weight)

    lru['x'] = 5
    assert lru.total_weight == 5

    lru['y'] = 4
    assert lru.total_weight == 9

    lru['z'] = 3
    assert d == {'y': 4, 'z': 3}
    assert lru.total_weight == 7

    del lru['z']
    assert lru.total_weight == 4

    lru['a'] = 10000
    assert 'a' not in lru
    assert d == {'y': 4}
