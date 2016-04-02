from zict import LRU

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

    assert 'a' in str(lru) and '5' in str(lru)
    assert 'a' in repr(lru) and '5' in repr(lru)


def test_callbacks():
    L = list()
    d = dict()
    lru = LRU(2, d, on_evict=lambda k, v: L.append((k, v)))

    lru['x'] = 1
    lru['y'] = 2
    lru['z'] = 3

    assert L == [('x', 1)]
