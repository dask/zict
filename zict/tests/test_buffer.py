from zict import Buffer


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
