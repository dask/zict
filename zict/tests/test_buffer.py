
from zict import Buffer
from . import utils_test


def test_simple():
    a = dict()
    b = dict()
    buff = Buffer(a, b, n=10, weight=lambda k, v: v, keep_slow=False)

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

    buff['y'] = 1
    assert a == {'x': 1, 'y': 1, 'z': 8}
    assert buff.fast.total_weight == 10
    assert b == {}

    del buff['z']
    assert a == {'x': 1, 'y': 1}
    assert buff.fast.total_weight == 2
    assert b == {}

    del buff['y']
    assert a == {'x': 1}
    assert buff.fast.total_weight == 1
    assert b == {}

    assert 'y' not in buff

    buff['a'] = 5
    assert set(buff) == set(buff.keys()) == {'a', 'x'}

    fast_keys = set(buff.fast)
    slow_keys = set(buff.slow)
    assert not (fast_keys & slow_keys)
    assert fast_keys | slow_keys == set(buff)

    # Overweight element stays in slow mapping
    buff['b'] = 1000
    assert 'b' in buff.slow
    assert set(buff.fast) == fast_keys
    assert set(buff.slow) == {'b'} | slow_keys
    assert 'b' in buff
    assert buff['b'] == 1000


def test_setitem_avoid_fast_slow_duplicate():

    a = dict()
    b = dict()
    buff = Buffer(a, b, n=10, weight=lambda k, v: v)
    for first, second in [
        (1, 12),
        (12, 1)
    ]:
        buff['a'] = first
        assert buff['a'] == first
        buff['a'] = second
        assert buff['a'] == second

        fast_keys = set(buff.fast)
        slow_keys = set(buff.slow)
        assert not (fast_keys & slow_keys)
        assert fast_keys | slow_keys == set(buff)

        del buff['a']
        assert 'a' not in buff
        assert 'a' not in a
        assert 'a' not in b


def test_mapping():
    """
    Test mapping interface for Buffer().
    """
    a = {}
    b = {}
    buff = Buffer(a, b, n=2)
    utils_test.check_mapping(buff)
    utils_test.check_closing(buff)


def test_callbacks():
    f2s = []
    def f2s_cb(k, v):
        f2s.append(k)

    s2f = []
    def s2f_cb(k, v):
        s2f.append(k)

    a = dict()
    b = dict()
    buff = Buffer(a, b, n=10, weight=lambda k, v: v,
                  fast_to_slow_callbacks=f2s_cb,
                  slow_to_fast_callbacks=s2f_cb)

    buff['x'] = 1
    buff['y'] = 2

    assert buff['x'] == 1
    assert buff['y'] == 2
    assert not f2s
    assert not s2f

    buff['z'] = 8

    assert f2s == ['x']
    assert s2f == []
    buff['z']

    assert f2s == ['x']
    assert s2f == []

    buff['x']
    assert f2s == ['x', 'y']
    assert s2f == ['x']


def test_keep_slow():
    a = {}
    b = {}
    buff = Buffer(a, b, n=2, keep_slow=True)

    buff['x'] = 1
    buff['y'] = 2
    buff['z'] = 3

    assert a == {'y': 2, 'z': 3}
    assert b == {'x': 1}

    buff['x']

    assert a == {'x': 1, 'z': 3}
    assert b == {'x': 1, 'y': 2}

    del buff['x']

    assert a == {'z': 3}
    assert b == {'y': 2}

    buff['x'] = 1
    buff['w'] = 4

    assert a == {'x': 1, 'w': 4}
    assert b == {'y': 2, 'z': 3}

    buff['x'] = 10

    assert a == {'x': 10, 'w': 4}
    assert b == {'y': 2, 'z': 3}

    buff['y']

    assert a == {'x': 10, 'y': 2}
    assert b == {'y': 2, 'z': 3, 'w': 4}

    buff['y'] = 12

    assert a == {'x': 10, 'y': 12}
    assert b == {'z': 3, 'w': 4}
