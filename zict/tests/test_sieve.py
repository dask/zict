from __future__ import absolute_import, division, print_function

import sys

from zict import Sieve
from . import utils_test


def test_simple():
    a = {}
    b = {}
    z = Sieve(a, b, threshold=sys.getsizeof(b'123'))

    z['x'] = b'12'
    z['y'] = b'3456'

    assert z['x'] == b'12'
    assert z['y'] == b'3456'
    assert a == {'x': b'12'}
    assert b == {'y': b'3456'}

    z['z'] = b'789'
    assert a == {'x': b'12'}
    assert b == {'y': b'3456', 'z': b'789'}

    assert 'x' in z
    assert 'y' in z
    assert 'missing' not in z

    # Changing existing keys can move values from large to small or vice-versa.
    z['x'] = b'121212'
    z['y'] = b'34'
    assert 'x' in z
    assert 'y' in z
    assert a == {'y': b'34'}
    assert b == {'x': b'121212', 'z': b'789'}
    assert sorted(z.items()) == [('x', b'121212'),
                                 ('y', b'34'),
                                 ('z', b'789'),
                                 ]

    del z['y']
    assert a == {}
    assert b == {'x': b'121212', 'z': b'789'}

    del z['x']
    assert a == {}
    assert b == {'z': b'789'}


def test_mapping():
    """
    Test mapping interface for Sieve().
    """
    a = {}
    b = {}
    z = Sieve(a, b, threshold=3)
    utils_test.check_mapping(z)
