from __future__ import absolute_import, division, print_function

from collections import MutableMapping
import random
import string

import pytest


def generate_random_strings(n, min_len, max_len):
    r = random.Random(42)
    l = []
    chars = string.ascii_lowercase + string.digits

    for i in range(n):
        nchars = r.randint(min_len, max_len)
        s = ''.join(r.choice(chars) for _ in range(nchars))
        l.append(s)

    return l


def to_bytestring(s):
    if isinstance(s, bytes):
        return s
    else:
        return s.encode('latin1')


def stress_test_mapping_updates(z):
    # Certain mappings shuffle between several underlying stores
    # during updates.  This stress tests the internal mapping
    # consistency.
    r = random.Random(42)

    keys = list(string.ascii_lowercase)
    values = [to_bytestring(s)
              for s in generate_random_strings(len(keys), 1, 10)]

    z.clear()
    assert len(z) == 0

    for k, v in zip(keys, values):
        z[k] = v
    assert len(z) == len(keys)
    assert sorted(z) == sorted(keys)
    assert sorted(z.items()) == sorted(zip(keys, values))

    for i in range(3):
        r.shuffle(keys)
        r.shuffle(values)
        for k, v in zip(keys, values):
            z[k] = v
        assert len(z) == len(keys)
        assert sorted(z) == sorted(keys)
        assert sorted(z.items()) == sorted(zip(keys, values))

        r.shuffle(keys)
        r.shuffle(values)
        z.update(zip(keys, values))
        assert len(z) == len(keys)
        assert sorted(z) == sorted(keys)
        assert sorted(z.items()) == sorted(zip(keys, values))


def check_mapping(z):
    assert isinstance(z, MutableMapping)
    assert not z

    assert list(z) == list(z.keys()) == []
    assert list(z.values()) == []
    assert list(z.items()) == []
    assert len(z) == 0

    z['abc'] = b'456'
    z['xyz'] = b'12'
    assert len(z) == 2
    assert z['abc'] == b'456'

    items = list(z.items())
    assert set(items) == {('abc', b'456'), ('xyz', b'12')}
    assert list(z.keys()) == [k for k, v in items]
    assert list(z.values()) == [v for k, v in items]
    assert list(z) == [k for k, v in items]

    assert 'abc' in z
    assert 'xyz' in z
    assert 'def' not in z

    with pytest.raises(KeyError):
        z['def']

    z.update(xyz=b'707', uvw=b'000')
    assert set(z.items()) == {('abc', b'456'), ('xyz', b'707'), ('uvw', b'000')}
    z.update([('xyz', b'654'), ('uvw', b'999')])
    assert set(z.items()) == {('abc', b'456'), ('xyz', b'654'), ('uvw', b'999')}
    z.update({'xyz': b'321'})
    assert set(z.items()) == {('abc', b'456'), ('xyz', b'321'), ('uvw', b'999')}

    del z['abc']
    with pytest.raises(KeyError):
        z['abc']
    with pytest.raises(KeyError):
        del z['abc']
    assert 'abc' not in z
    assert set(z) == {'uvw', 'xyz'}
    assert len(z) == 2

    z['def'] = b'\x00\xff'
    assert len(z) == 3
    assert z['def'] == b'\x00\xff'
    assert 'def' in z

    stress_test_mapping_updates(z)
