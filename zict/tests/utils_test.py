import random
import string
from collections.abc import ItemsView, KeysView, MutableMapping, ValuesView

import pytest


def generate_random_strings(n, min_len, max_len):
    r = random.Random(42)
    out = []
    chars = string.ascii_lowercase + string.digits

    for _ in range(n):
        nchars = r.randint(min_len, max_len)
        s = "".join(r.choice(chars) for _ in range(nchars))
        out.append(s)

    return out


def to_bytestring(s):
    if isinstance(s, bytes):
        return s
    else:
        return s.encode("latin1")


def check_items(z, expected_items):
    items = list(z.items())
    assert len(items) == len(expected_items)
    assert sorted(items) == sorted(expected_items)
    # All iterators should walk the mapping in the same order
    assert list(z.keys()) == [k for k, v in items]
    assert list(z.values()) == [v for k, v in items]
    assert list(z) == [k for k, v in items]

    # ItemsView, KeysView, ValuesView.__contains__()
    assert isinstance(z.keys(), KeysView)
    assert isinstance(z.values(), ValuesView)
    assert isinstance(z.items(), ItemsView)
    assert items[0] in z.items()
    assert items[0][0] in z.keys()
    assert items[0][0] in z
    assert items[0][1] in z.values()
    assert (object(), object()) not in z.items()
    assert object() not in z.keys()
    assert object() not in z
    assert object() not in z.values()


def stress_test_mapping_updates(z):
    # Certain mappings shuffle between several underlying stores
    # during updates.  This stress tests the internal mapping
    # consistency.
    r = random.Random(42)

    keys = list(string.ascii_lowercase)
    values = [to_bytestring(s) for s in generate_random_strings(len(keys), 1, 10)]

    z.clear()
    assert len(z) == 0

    for k, v in zip(keys, values):
        z[k] = v
    assert len(z) == len(keys)
    assert sorted(z) == sorted(keys)
    assert sorted(z.items()) == sorted(zip(keys, values))

    for _ in range(3):
        r.shuffle(keys)
        r.shuffle(values)
        for k, v in zip(keys, values):
            z[k] = v
        check_items(z, list(zip(keys, values)))

        r.shuffle(keys)
        r.shuffle(values)
        z.update(zip(keys, values))
        check_items(z, list(zip(keys, values)))


def check_empty_mapping(z):
    assert not z
    assert list(z) == list(z.keys()) == []
    assert list(z.values()) == []
    assert list(z.items()) == []
    assert len(z) == 0
    assert "x" not in z
    assert "x" not in z.keys()
    assert ("x", b"123") not in z.items()
    assert b"123" not in z.values()


def check_mapping(z):
    """See also test_zip.check_mapping"""
    assert type(z).__name__ in str(z)
    assert type(z).__name__ in repr(z)
    assert isinstance(z, MutableMapping)
    check_empty_mapping(z)

    z["abc"] = b"456"
    z["xyz"] = b"12"
    assert len(z) == 2
    assert z["abc"] == b"456"

    check_items(z, [("abc", b"456"), ("xyz", b"12")])

    assert "abc" in z
    assert "xyz" in z
    assert "def" not in z
    assert object() not in z

    with pytest.raises(KeyError):
        z["def"]

    z.update(xyz=b"707", uvw=b"000")
    check_items(z, [("abc", b"456"), ("xyz", b"707"), ("uvw", b"000")])
    z.update([("xyz", b"654"), ("uvw", b"999")])
    check_items(z, [("abc", b"456"), ("xyz", b"654"), ("uvw", b"999")])
    z.update({"xyz": b"321"})
    check_items(z, [("abc", b"456"), ("xyz", b"321"), ("uvw", b"999")])
    # Update with iterator (can read only once)
    z.update(iter([("foo", b"132"), ("bar", b"887")]))
    check_items(
        z,
        [
            ("abc", b"456"),
            ("xyz", b"321"),
            ("uvw", b"999"),
            ("foo", b"132"),
            ("bar", b"887"),
        ],
    )

    del z["abc"]
    with pytest.raises(KeyError):
        z["abc"]
    with pytest.raises(KeyError):
        del z["abc"]
    assert "abc" not in z
    assert set(z) == {"uvw", "xyz", "foo", "bar"}
    assert len(z) == 4

    z["def"] = b"\x00\xff"
    assert len(z) == 5
    assert z["def"] == b"\x00\xff"
    assert "def" in z

    stress_test_mapping_updates(z)


def check_closing(z):
    z.close()


def check_bad_key_types(z, has_del=True):
    """z does not accept any Hashable as keys.
    Test that it reacts correctly when confronted with an invalid key type.
    """
    bad = object()

    assert bad not in z
    assert bad not in z.keys()
    assert (bad, b"123") not in z.items()

    with pytest.raises(TypeError):
        z[bad] = b"123"
    with pytest.raises(TypeError):
        z.update({bad: b"123"})
    with pytest.raises(KeyError):
        z[bad]
    if has_del:
        with pytest.raises(KeyError):
            del z[bad]


def check_bad_value_types(z):
    """z does not accept any Python object as values.
    Test that it reacts correctly when confronted with an invalid value type.
    """
    bad = object()

    assert bad not in z.values()
    assert ("x", bad) not in z.items()

    with pytest.raises(TypeError):
        z["x"] = bad
    with pytest.raises(TypeError):
        z.update({"x": bad})
