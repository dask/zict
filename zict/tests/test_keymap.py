import pytest

from zict import KeyMap
from zict.tests import utils_test


def test_simple():
    d = {}
    z = KeyMap(str, d)
    z[1] = 10
    assert d == {"1": 10}
    assert z.keymap == {1: "1"}
    assert 1 in z
    assert 2 not in z
    assert list(z) == [1]
    assert len(z) == 1
    assert z[1] == 10
    with pytest.raises(KeyError):
        z[2]
    del z[1]
    assert 1 not in z
    assert 1 not in z.keymap


def test_mapping():
    z = KeyMap(str, {})
    utils_test.check_mapping(z)
    utils_test.check_closing(z)


@pytest.mark.stress
@pytest.mark.repeat(utils_test.REPEAT_STRESS_TESTS)
def test_stress_same_key_threadsafe():
    d = utils_test.SlowDict(0.001)
    z = KeyMap(str, d)
    utils_test.check_same_key_threadsafe(z)
    assert not z.keymap
    assert not z.d
    utils_test.check_mapping(z)
