import pytest

from pipe_gaps.common.key import Key


@pytest.fixture
def key():
    return Key(["user", "country"])


def test_repr_returns_list_string():
    k = Key(["x", "y"])
    assert repr(k) == "['x', 'y']"


def test_list_returns_keys(key):
    assert key.list() == ["user", "country"]


@pytest.mark.parametrize(
    "values, expected",
    [
        (("alice", "AR"), "(user=alice, country=AR)"),
        (["bob", "US"], "(user=bob, country=US)"),
        ("solo", "(user=solo)"),
    ]
)
def test_format_values(values, expected):
    keys = Key(["user", "country"]) if isinstance(values, (list, tuple)) else Key(["user"])
    assert keys.format(values) == expected
