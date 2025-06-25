import pytest
from pipe_gaps.common.dictionaries import copy_dict_without


@pytest.mark.parametrize(
    "original, keys, expected",
    [
        pytest.param(
            {"a": 1, "b": 2, "c": 3},
            ["b", "c"],
            {"a": 1},
            id="remove_existing_keys"
        ),
        pytest.param(
            {"x": 10, "y": 20},
            [],
            {"x": 10, "y": 20},
            id="no_keys_removed_when_none_specified"
        ),
        pytest.param(
            {"foo": "bar"},
            ["nonexistent"],
            {"foo": "bar"},
            id="remove_nonexistent_key"
        ),
    ]
)
def test_copy_dict_without_behavior(original, keys, expected):
    result = copy_dict_without(original, keys)
    assert result == expected


def test_original_dict_is_unchanged():
    original = {"key": "value", "remove": "me"}
    _ = copy_dict_without(original, ["remove"])
    assert original == {"key": "value", "remove": "me"}
