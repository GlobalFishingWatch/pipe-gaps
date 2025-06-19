import pytest

from pipe_gaps.common.iterables import binary_search_first_ge


@pytest.mark.parametrize(
    "items, start_value, key, expected",
    [
        pytest.param([], 5, lambda x: x, None, id="empty list"),
        pytest.param([1, 2, 3, 4, 5], 3, lambda x: x, 2, id="exact match"),
        pytest.param([1, 2, 4, 5], 3, lambda x: x, 2, id="no exact match, find next"),
        pytest.param([1, 2, 3, 4, 5], 6, lambda x: x, None, id="start_value too high"),
        pytest.param([1, 2, 3, 4, 5], 0, lambda x: x, 0, id="start_value too low"),
        pytest.param(
            [{"val": 10}, {"val": 20}, {"val": 30}],
            25,
            lambda x: x["val"],
            2,
            id="key function with dicts"
        ),
        pytest.param(
            [{"val": 10}, {"val": 20}, {"val": 30}],
            35,
            lambda x: x["val"],
            None,
            id="start_value above max key with dicts"
        ),
    ],
)
def test_binary_search_first_ge(items, start_value, key, expected):
    result = binary_search_first_ge(items, start_value, key)
    assert result == expected
