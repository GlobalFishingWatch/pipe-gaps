from pipe_gaps.utils import timing


def test_timing():
    expected = "mocked_result"

    result, elapsed = timing(lambda x: x)(expected)

    assert result == expected
    assert isinstance(elapsed, float)
