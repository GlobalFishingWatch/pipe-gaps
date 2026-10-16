from pipe_gaps.cli.validations import ssvids


def test_ssvids_single():
    result = ssvids("123456789")
    assert result == ("123456789",)


def test_ssvids_multiple():
    result = ssvids("123456789,987654321,111222333")
    assert result == ("123456789", "987654321", "111222333")


def test_ssvids_empty():
    result = ssvids("")
    assert result == ("",)
