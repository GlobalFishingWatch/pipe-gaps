import pytest
import argparse

from pipe_gaps.cli.validations import date_range, ssvids, ERROR_DATE_RANGE


def test_date_range_valid():
    result = date_range("2023-01-01,2023-01-31")
    assert result == ("2023-01-01", "2023-01-31")


def test_date_range_invalid_no_comma():
    with pytest.raises(argparse.ArgumentTypeError) as excinfo:
        date_range("2023-01-01")
    assert ERROR_DATE_RANGE in str(excinfo.value)


def test_date_range_invalid_three_parts():
    with pytest.raises(argparse.ArgumentTypeError) as excinfo:
        date_range("2023-01-01,2023-01-31,2023-02-01")
    assert ERROR_DATE_RANGE in str(excinfo.value)


def test_ssvids_single():
    result = ssvids("123456789")
    assert result == ("123456789",)


def test_ssvids_multiple():
    result = ssvids("123456789,987654321,111222333")
    assert result == ("123456789", "987654321", "111222333")


def test_ssvids_empty():
    result = ssvids("")
    assert result == ("",)
