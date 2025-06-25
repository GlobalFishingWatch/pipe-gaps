import pytest
from datetime import date

from pipe_gaps.common.config.config import Config, ConfigError, ERROR_DATE


def test_parsed_date_range_valid():
    cfg = Config(date_range=("2023-01-01", "2023-12-31"))
    parsed = cfg.parsed_date_range
    assert isinstance(parsed, tuple)

    assert parsed[0] == date(2023, 1, 1)
    assert parsed[1] == date(2023, 12, 31)

    assert cfg.start_date == date(2023, 1, 1)
    assert cfg.end_date == date(2023, 12, 31)


def test_parsed_date_range_invalid_raises():
    invalid_range = ("2023-01-01", "not-a-date")
    cfg = Config(date_range=invalid_range)
    with pytest.raises(ConfigError) as exc_info:
        _ = cfg.parsed_date_range

    assert ERROR_DATE.format(invalid_range) in str(exc_info.value)


def test_to_dict_includes_fields():
    cfg = Config(
        date_range=("2023-01-01", "2023-12-31"),
        unknown_parsed_args={"foo": "bar"},
        unknown_unparsed_args=["--baz"],
    )

    d = cfg.to_dict()
    assert d["date_range"] == ("2023-01-01", "2023-12-31")
    assert d["unknown_parsed_args"] == {"foo": "bar"}
    assert d["unknown_unparsed_args"] == ["--baz"]
