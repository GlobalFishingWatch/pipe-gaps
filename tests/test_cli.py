import pytest

from pipe_gaps import cli
from pipe_gaps.utils import json_save


def test_cli(tmp_path):
    config_file_content = {
        "json_input_messages":  "pipe_gaps/data/sample_messages_lines.json",
        "ssvids": [],
        "date_range": [
            "2024-01-01",
            "2024-01-02"
        ],
        "min_gap_length": 1.2,
        "n_hours_before": 12,
        "window_period_d": 1,
        "filter_good_seg": False,
        "filter_not_overlapping_and_short": False,
    }

    config_file_path = tmp_path.joinpath("test.json")
    json_save(config_file_content, config_file_path)

    OVERWRITE_START_DATE = "2024-01-01"
    OVERWRITE_END_DATE = "2024-02-01"
    OVERWRITE_DATE_RANGE = f"{OVERWRITE_START_DATE},{OVERWRITE_END_DATE}"

    args = [
        "--work-dir", str(tmp_path),
        "--date-range", OVERWRITE_DATE_RANGE,
        "--filter-good-seg",
        "--filter-not-overlapping-and-short",
        "--mock-db-client",
        "--save-json",
    ]

    # With config file and verbose flag.
    args.extend(["--config-file", str(config_file_path), "--pipe-type", "naive"])
    args.extend(["-v"])

    parsed_config = cli.cli(args)

    assert parsed_config["filter_good_seg"]
    assert parsed_config["filter_not_overlapping_and_short"]
    assert parsed_config["date_range"] == [OVERWRITE_START_DATE, OVERWRITE_END_DATE]
    assert parsed_config["mock_db_client"]
    assert parsed_config["save_json"]

    args.remove("--filter-good-seg")
    parsed_config = cli.cli(args)
    assert not parsed_config["filter_good_seg"]

    with pytest.raises(SystemExit):
        cli.cli([])
