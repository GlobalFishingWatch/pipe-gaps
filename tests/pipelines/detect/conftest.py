import pytest
from pipe_gaps.pipelines.detect.config import DetectGapsConfig


@pytest.fixture
def base_config():
    return DetectGapsConfig(
        date_range=("2023-01-01", "2023-01-02"),
        json_in_messages=None,
        bq_in_messages="some_bq_table",
        bq_in_segments="some_bq_segments",
        bq_out_gaps="output_gaps_table",
        bq_write_disposition="WRITE_APPEND",
        filter_good_seg=True,
        filter_not_overlapping_and_short=True,
        min_gap_length=5.0,
        n_hours_before=10,
        eval_last=True,
        normalize_output=True,
        save_json=False,
        mock_bq_clients=False,
        work_dir="workdir",
        skip_open_gaps=False,
        open_gaps_start_date="2022-12-31",
        bq_in_open_gaps=None,
        bq_read_method="EXPORT",
        ssvids=(),
        window_period_d=None,
    )
