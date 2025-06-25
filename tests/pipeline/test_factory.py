import pytest
from datetime import timedelta

from gfw.common.beam.transforms import WriteToPartitionedBigQuery

from pipe_gaps.common.beam.transforms.read_from_json import ReadFromJson
from pipe_gaps.common.beam.transforms.read_from_bigquery import ReadFromBigQuery
from pipe_gaps.common.beam.transforms.write_to_json import WriteToJson

from pipe_gaps.pipeline.factory import RawGapsLinearDagFactory
from pipe_gaps.pipeline.config import RawGapsConfig
from pipe_gaps.pipeline.transforms.detect_gaps import DetectGaps
from pipe_gaps.pipeline.table_config import RawGapsTableConfig


@pytest.fixture
def base_config():
    return RawGapsConfig(
        date_range=("2023-01-01", "2023-01-02"),
        json_input_messages=None,
        bq_input_messages="some_bq_table",
        bq_input_segments="some_bq_segments",
        bq_output_gaps="output_gaps_table",
        bq_write_disposition="WRITE_APPEND",
        bq_output_gaps_description=True,
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
        bq_input_open_gaps=None,
        bq_read_method="EXPORT",
        ssvids=(),
        window_period_d=None,
    )


def test_raw_gaps_table_config_property(base_config):
    factory = RawGapsLinearDagFactory(base_config)
    config = factory.raw_gaps_table_config

    assert isinstance(config, RawGapsTableConfig)
    assert config.table_id == base_config.bq_output_gaps
    assert config.write_disposition == base_config.bq_write_disposition


def test_bq_output_gaps_description_params(base_config):
    factory = RawGapsLinearDagFactory(base_config)
    params = factory.bq_output_gaps_description_params

    assert params["bq_input_messages"] == base_config.bq_input_messages
    assert params["bq_input_segments"] == base_config.bq_input_segments
    assert params["filter_good_seg"] == base_config.filter_good_seg
    assert params["min_gap_length"] == base_config.min_gap_length
    assert params["n_hours_before"] == base_config.n_hours_before
    assert (
        params["filter_not_overlapping_and_short"]
        == base_config.filter_not_overlapping_and_short
    )


def test_sources_with_json_input_and_bq_input(base_config):
    # Test with both JSON and BigQuery inputs
    base_config.json_input_messages = "path/to/json"
    factory = RawGapsLinearDagFactory(base_config)

    sources = factory.sources
    # Should include ReadFromJson and ReadFromBigQuery transforms
    assert any(isinstance(s, ReadFromJson) for s in sources)
    assert any(isinstance(s, ReadFromBigQuery) for s in sources)
    assert len(sources) == 2


def test_sources_with_only_bq_input(base_config):
    # JSON input disabled
    base_config.json_input_messages = None
    factory = RawGapsLinearDagFactory(base_config)

    sources = factory.sources
    assert all(isinstance(s, ReadFromBigQuery) for s in sources)
    assert len(sources) == 1


def test_core_property(base_config):
    factory = RawGapsLinearDagFactory(base_config)
    core = factory.core

    assert isinstance(core, DetectGaps)
    assert core._gap_detector._threshold_h == timedelta(hours=base_config.min_gap_length)
    assert core._gap_detector._normalize_output == base_config.normalize_output
    assert core._eval_last == base_config.eval_last
    assert core._date_range == base_config.date_range
    assert core._window_period_d == base_config.window_period_d
    assert core._window_offset_h == base_config.n_hours_before


def test_side_inputs_with_open_gaps(base_config):
    # Case where skip_open_gaps is False and start_date > open_gaps_start
    factory = RawGapsLinearDagFactory(base_config)
    side_inputs = factory.side_inputs

    assert side_inputs is not None
    assert isinstance(side_inputs, ReadFromBigQuery)


def test_side_inputs_skipped_when_configured(base_config):
    base_config.skip_open_gaps = True
    factory = RawGapsLinearDagFactory(base_config)
    side_inputs = factory.side_inputs

    assert side_inputs is None


def test_side_inputs_skipped_when_start_date_before_open_gaps(base_config):
    base_config.skip_open_gaps = False
    # Set start_date before open_gaps_start_date
    base_config.date_range = ("2020-01-01", "2020-01-02")
    base_config.open_gaps_start_date = "2021-01-01"
    factory = RawGapsLinearDagFactory(base_config)
    side_inputs = factory.side_inputs

    assert side_inputs is None


def test_sinks_with_bq_and_json(base_config):
    base_config.save_json = True
    factory = RawGapsLinearDagFactory(base_config)

    sinks = factory.sinks
    # Should contain a WriteToPartitionedBigQuery and a WriteToJson
    assert any(isinstance(s, WriteToPartitionedBigQuery) for s in sinks)
    assert any(isinstance(s, WriteToJson) for s in sinks)


def test_sinks_with_only_bq(base_config):
    base_config.save_json = False
    factory = RawGapsLinearDagFactory(base_config)

    sinks = factory.sinks
    assert all(isinstance(s, WriteToPartitionedBigQuery) for s in sinks)
    assert len(sinks) == 1
