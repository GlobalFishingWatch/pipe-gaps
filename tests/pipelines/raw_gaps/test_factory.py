from datetime import timedelta
from dataclasses import replace

from gfw.common.beam.transforms import (
    ReadFromBigQuery,
    ReadFromJson,
    WriteToBigQueryWrapper,
    WriteToJson,
)

from pipe_gaps.pipelines.raw_gaps.factory import RawGapsLinearDagFactory
from pipe_gaps.pipelines.raw_gaps.transforms.detect_gaps import DetectGaps


def test_sources_with_json_in_and_bq_in(base_config):
    # Test with both JSON and BigQuery inputs
    base_config = replace(base_config, json_in_messages="path/to/json")
    factory = RawGapsLinearDagFactory(base_config)

    sources = factory.sources
    # Should include ReadFromJson and ReadFromBigQuery transforms
    assert any(isinstance(s, ReadFromJson) for s in sources)
    assert any(isinstance(s, ReadFromBigQuery) for s in sources)
    assert len(sources) == 2


def test_sources_with_only_bq_in(base_config):
    # JSON input disabled
    base_config = replace(base_config, json_in_messages=None)
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
    base_config = replace(base_config, skip_open_gaps=True)
    factory = RawGapsLinearDagFactory(base_config)
    side_inputs = factory.side_inputs

    assert side_inputs is None


def test_side_inputs_skipped_when_start_date_before_open_gaps(base_config):
    base_config = replace(
        base_config,
        skip_open_gaps=False,
        # Set start_date before open_gaps_start_date
        date_range=("2020-01-01", "2020-01-02"),
        open_gaps_start_date="2021-01-01",
    )

    factory = RawGapsLinearDagFactory(base_config)
    side_inputs = factory.side_inputs

    assert side_inputs is None


def test_sinks_with_bq_and_json(base_config):
    base_config = replace(base_config, save_json=True)
    factory = RawGapsLinearDagFactory(base_config)

    sinks = factory.sinks
    # Should contain a WriteToBigQueryWrapper and a WriteToJson
    assert any(isinstance(s, WriteToBigQueryWrapper) for s in sinks)
    assert any(isinstance(s, WriteToJson) for s in sinks)


def test_sinks_with_only_bq(base_config):
    base_config = replace(base_config, save_json=False)
    factory = RawGapsLinearDagFactory(base_config)

    sinks = factory.sinks
    assert all(isinstance(s, WriteToBigQueryWrapper) for s in sinks)
    assert len(sinks) == 1
