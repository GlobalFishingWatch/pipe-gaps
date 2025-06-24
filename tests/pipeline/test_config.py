import pytest
from datetime import date, timedelta

from pipe_gaps.pipeline.config import GapsPipelineConfig
from pipe_gaps.common.beam.transforms.read_from_json import ReadFromJson
from pipe_gaps.common.beam.transforms.write_to_json import WriteToJson
from pipe_gaps.common.beam.transforms.read_from_bigquery import ReadFromBigQuery
from gfw.common.beam.transforms import WriteToPartitionedBigQuery
from pipe_gaps.pipeline.transforms.detect_gaps import DetectGaps


@pytest.fixture
def base_config_kwargs():
    return {
        "date_range": ("2024-01-01", "2024-01-03"),
        "bq_input_messages": "project.dataset.messages",
        "bq_input_segments": "project.dataset.segments",
        "bq_output_gaps": "project.dataset.gaps",
    }


def test_post_init_validation_raises_when_no_inputs():
    with pytest.raises(ValueError, match="You need to provide either a JSON inputs or BQ input."):
        GapsPipelineConfig(date_range=("2024-01-01", "2024-01-03"))


@pytest.fixture
def config(base_config_kwargs) -> GapsPipelineConfig:
    return GapsPipelineConfig(**base_config_kwargs)


@pytest.fixture
def config_with_json(base_config_kwargs) -> GapsPipelineConfig:
    return GapsPipelineConfig(json_input_messages="messages.json", **base_config_kwargs)


@pytest.fixture
def config_with_save_json(base_config_kwargs) -> GapsPipelineConfig:
    return GapsPipelineConfig(save_json=True, **base_config_kwargs)


@pytest.fixture
def config_without_bq_io(config_with_json):
    # Provide JSON input, remove BQ inputs & output for testing
    config = config_with_json.to_dict()
    del config["bq_input_messages"]
    del config["bq_output_gaps"]
    return GapsPipelineConfig(**config)


def test_start_and_end_dates(config):
    assert config.start_date == date(2024, 1, 1)
    assert config.end_date == date(2024, 1, 3)


def test_open_gaps_start_parsing(config):
    assert config.open_gaps_start == date(2019, 1, 1)


def test_messages_query_start_date(config):
    expected = date(2024, 1, 1) - timedelta(days=1)
    assert config.messages_query_start_date == expected


def test_read_from_bigquery_factory_type(config):
    factory = config.read_from_bigquery_factory
    assert callable(factory)


def test_write_to_bigquery_factory_type(config):
    factory = config.write_to_bigquery_factory
    assert callable(factory)


def test_gaps_table_config(config):
    table_config = config.gaps_table_config
    assert table_config.table_id == "project.dataset.gaps"


def test_sources_with_json(config_with_json):
    sources = config_with_json.sources
    assert any(isinstance(s, ReadFromJson) for s in sources)


def test_sources_with_bigquery(config):
    sources = config.sources
    assert any(isinstance(s, ReadFromBigQuery) for s in sources)


def test_core_returns_detect_gaps(config):
    core = config.core
    assert isinstance(core, DetectGaps)


def test_side_inputs_none_when_skipped(config):
    config.skip_open_gaps = True
    assert config.side_inputs is None


def test_side_inputs_none_when_before_start(config):
    config.open_gaps_start_date = "2025-01-01"
    assert config.side_inputs is None


def test_side_inputs_returns_read_from_bq(config):
    config.open_gaps_start_date = "2020-01-01"
    config.skip_open_gaps = False
    assert isinstance(config.side_inputs, ReadFromBigQuery)


def test_sinks_with_bq_output(config):
    sinks = config.sinks
    assert any(isinstance(s, WriteToPartitionedBigQuery) for s in sinks)


def test_sinks_with_save_json(config_with_save_json):
    sinks = config_with_save_json.sinks
    assert any(isinstance(s, WriteToJson) for s in sinks)


def test_no_bigquery_io(config_without_bq_io):
    sources = config_without_bq_io.sources
    sinks = config_without_bq_io.sinks

    assert not any(isinstance(s, ReadFromBigQuery) for s in sources)
    assert not any(isinstance(s, WriteToPartitionedBigQuery) for s in sinks)
