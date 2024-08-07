import pytest

from pipe_gaps.pipeline import PipelineConfig, PipeConfigError, Pipeline


def test_config(tmp_path):
    with pytest.raises(PipeConfigError):
        config = PipelineConfig()
        config.validate()

    config = PipelineConfig(input_file=str(tmp_path.joinpath("test.json")))
    config.validate()
    config.to_json()

    with pytest.raises(PipeConfigError):
        config = PipelineConfig(input_query=dict(start_date="2024-01-01"))
        config.validate()

    config = PipelineConfig(input_query=dict(start_date="2024-01-01", end_date="2024-01-01"))
    config.validate()
    config.to_json()


def test_build_raises_error():
    with pytest.raises(NotImplementedError):
        Pipeline.build(input_file="")
