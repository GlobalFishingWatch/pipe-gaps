import pytest

from pipe_gaps.pipeline import Config, ConfigError, Pipeline


def test_config(tmp_path):
    with pytest.raises(ConfigError):
        config = Config()
        config.validate()

    config = Config(input_file=str(tmp_path.joinpath("test.json")))
    config.validate()
    config.to_json()

    with pytest.raises(ConfigError):
        config = Config(query_params=dict(start_date="2024-01-01"))
        config.validate()

    config = Config(query_params=dict(start_date="2024-01-01", end_date="2024-01-01"))
    config.validate()
    config.to_json()


def test_build_raises_error():
    with pytest.raises(NotImplementedError):
        Pipeline.build(input_file="")
