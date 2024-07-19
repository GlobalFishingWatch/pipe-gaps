import pytest

from pipe_gaps import pipeline
from pipe_gaps.data import sample_messages_path
from pipe_gaps.pipeline.base import Config, ConfigError, Pipeline, PipelineError

from pipe_gaps.utils import json_save


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


def test_pipe_build(monkeypatch):
    input_file = sample_messages_path()

    with pytest.raises(PipelineError):
        Pipeline.create()

    pipeline.is_beam_installed = False
    with pytest.raises(PipelineError):
        Pipeline.create(pipe_type="beam", input_file=input_file)
    pipeline.is_beam_installed = True

    with pytest.raises(PipelineError):
        Pipeline.create(pipe_type="not_implemented", input_file=input_file)


def test_pipe_saved_file(tmp_path):
    messages = [
        {
            "ssvid": "226013750",
            "msgid": "295fa26f-cee9-1d86-8d28-d5ed96c32835",
            "timestamp": 1704412120.0,
        }
    ]

    input_file = tmp_path.joinpath("test.json")
    json_save(messages, input_file)

    # Without saving results.
    pipe = Pipeline.create(pipe_type="beam", input_file=input_file, save_json=False)
    pipe.run()

    # Saving results.
    pipe = Pipeline.create(pipe_type="beam", input_file=input_file, save_json=False)
    pipe.run()


def test_run(tmp_path):
    messages = [
        {
            "ssvid": "226013750",
            "msgid": "295fa26f-cee9-1d86-8d28-d5ed96c32835",
            "timestamp": 1704412120.0,
        },
        {
            "ssvid": "226013750",
            "msgid": "295fa26f-cee9-1d86-8d28-d5ed96c32835",
            "timestamp": 1704412120.0,
        },
    ]

    input_file = tmp_path.joinpath("test.json")
    json_save(messages, input_file)

    config = dict(input_file=input_file)
    pipeline.run(config)
