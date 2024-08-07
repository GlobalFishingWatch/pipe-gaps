import pytest

from pipe_gaps import pipeline
from pipe_gaps.pipeline import PipelineFactoryError
from pipe_gaps.data import sample_messages_path


def test_pipe_factory(monkeypatch):
    input_file = sample_messages_path()

    pipeline.is_beam_installed = False
    with pytest.raises(PipelineFactoryError):
        pipeline.create(pipe_type="beam", input_file=input_file)
    pipeline.is_beam_installed = True

    with pytest.raises(PipelineFactoryError):
        pipeline.create(pipe_type="not_implemented", input_file=input_file)
