import pytest

from pipe_gaps.pipeline import Pipeline, PipelineConfig, PipeConfigError


def test_config(tmp_path):
    with pytest.raises(PipeConfigError):
        config = PipelineConfig()
        config.validate()

    with pytest.raises(PipeConfigError):
        config = PipelineConfig(inputs=[{}], side_inputs=[{}, {}])
        config.validate()

    with pytest.raises(PipeConfigError):
        config = PipelineConfig(inputs=[{}], side_inputs=[{}])
        config.validate()


def test_build():
    with pytest.raises(NotImplementedError):
        Pipeline.build(inputs=[{}], core={"kind": "dummy"})
