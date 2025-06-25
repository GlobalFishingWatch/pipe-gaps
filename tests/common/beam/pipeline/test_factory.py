from apache_beam import Map
from pipe_gaps.common.config.config import Config
from pipe_gaps.common.beam.pipeline.factory import PipelineFactory
from pipe_gaps.common.beam.pipeline.dag.factory import LinearDagFactory
from gfw.common.beam.pipeline import Pipeline


class DummyConfig(Config):
    version: str = "test-version"
    mock_bq_clients: bool = False


class DummyDagFactory(LinearDagFactory):
    def __init__(self, config):
        self.config = config

    @property
    def sources(self):
        return (Map(lambda x: x),)

    @property
    def core(self):
        return Map(lambda x: x)

    @property
    def sinks(self):
        return (Map(lambda x: x),)


def test_pipeline_factory_with_real_factory():
    config = DummyConfig(date_range=("2020-01-01", "2020-01-02"))
    factory = PipelineFactory(config=config, dag_factory=DummyDagFactory(config))

    pipeline = factory.build_pipeline()

    assert isinstance(pipeline, Pipeline)
