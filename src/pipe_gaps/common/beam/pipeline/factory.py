from gfw.common.beam.pipeline import Pipeline

from pipe_gaps.common.config.config import Config
from pipe_gaps.common.beam.pipeline.dag.factory import DagFactory


class PipelineFactory:
    def __init__(self, config: Config, dag_factory: DagFactory, name: str = ""):
        self._config = config
        self._dag_factory = dag_factory
        self._name = name

    def build_pipeline(self) -> Pipeline:
        return Pipeline(
            name=self._name,
            version=self._config.version,
            dag=self._dag_factory.build_dag(),
            unparsed_args=self._config.unknown_unparsed_args,
            **self._config.unknown_parsed_args
        )
