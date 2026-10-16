import logging

from gfw.common.beam.pipeline.factory import PipelineFactory

from pipe_gaps.pipelines.raw_gaps.config import RawGapsConfig
from pipe_gaps.pipelines.raw_gaps.factory import RawGapsLinearDagFactory


logger = logging.getLogger(__name__)


def run(config: RawGapsConfig) -> None:
    dag_factory = RawGapsLinearDagFactory(config)
    pipeline_factory = PipelineFactory(config, dag_factory=dag_factory)
    pipeline = pipeline_factory.build_pipeline()
    result, _ = pipeline.run()
