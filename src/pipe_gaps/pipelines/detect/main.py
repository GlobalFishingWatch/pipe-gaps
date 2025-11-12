import logging
from types import SimpleNamespace

from gfw.common.beam.pipeline.factory import PipelineFactory

from pipe_gaps.pipelines.detect.config import DetectGapsConfig
from pipe_gaps.pipelines.detect.factory import DetectGapsLinearDagFactory
from pipe_gaps.version import __version__


logger = logging.getLogger(__name__)


def run(config: SimpleNamespace) -> None:
    config = DetectGapsConfig.from_namespace(config, version=__version__)
    dag_factory = DetectGapsLinearDagFactory(config)
    pipeline_factory = PipelineFactory(config, dag_factory=dag_factory)
    pipeline = pipeline_factory.build_pipeline()
    result, _ = pipeline.run()
