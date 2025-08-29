import logging
from types import SimpleNamespace

from gfw.common.beam.pipeline.factory import PipelineFactory

from pipe_gaps.pipeline.config import RawGapsConfig
from pipe_gaps.pipeline.factory import RawGapsLinearDagFactory
from pipe_gaps.version import __version__


logger = logging.getLogger(__name__)


def run(config: SimpleNamespace) -> None:
    config = RawGapsConfig.from_namespace(config, version=__version__)

    # This is how we pass the execution project to dataflow.
    # TODO: Allow PipelineFactory to pass extra kwargs to be passed to Pipeline constructor.
    config.unknown_parsed_args["project"] = config.gcp_project

    dag_factory = RawGapsLinearDagFactory(config)
    pipeline_factory = PipelineFactory(config, dag_factory=dag_factory)
    pipeline = pipeline_factory.build_pipeline()
    result, _ = pipeline.run()
