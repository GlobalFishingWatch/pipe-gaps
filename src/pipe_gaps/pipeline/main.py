import logging
from types import SimpleNamespace

from apache_beam.runners.runner import PipelineState

from gfw.common.beam.pipeline.factory import PipelineFactory

from pipe_gaps.pipeline.config import RawGapsConfig
from pipe_gaps.pipeline.factory import RawGapsLinearDagFactory
from pipe_gaps.pipeline.hooks import create_view_hook
from pipe_gaps.version import __version__


logger = logging.getLogger(__name__)


def run(config: SimpleNamespace) -> None:
    config = RawGapsConfig.from_namespace(config, version=__version__)

    dag_factory = RawGapsLinearDagFactory(config)
    pipeline = PipelineFactory(config, dag_factory=dag_factory).build_pipeline()
    result, _ = pipeline.run()

    # TODO: Move this logic inside to Pipeline.run() method of gfw-common.
    if result.state == PipelineState.DONE and config.bq_output_gaps:
        hook = create_view_hook(dag_factory.raw_gaps_table_config, mock=config.mock_bq_clients)
        hook(pipeline)
    else:
        logger.warning("Pipeline did not finish successfully; skipping view creation.")
