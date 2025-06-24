import logging
from typing import Callable
from types import SimpleNamespace

from apache_beam.runners.runner import PipelineState

from gfw.common.beam.pipeline import Pipeline
from gfw.common.beam.pipeline.dag import LinearDag
from gfw.common.bigquery_helper import BigQueryHelper

from pipe_gaps.version import __version__
from pipe_gaps.pipeline.config import GapsPipelineConfig
from pipe_gaps.common.config.bigquery_table import BigQueryTableConfig as BQTableConfig

logger = logging.getLogger(__name__)


def create_view_hook(table_config: BQTableConfig, mock: bool = False) -> Callable[Pipeline, None]:
    def _hook(_: Pipeline) -> None:
        view_id = table_config.view_id
        view_query = table_config.view_query
        logger.info(f"Creating view: {view_id}")
        client_factory = BigQueryHelper.get_client_factory(mocked=mock)
        bq_client = BigQueryHelper(client_factory=client_factory, project=None)
        bq_client.create_view(view_id=view_id, view_query=view_query, exists_ok=True)
    return _hook


def run(config: SimpleNamespace) -> None:
    config = GapsPipelineConfig(**vars(config))

    pipeline = Pipeline(
        name="pipe-gaps",
        version=__version__,
        dag=LinearDag(
            sources=config.sources,
            core=config.core,
            sinks=config.sinks,
            side_inputs=config.side_inputs,
        ),
        unparsed_args=config.unknown_unparsed_args,
        **config.unknown_parsed_args
    )

    result, _ = pipeline.run()

    # Move this logic inside to pipeline.run() method.
    if result.state == PipelineState.DONE and config.bq_output_gaps:
        hook = create_view_hook(config.gaps_table_config, mock=config.mock_db_client)
        hook(pipeline)
    else:
        logger.warning("Pipeline did not finish successfully; skipping view creation.")
