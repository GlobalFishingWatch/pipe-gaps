import logging
from typing import Callable

from gfw.common.bigquery.helper import BigQueryHelper
from gfw.common.beam.pipeline import Pipeline

from pipe_gaps.common.config.bigquery_table import BigQueryTableConfig as BQTableConfig

logger = logging.getLogger(__name__)


# TODO: Move this reusable hook to gfw-common.
def create_view_hook(table_config: BQTableConfig, mock: bool = False) -> Callable[Pipeline, None]:
    def _hook(_: Pipeline) -> None:
        view_id = table_config.view_id
        view_query = table_config.view_query
        logger.info(f"Creating view: {view_id}")
        client_factory = BigQueryHelper.get_client_factory(mocked=mock)
        bq_client = BigQueryHelper(client_factory=client_factory, project=None)
        bq_client.create_view(view_id=view_id, view_query=view_query, exists_ok=True)
    return _hook
