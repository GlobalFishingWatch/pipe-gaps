import logging
from types import SimpleNamespace
from typing import Callable
from functools import cached_property

from gfw.common.bigquery.helper import BigQueryHelper
from gfw.common.query import Query

from pipe_gaps.version import __version__
from pipe_gaps.pipelines.publish.config import PublishGapsConfig
from pipe_gaps.pipelines.publish.table_config import (
    GapEventsTableConfig, GapEventsTableDescription
)

logger = logging.getLogger(__name__)


class GapEventQuery(Query):
    def __init__(self, config: PublishGapsConfig) -> None:
        self.config = config

    @cached_property
    def template_filename(self) -> str:
        return "events.sql.j2"

    @cached_property
    def template_vars(self) -> dict:
        start_date, end_date = self.config.date_range

        return {
            "source_gaps": self.config.bq_input_gaps,
            "source_segment_info": self.config.bq_input_segment_info,
            "source_segs_activity": self.config.bq_input_segs_activity,
            "source_regions": self.config.bq_input_regions,
            "source_voyages": self.config.bq_input_voyages,
            "source_port_visits": self.config.bq_input_port_visits,
            "source_vessels_byyear": self.config.bq_input_vessels_byyear,
            "start_date": self.config.start_date,
            "end_date": self.config.end_date,
        }


def run(
    config: SimpleNamespace,
    unknown_unparsed_args: tuple = (),
    unknown_parsed_args: dict = None,
    bq_client_factory: Callable = None,
) -> None:

    config = PublishGapsConfig.from_namespace(
        config,
        version=__version__,
        name="pipe-gaps--publish"
    )

    bq_client_factory = bq_client_factory or BigQueryHelper.get_client_factory(
        mocked=config.mock_bq_clients
    )

    events_query = GapEventQuery(config)

    bq = BigQueryHelper(
        dry_run=config.dry_run,
        project=config.project,
        client_factory=bq_client_factory
    )

    table_config = GapEventsTableConfig(
        table_id=config.bq_output,
        description=GapEventsTableDescription(
            version=__version__,
            relevant_params={}
        ),
    )

    logger.info(f'Executing events query for date range: {config.date_range}...')
    query_result = bq.run_query(
        query_str=events_query.render(),
        destination=config.bq_output,
        labels=config.labels,
        write_disposition="WRITE_TRUNCATE"
    )

    _ = query_result.query_job.result()

    # TODO: Move this to BigQueryHelper.
    logger.info("Updating table schema and description...")
    table = bq.client.get_table(table_config.table_id)
    table.schema = table_config.schema
    table.description = table_config.description.render()
    table = bq.client.update_table(table, ["schema", "description"])
    logger.info("Done.")
    logger.info("You can check the results in:")
    logger.info(f"{table_config.table_id}")
