import logging

from functools import cached_property
from types import SimpleNamespace
from typing import Any, Callable, Sequence

from gfw.common.bigquery.helper import BigQueryHelper
from gfw.common.query import Query
from pipe_gaps.pipelines.events.config import GapEventsConfig
from pipe_gaps.pipelines.events.table_config import GapEventsTableConfig, GapEventsTableDescription
from pipe_gaps.version import __version__


logger = logging.getLogger(__name__)


def fetch_regions_registry(
    bq_helper: BigQueryHelper, bq_in_regions_registry: str
) -> list[dict[str, Any]]:
    """Reads pipe-regions' ``(name, description)`` registry table.

    Used to build the query's region struct and the output table's per-region schema fields
    dynamically, instead of hardcoding the region list -- see `GapEventQuery.template_vars` and
    `GapEventsTableConfig.schema`.

    Always runs for real, even when ``bq_helper`` is configured for dry runs: this is a cheap
    metadata read needed to build a syntactically valid query, not the expensive/destructive
    operation ``--dry-run`` is meant to skip. Skipping it too would leave `regions` empty and
    render an invalid ``STRUCT<>`` in the main query (see `utils.sql.j2`).
    """
    query = f"SELECT name, description FROM `{bq_in_regions_registry}` ORDER BY name"
    rows = bq_helper.run_query(query, dry_run=False).tolist(as_dicts=True)
    return [dict(row) for row in rows]


class GapEventQuery(Query):
    def __init__(
        self, config: GapEventsConfig, regions: Sequence[dict[str, Any]] = ()
    ) -> None:
        self.config = config
        self.regions = regions

    @cached_property
    def template_filename(self) -> str:
        return "events.sql.j2"

    @cached_property
    def template_vars(self) -> dict:
        start_date, end_date = self.config.date_range

        return {
            "source_gaps": self.config.bq_in_raw_gaps,
            "source_segment_info": self.config.bq_in_segment_info,
            "source_segs_activity": self.config.bq_in_segs_activity,
            "source_regions": self.config.bq_in_regions,
            "source_voyages": self.config.bq_in_voyages,
            "source_port_visits": self.config.bq_in_port_visits,
            "source_vessels_byyear": self.config.bq_in_vessels_byyear,
            "vessel_info_field_prefix": self.config.bq_in_vessels_byyear_field_prefix,
            "vessel_info_flag_field": self.config.vessels_byyear_flag_field,
            "start_date": self.config.start_date,
            "end_date": self.config.end_date,
            "exclude_open_gaps": self.config.exclude_open_gaps,
            "regions": [region["name"] for region in self.regions],
        }


def run(
    config: SimpleNamespace,
    unknown_unparsed_args: tuple = (),
    unknown_parsed_args: dict = None,
    bq_client_factory: Callable = None,
) -> None:

    config = GapEventsConfig.from_namespace(
        config,
        version=__version__,
        name="pipe-gaps--gap-events"
    )

    bq_client_factory = bq_client_factory or BigQueryHelper.get_client_factory(
        mocked=config.mock_bq_clients
    )

    bq = BigQueryHelper(
        dry_run=config.dry_run,
        project=config.project,
        client_factory=bq_client_factory
    )

    regions = fetch_regions_registry(bq, config.bq_in_regions_registry)
    events_query = GapEventQuery(config, regions=regions)

    table_config = GapEventsTableConfig(
        table_id=config.bq_out_gap_events,
        description=GapEventsTableDescription(
            version=__version__,
            relevant_params={}
        ),
        regions=regions,
    )

    logger.info(f'Executing events query for date range: {config.date_range}...')
    bq.run_query(
        query_str=events_query.render(),
        destination=config.bq_out_gap_events,
        labels=config.labels,
        write_disposition="WRITE_TRUNCATE"
    )

    # TODO: Move this to BigQueryHelper.
    logger.info("Updating table schema and description...")
    table = bq.client.get_table(table_config.table_id)
    table.schema = table_config.schema
    table.description = table_config.description.render()
    table = bq.client.update_table(table, ["schema", "description"])
    logger.info("Done.")
    logger.info("You can check the results in:")
    logger.info(f"{table_config.table_id}")
