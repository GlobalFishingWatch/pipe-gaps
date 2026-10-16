import dataclasses
import logging

from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Optional, Sequence

from gfw.common.bigquery.helper import BigQueryHelper
from gfw.common.query import Query
from pipe_gaps.pipelines.events.config import GapEventsConfig
from pipe_gaps.pipelines.events.table_config import (
    GapEventsTableConfig,
    GapEventsTableDescription,
    IntentionalDisablingEventsTableConfig,
    IntentionalDisablingEventsTableDescription,
)
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


@dataclass
class GapEventQuery(Query):
    """Gap events, built from raw gaps and enriched with segments, vessels, voyages, port
    visits, regions and, optionally, the intentional disabling classification.

    Its fields are the template's variables (see template_vars). Use from_config to build it
    from the pipeline configuration.
    """

    source_gaps: str
    source_segment_info: str
    source_segs_activity: str
    source_regions: str
    source_voyages: str
    source_port_visits: str
    source_vessels_byyear: str
    vessel_info_field_prefix: str
    vessel_info_flag_field: str
    start_date: date
    end_date: date
    exclude_open_gaps: bool
    regions: list[str]
    classify_disabling: bool
    source_sat_reception: Optional[str]
    disabling_min_gap_duration_h: float
    disabling_min_distance_from_shore_m: float
    disabling_min_reception_positions_per_day: float
    disabling_min_positions_before: float
    disabling_min_year: int

    @property
    def template_filename(self) -> str:
        return "events.sql.j2"

    @property
    def template_vars(self) -> dict:
        return dataclasses.asdict(self)

    @classmethod
    def from_config(
        cls, config: GapEventsConfig, regions: Sequence[dict[str, Any]] = ()
    ) -> "GapEventQuery":
        """Builds the query from the pipeline configuration and pipe-regions' registry rows."""
        return cls(
            source_gaps=config.bq_in_raw_gaps,
            source_segment_info=config.bq_in_segment_info,
            source_segs_activity=config.bq_in_segs_activity,
            source_regions=config.bq_in_regions,
            source_voyages=config.bq_in_voyages,
            source_port_visits=config.bq_in_port_visits,
            source_vessels_byyear=config.bq_in_vessels_byyear,
            vessel_info_field_prefix=config.bq_in_vessels_byyear_field_prefix,
            vessel_info_flag_field=config.vessels_byyear_flag_field,
            start_date=config.start_date,
            end_date=config.end_date,
            exclude_open_gaps=config.exclude_open_gaps,
            regions=[region["name"] for region in regions],
            classify_disabling=config.classify_disabling,
            source_sat_reception=config.bq_in_sat_reception,
            disabling_min_gap_duration_h=config.disabling_min_gap_duration_h,
            disabling_min_distance_from_shore_m=config.disabling_min_distance_from_shore_m,
            disabling_min_reception_positions_per_day=(
                config.disabling_min_reception_positions_per_day
            ),
            disabling_min_positions_before=config.disabling_min_positions_before,
            disabling_min_year=config.disabling_min_year,
        )


@dataclass
class IntentionalDisablingEventsQuery(Query):
    """Gap events classified as intentional disabling, from the gap events table.

    Its fields are the template's variables (see template_vars).
    """

    source_gap_events: str

    @property
    def template_filename(self) -> str:
        return "intentional_disabling_events.sql.j2"

    @property
    def template_vars(self) -> dict:
        return dataclasses.asdict(self)


def run(config: GapEventsConfig, bq_client_factory: Callable = None) -> None:
    bq_client_factory = bq_client_factory or BigQueryHelper.get_client_factory(
        mocked=config.mock_bq_clients
    )

    bq = BigQueryHelper(
        dry_run=config.dry_run,
        project=config.project,
        client_factory=bq_client_factory
    )

    regions = fetch_regions_registry(bq, config.bq_in_regions_registry)
    events_query = GapEventQuery.from_config(config, regions=regions)

    table_config = GapEventsTableConfig(
        table_id=config.bq_out_gap_events,
        description=GapEventsTableDescription(
            version=__version__,
            relevant_params={}
        ),
        regions=regions,
    )

    writes = [(events_query, table_config)]

    if config.bq_out_intentional_disabling_events is not None:
        disabling_table_config = IntentionalDisablingEventsTableConfig(
            table_id=config.bq_out_intentional_disabling_events,
            description=IntentionalDisablingEventsTableDescription(
                version=__version__,
                relevant_params={}
            ),
            regions=regions,
        )
        disabling_query = IntentionalDisablingEventsQuery(table_config.table_id)
        writes.append((disabling_query, disabling_table_config))

    logger.info(f"Creating gap events for dates [{config.start_date}, {config.end_date})...")
    for query, query_table_config in writes:
        logger.info(f"Writing {query_table_config.table_id}...")
        # Recreates the table if its partitioning or clustering differ from its config,
        # which WRITE_TRUNCATE can't change.
        bq.run_query(
            query_str=query.render(),
            destination=query_table_config.table_id,
            labels=config.labels,
            write_disposition="WRITE_TRUNCATE",
            partition_field=query_table_config.partition_field,
            partition_type=query_table_config.partition_type,
            clustering_fields=query_table_config.clustering_fields,
            schema=query_table_config.schema,
            description=query_table_config.description.render(),
            allow_table_recreation=True,
        )

    logger.info("Done.")
    logger.info("You can check the results in:")
    for _, query_table_config in writes:
        logger.info(query_table_config.table_id)
