import logging
from datetime import date
from types import SimpleNamespace
from typing import Callable
from importlib import resources


from gfw.common.bigquery.helper import BigQueryHelper
from gfw.common.datetime import datetime_from_isoformat

from pipe_gaps.assets import queries
from pipe_gaps.version import __version__
from pipe_gaps.pipelines.raw_gaps_events.table_config import (
    RawGapsEventsTableConfig, RawGapsEventsTableDescription
)

logger = logging.getLogger(__name__)


QUERY_FILENAME = 'normalize.sql.j2'


def run(
    config: SimpleNamespace,
    unknown_unparsed_args: tuple = (),
    unknown_parsed_args: dict = None,
    bq_client_factory: Callable = BigQueryHelper.get_client_factory(),
    labels: dict = None,
    dry_run: bool = False,
) -> None:

    if config.date_range is None:
        raise ValueError("You must provide a date-range.")

    start_date_str, end_date_str = config.date_range
    start_date, end_date = (date.fromisoformat(start_date_str), date.fromisoformat(end_date_str))

    labels = labels or {}

    bq = BigQueryHelper(dry_run=dry_run, client_factory=bq_client_factory)

    table_config = RawGapsEventsTableConfig(
        table_id=config.bq_output,
        description=RawGapsEventsTableDescription(
            version=__version__,
            relevant_params={}
        ),
    )

    logger.info(f"Creating events table '{config.bq_output}' (if it doesn't already exist)...")
    bq.create_table(**table_config.to_bigquery_params(), exists_ok=True, labels=labels)

    queries_path = resources.files(queries)
    query_params = vars(config)
    start_date = datetime_from_isoformat(config.pop("start_date"))

    events_query = bq.format_jinja2(
        template_path=QUERY_FILENAME,
        search_path=queries_path,
        start_date=start_date,
        end_date=end_date,
        **query_params,
    )

    logger.info(f'Executing events query from date {start_date}...')
    bq.run_query(events_query, labels=labels)
    logger.info("Done.")
