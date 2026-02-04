import logging
from typing import Callable

from google.cloud import bigquery

from gfw.common.beam.pipeline.config import PipelineConfig
from gfw.common.beam.pipeline.base import Pipeline
from gfw.common.bigquery.helper import BigQueryHelper


logger = logging.getLogger(__name__)

N_DAYS_AHEAD_QUERY = """
WITH max_day AS (
  SELECT MAX(DATE(last_timestamp)) AS max_day
  FROM `{table}`
)

SELECT
  max_day.max_day AS max_day,
  DATE_ADD(@end_date, INTERVAL @n_days - 1 DAY) AS required_day,
  max_day.max_day >= DATE_ADD(@end_date, INTERVAL @n_days - 1 DAY) AS is_ready
FROM max_day
"""


class InssuficientSegmentsDataError(Exception):
    pass


def create_segments_n_days_ahead_hook(
    pipeline_config: PipelineConfig,
    mock: bool = False,
) -> Callable[[Pipeline], None]:
    """Returns a hook function to validate that the segments data is N days ahead
    the last day of interval to process.

    Returns:
        A callable hook that accepts a :class:`~gfw.common.beam.pipeline.Pipeline`
        instance and performs the validation.
    """

    def _hook(p: Pipeline) -> None:
        """Ensures segments table has enough future data so good_seg2 can be trusted.

        Raises:
            RuntimeError: if the table is not sufficiently stabilized.
        """
        n_days = pipeline_config.good_seg_stabilization_days
        end_date = pipeline_config.end_date
        segments_table = pipeline_config.bq_input_segments

        query = N_DAYS_AHEAD_QUERY.format(table=segments_table)
        client_factory = BigQueryHelper.get_client_factory(mocked=mock)
        bq_client = BigQueryHelper(client_factory=client_factory, project=p.cloud_options.project)

        logger.info(f"Validating {segments_table} table is {n_days} days ahead...")

        query_result = bq_client.run_query(query, query_parameters=[
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
                bigquery.ScalarQueryParameter("n_days", "INT64", n_days),
            ]
        )

        # TODO: improve query_result interface so tolist() method doesn't return dictionaries.
        result = list(query_result.query_job.result())[0]

        if not result.is_ready:
            raise InssuficientSegmentsDataError(
                f"segs_activity not stabilized.\n"
                f"max_day: {result.max_day}\n"
                f"required_day: {result.required_day}\n"
                f"end_date: {end_date}\n"
                f"stabilization_days: {n_days}"
            )

        logger.info("Done.")

    return _hook
