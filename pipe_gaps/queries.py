"""This module encapsulates database queries."""
import logging

from typing import Any
from datetime import date
from dataclasses import dataclass

from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

from pipe_gaps.utils import mocks


logger = logging.getLogger(__name__)


class QueryTemplate:
    AIS_MESSAGES = """
    SELECT
      ssvid,
      seg_id,
      msgid,
      CAST(UNIX_MICROS(timestamp) AS FLOAT64) / 1000000  AS timestamp,
      # lat,
      # lon,
      # course,
      # speed_knots,
      # type,
      # receiver_type,
      # regions,
      # distance_from_shore_m,
      # distance_from_port_m
    FROM
      `{source_messages}`
    WHERE
      (timestamp BETWEEN "{start_date}" AND "{end_date}")
    """


DB_PROJECT = "world-fishing-827"
DB_TABLE_MESSAGES = "pipe_production_v20201001.research_messages"


@dataclass
class AISMessagesQueryResult:
    """Encapsulates the result of AIS messages query.

    Args:
        row_iterator: result of the query.
    """

    row_iterator: bigquery.table.RowIterator

    def __len__(self):
        return self.row_iterator.total_rows

    def __iter__(self):
        for row in self.row_iterator:
            yield dict(row.items())

    def tolist(self):
        return list(self)


class AISMessagesQueryError(Exception):
    pass


class AISMessagesQuery:
    """Encapsulates a query of AIS position messages.

    Args:
        client (Any): any DB client object that has the method "query(query: str)."
    """

    def __init__(self, client: Any):
        self._client = client

    @classmethod
    def build(cls, project: str = DB_PROJECT, mock_client=False, use_cache=False):
        """Builds a AISMessagesQuery object.

        Args:
            project (str, optional): project to use.
            mock_client (bool, optional): if true, use a mocked DB client.
            use_cache (bool, optional): if true, uses cached query.

        Returns:
            The instance of AISMessagesQuery.
        """
        if mock_client:
            return cls(client=mocks.BigQueryClientMock(project=project))

        job_config = bigquery.QueryJobConfig()
        job_config.use_query_cache = use_cache  # Is this working?

        return cls(client=bigquery.Client(project=project, default_query_job_config=job_config))

    def run(self, **query_params) -> AISMessagesQueryResult:
        """Executes the query.

        Args:
            query_params: parameters of render_query method.

        Returns:
            AISMessagesQueryResult: results of the query.

        Raises:
            AISMessagesQueryError: when the query failed for some reason.
        """
        query = self.render_query(**query_params)
        logger.debug("Query for AIS messages: ")
        logger.debug(query)

        try:
            query_job = self._client.query(query)
            row_iterator = query_job.result()
        except BadRequest as e:
            raise AISMessagesQueryError(f"Failed to fetch messages: {e}")

        return AISMessagesQueryResult(row_iterator)

    @staticmethod
    def render_query(start_date: date, end_date: date, ssvids: list = None):
        """Renders AIS messages query.

        Args:
            start_date: start date of query.
            end_date: end date of query.
            ssvids: list of ssvdis to filter.
        """
        query = QueryTemplate.AIS_MESSAGES.format(
            source_messages=DB_TABLE_MESSAGES, start_date=start_date, end_date=end_date
        )

        if ssvids is not None:
            ssvid_filter = ",".join(f'"{s}"' for s in ssvids)
            query = f"{query} AND ssvid IN ({ssvid_filter})"

        return query
