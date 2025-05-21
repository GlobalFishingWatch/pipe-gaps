"""This module wraps the bigquery client."""
import logging

from typing import Any
from dataclasses import dataclass

from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

from pipe_gaps.utils import mocks
from pipe_gaps.queries import Query

logger = logging.getLogger(__name__)


@dataclass
class QueryResult:
    """Encapsulates the result of a BigQuery query.

    Args:
        row_iterator: result of the query.
    """

    row_iterator: bigquery.table.RowIterator

    def __len__(self):
        return self.row_iterator.total_rows

    def __next__(self):
        return dict(next(self.row_iterator).items())

    def __iter__(self):
        for row in self.row_iterator:
            yield dict(row.items())

    def tolist(self):
        return list(self)


class QueryError(Exception):
    pass


class BigQueryClient:
    """Wrapper of bigquery.Client.

    Args:
        client (bigquery.Client): bigquery client object."
    """

    def __init__(self, client: Any):
        self.client = client

    @classmethod
    def build(cls, project: str = None, mock_client=False, use_cache=False):
        """Builds a BigQueryClient object.

        Args:
            project: project to use.
            mock_client: if True, use a mocked DB client.
            use_cache: if True, uses cached query.

        Returns:
            The instance of BigQueryClient.
        """
        if mock_client:
            logger.warning("Using mocked BigQuery client.")
            return cls(client=mocks.BigQueryClientMock(project=project))

        job_config = bigquery.QueryJobConfig()
        job_config.use_query_cache = use_cache  # Is this working?

        return cls(client=bigquery.Client(project=project, default_query_job_config=job_config))

    def run_query(self, query: Query) -> QueryResult:
        """Executes a query.

        Args:
            query: type of query to run.

        Returns:
            QueryResult: results of the query.

        Raises:
            QueryError: when the query failed for some reason.
        """
        query_string = query.render()
        try:
            query_job = self.client.query(query_string)
            row_iterator = query_job.result()
        except BadRequest as e:
            raise QueryError(f"Failed to fetch messages: {e}")

        return QueryResult(row_iterator)

    def create_view(
        self, view_id: str, view_query: str, **kwargs
    ) -> bigquery.Table:
        """Creates a view on a table.

        Args:
            view_id: the destination view: "dataset.view_id".
            view_query: the query to perform to create the view.

        Returns:
            A table object representing the view.
        """
        table_ref = bigquery.table.TableReference.from_string(
            view_id,
            default_project=self.client.project
        )

        view = bigquery.Table(table_ref)
        view.view_query = view_query

        view = self.client.create_table(view, **kwargs)
        logger.debug(f"Created: {view.table_type}: {str(view.reference)}")

        return view
