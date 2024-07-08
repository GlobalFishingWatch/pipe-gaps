"""Module with useful mocks for testing."""

from dataclasses import dataclass
from datetime import datetime

from google.cloud import bigquery
from google.api_core.exceptions import BadRequest


messages = [{"ssvid": 1234, "timestamp": datetime(2024, 1, 1).timestamp()}]


@dataclass
class _RowIteratorMock:
    def __init__(self, items):
        self.items = items
        self.total_rows = len(self.items)

    def __iter__(self):
        for x in self.items:
            yield x


@dataclass
class _QueryJobMock:
    client: bigquery.Client = None
    fail: bool = False

    def result(self):
        if self.fail:
            raise BadRequest("Bad request")

        return _RowIteratorMock(items=messages)


@dataclass
class BigQueryClientMock:
    project: str
    default_query_job_config: bigquery.QueryJobConfig = None

    def query(self, query):
        if self.project is None:
            return _QueryJobMock(fail=True)

        return _QueryJobMock(client=self)
