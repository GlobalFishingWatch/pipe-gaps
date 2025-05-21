from datetime import datetime

import pytest
from google.cloud import bigquery

from pipe_gaps import bq_client
from pipe_gaps.utils import mocks
from pipe_gaps.queries import AISMessagesQuery


def test_build(monkeypatch):
    # Test build as if we were using the real client.
    monkeypatch.setattr(bigquery, "Client", mocks.BigQueryClientMock)
    client = bq_client.BigQueryClient.build(project="")

    # Test build with explicit mock.
    client = bq_client.BigQueryClient.build(project="", mock_client=True)

    # Test without ssvids filter.
    start_date = datetime(2024, 1, 1).date()
    end_date = start_date

    res = client.run_query(query=AISMessagesQuery(start_date=start_date, end_date=end_date))
    assert isinstance(res.tolist(), list)
    assert isinstance(next(res), dict)

    with pytest.raises(bq_client.QueryError):
        client = bq_client.BigQueryClient(client=mocks.BigQueryClientMock(project="", fail=True))
        client.run_query(query=AISMessagesQuery(start_date=start_date, end_date=end_date))
