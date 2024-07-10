from datetime import datetime

import pytest
from google.cloud import bigquery

from pipe_gaps import queries
from pipe_gaps.utils import mocks


def test_ais_position_messages_query(monkeypatch):
    # Test build as if we were using the real client.
    monkeypatch.setattr(bigquery, "Client", mocks.BigQueryClientMock)
    query = queries.AISMessagesQuery.build(project="")

    # Test build with explicit mock.
    query = queries.AISMessagesQuery.build(project="", mock_client=True)

    # Test without ssvids filter.
    start_date = datetime(2024, 1, 1).date()
    end_date = start_date
    query_result = query.run(start_date=start_date, end_date=end_date)
    assert len(query_result.tolist()) == 1

    # With ssvids filter.
    query.run(start_date=start_date, end_date=end_date, ssvids=["1234"])

    with pytest.raises(queries.AISMessagesQueryError):
        query = queries.AISMessagesQuery.build(project=None, mock_client=True)
        query.run(start_date=start_date, end_date=end_date)
