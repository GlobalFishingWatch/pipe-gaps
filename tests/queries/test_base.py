import pytest
from datetime import datetime

from pipe_gaps import queries


def test_get_query():
    start_date = datetime(2024, 1, 1).date()

    with pytest.raises(NotImplementedError):
        query = queries.get_query(query_name="dummy", query_params={})

    with pytest.raises(NotImplementedError):
        query = queries.get_query(query_name="dummy", query_params={})

    query_params = dict(start_date=start_date, end_date=start_date)
    query = queries.get_query(query_name="messages", query_params=query_params)
    query.render()
