from datetime import datetime

from pipe_gaps import queries


def test_ais_messages_query():
    start_date = datetime(2024, 1, 1).date()

    # Test without ssvids filter.
    query = queries.AISMessagesQuery(start_date=start_date, end_date=start_date)
    query.render()

    # Test with ssvids filter.
    query = queries.AISMessagesQuery(start_date=start_date, end_date=start_date, ssvids=["1234"])
    query.render()

    query.schema()
