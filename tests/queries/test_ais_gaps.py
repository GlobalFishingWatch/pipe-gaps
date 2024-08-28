from datetime import datetime

from pipe_gaps import queries


def test_ais_gaps_query():
    start_date = datetime(2024, 1, 1).date()

    # Test without ssvids filter.
    query = queries.AISGapsQuery(start_date=start_date)
    query.render()

    # Test with ssvids filter.
    query = queries.AISGapsQuery(start_date=start_date, ssvids=["1234"])
    query.render()

    # Test with end_date filter.
    query = queries.AISGapsQuery(start_date=start_date, end_date=start_date)
    query.render()

    query.schema()
