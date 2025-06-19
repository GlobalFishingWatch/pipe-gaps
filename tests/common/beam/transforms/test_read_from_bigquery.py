from datetime import datetime
from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline

from pipe_gaps.common.beam.transforms.read_from_bigquery import (
    ReadFromBigQuery, FakeReadFromBigQuery
)
from pipe_gaps.queries import AISMessagesQuery


messages = [
    {
        "ssvid": 1234,
        "timestamp": datetime(2024, 1, 1).timestamp(),
        "distance_from_shore_m": 1
    }
]


def test_read_from_query(messages):
    query_params = dict(
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2024, 1, 1)
    )

    tr = ReadFromBigQuery(
        query=AISMessagesQuery(**query_params),
        read_from_bigquery_factory=FakeReadFromBigQuery
    )

    with _TestPipeline() as p:
        p | tr

    tr = ReadFromBigQuery(
        query=AISMessagesQuery(**query_params),
        read_from_bigquery_factory=FakeReadFromBigQuery,
        elements=messages
    )
    with _TestPipeline() as p:
        p | tr
