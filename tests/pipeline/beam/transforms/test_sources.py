import pytest
from datetime import datetime
from apache_beam.testing.test_pipeline import TestPipeline

from pipe_gaps.common.beam.transforms.read_from_bigquery import ReadFromQuery, ReadFromBigQueryMock
from pipe_gaps.common.beam.transforms.read_from_json import ReadFromJson

from pipe_gaps.data import sample_messages_path


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

    tr = ReadFromQuery.build(query_name="messages", query_params=query_params, mock_db_client=True)

    with TestPipeline() as p:
        p | tr

    mock_transform = ReadFromBigQueryMock(elements=messages)

    tr = ReadFromQuery(mock_transform)
    with TestPipeline() as p:
        p | tr


def test_read_from_json():
    path = sample_messages_path()

    with TestPipeline() as p:
        p | ReadFromJson.build(input_file=path, schema="messages")
