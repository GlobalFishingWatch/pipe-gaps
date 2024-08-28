from datetime import datetime
from apache_beam.testing.test_pipeline import TestPipeline

from pipe_gaps.pipeline.beam.transforms import ReadFromQuery


def test_read_from_query(messages):
    query_params = dict(
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2024, 1, 1)
    )

    tr = ReadFromQuery.build(query_name="messages", query_params=query_params, mock_db_client=True)

    with TestPipeline() as p:
        p | tr
