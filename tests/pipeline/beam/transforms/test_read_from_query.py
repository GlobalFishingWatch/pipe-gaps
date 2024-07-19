from apache_beam.testing.test_pipeline import TestPipeline

from pipe_gaps.pipeline.beam.transforms import ReadFromQuery
from pipe_gaps.pipeline.schemas import Message


def test_detect_gaps(messages):
    with TestPipeline() as p:
        p | ReadFromQuery(schema=Message, mock_db_client=True)
