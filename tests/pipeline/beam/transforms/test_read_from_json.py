from apache_beam.testing.test_pipeline import TestPipeline

from pipe_gaps.data import sample_messages_path

from pipe_gaps.pipeline.beam.transforms import ReadFromJson
from pipe_gaps.pipeline.schemas import Message


def test_detect_gaps():
    path = sample_messages_path()

    with TestPipeline() as p:
        p | ReadFromJson(input_file=path, schema=Message)
