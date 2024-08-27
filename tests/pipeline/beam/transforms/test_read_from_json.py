from apache_beam.testing.test_pipeline import TestPipeline

from pipe_gaps.data import sample_messages_path

from pipe_gaps.pipeline.beam.transforms import ReadFromJson


def test_detect_gaps():
    path = sample_messages_path()

    with TestPipeline() as p:
        p | ReadFromJson.build(input_file=path, schema="messages")
