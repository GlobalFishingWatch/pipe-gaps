from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline

from pipe_gaps.common.beam.transforms.read_from_json import ReadFromJson
from pipe_gaps.assets import sample_messages_path


def test_read_from_json():
    path = sample_messages_path()

    with _TestPipeline() as p:
        p | ReadFromJson.build(input_file=path, schema="messages")
