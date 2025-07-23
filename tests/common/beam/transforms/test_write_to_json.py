from pathlib import Path

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline

from pipe_gaps.common.beam.transforms.write_to_json import WriteToJson
from pipe_gaps.queries.ais_messages import AISMessage
from pipe_gaps.common.io import json_load


def test_write_json(messages, tmp_path):
    transform = WriteToJson(output_dir=tmp_path, output_prefix="test")

    with TestPipeline() as p:
        inputs = p | beam.Create(messages).with_output_types(AISMessage)
        inputs | transform

    output_file = transform.path
    assert Path(output_file).is_file()

    output_messages = json_load(output_file, lines=True)
    assert len(output_messages) == len(messages)
