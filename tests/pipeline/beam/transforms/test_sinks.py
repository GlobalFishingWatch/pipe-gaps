from pathlib import Path

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline

from pipe_gaps.pipeline.beam.transforms import WriteJson
from pipe_gaps.pipeline.schemas import Message
from pipe_gaps.utils import json_load


def test_write_json(messages, tmp_path):
    with TestPipeline() as p:
        inputs = p | beam.Create(messages).with_output_types(Message)
        inputs | WriteJson(output_dir=tmp_path, output_prefix="test")

    output_file = Path(tmp_path).joinpath("test.json")
    assert Path(output_file).is_file()

    output_messages = json_load(output_file, lines=True)
    assert len(output_messages) == len(messages)
