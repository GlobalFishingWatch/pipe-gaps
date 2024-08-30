import pytest
from pathlib import Path

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline

from pipe_gaps.pipeline.beam.transforms import WriteJson, sinks_factory
from pipe_gaps.pipeline.schemas import Message
from pipe_gaps.utils import json_load


def test_write_json(messages, tmp_path):
    transform = WriteJson(output_dir=tmp_path, output_prefix="test")

    with TestPipeline() as p:
        inputs = p | beam.Create(messages).with_output_types(Message)
        inputs | transform

    output_file = transform.path
    assert Path(output_file).is_file()

    output_messages = json_load(output_file, lines=True)
    assert len(output_messages) == len(messages)


def test_factory(tmp_path):
    with pytest.raises(NotImplementedError):
        sinks_factory("dummy")

    sinks_factory("json", output_dir=tmp_path, output_prefix="path")
