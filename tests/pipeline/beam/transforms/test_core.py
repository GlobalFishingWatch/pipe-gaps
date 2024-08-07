import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline

from pipe_gaps.pipeline.beam.fns import DetectGapsFn
from pipe_gaps.pipeline.beam.transforms import Core

from pipe_gaps.pipeline.schemas import Message


def test_core(messages):
    fn = DetectGapsFn(threshold=1.2)
    transform = Core(fn)

    with TestPipeline() as p:
        inputs = p | beam.Create(messages).with_output_types(Message)
        inputs | transform
