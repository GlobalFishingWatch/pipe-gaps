import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline

from pipe_gaps.pipeline.beam.fns import DetectGapsFn
from pipe_gaps.pipeline.schemas import Message


def test_detect_gaps(messages):
    fn = DetectGapsFn(threshold=1.2)

    with TestPipeline() as p:
        inputs = p | beam.Create(messages).with_output_types(Message)
        inputs | beam.GroupBy(fn.parallelization_unit) | beam.ParDo(fn)
