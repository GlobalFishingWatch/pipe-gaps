import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline

from pipe_gaps.pipeline.processes import DetectGaps
from pipe_gaps.pipeline.beam.transforms import Core


def test_core(messages):
    core_process = DetectGaps.build(threshold=1.2)
    transform = Core(core_process=core_process)

    with TestPipeline() as p:
        inputs = p | beam.Create(messages)
        inputs | transform
