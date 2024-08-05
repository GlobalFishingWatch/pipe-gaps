"""Module with beam transforms for processing input pcollections."""
import apache_beam as beam

from ..fns.base import BaseFn


class Core(beam.PTransform):
    def __init__(self, core_fn: BaseFn):
        """A core beam transform for pipelines.

        This transform:
            1. Groups by a parallelization unit defined in Fn object.
            2. Process groups with Fn.
            3. Ungroups the results and assigns the schema defined in Fn object.

        Args:
            core_fn: The Fn that encapsulates the core transform.
        """
        self._core_fn = core_fn

    def expand(self, pcoll):
        return (
            pcoll
            | beam.GroupBy(self._core_fn.parallelization_unit)
            | beam.ParDo(self._core_fn)
            | beam.FlatMapTuple(lambda k, v: v).with_output_types(self._core_fn.type())
        )
