"""Module with beam transforms for processing input pcollections."""

import logging
import apache_beam as beam

from ..fns.base import BaseFn

logger = logging.getLogger(__name__)


class Core(beam.PTransform):
    def __init__(self, core_fn: BaseFn):
        """A core beam transform for pipelines.

        This transform will:
            1. Groups input p-collection by a key defined in core_fn.
            2. Process groups in parallel, applying core_fn.
            3. Ungroups the results and assigns the schema defined in core_fn.

        Args:
            core_fn: The Fn that encapsulates the core transform.
        """
        self._core_fn = core_fn

    def expand(self, pcoll):
        groups = pcoll | "GroupBySsvidAndYear" >> beam.GroupBy(self._core_fn.processing_unit_key)

        interior = (
            groups | "ProcessGroups" >> (
                beam.ParDo(self._core_fn)
                | beam.FlatMapTuple(lambda k, v: v).with_output_types(self._core_fn.type())
            )
        )

        boundaries = (
            groups | "ProcessGroupsBoundaries" >> (
                beam.Map(self._core_fn.get_groups_boundaries)
                | beam.GroupBy(self._core_fn.boundaries_key)
                | beam.Map(self._core_fn.process_groups_boundaries)
                | beam.FlatMap().with_output_types(self._core_fn.type())
            )
        )

        return (interior, boundaries) | "JoinOutputs" >> beam.Flatten()
