"""Module with Core beam transform, a unique step between Sources and Sinks."""

import logging
import apache_beam as beam

from ..fns.base import BaseFn

logger = logging.getLogger(__name__)


class Core(beam.PTransform):
    def __init__(self, core_fn: BaseFn):
        """A core beam transform for pipelines.

        This pTransform will:
            1. Groups input p-collection by a key defined in core_fn.
            2. Process groups in parallel, applying core_fn.
            3. Process boundaries in parallel, applying core_fn.
            4. Join outputs from groups and boundaries and assigns
                the output schema defined in core_fn.

        Args:
            core_fn: The Fn that encapsulates the core transform.
        """
        self._core_fn = core_fn

    def expand(self, pcoll):
        groups = pcoll | self.group_by()

        outputs = (
            groups | self.process_interior(),
            groups | self.process_boundaries(),
        )

        return outputs | self.join_outputs()

    def group_by(self):
        """Returns the GroupBy pTransform."""
        key_class = self._core_fn.processing_unit_key()

        return f"GroupBy{key_class.name()}" >> beam.GroupBy(key_class.from_dict)

    def process_interior(self):
        """Returns the ProcessInterior pTransform."""
        return "ProcessInterior" >> beam.ParDo(self._core_fn.process)

    def process_boundaries(self):
        """Returns the ProcessBoundaries pTransform."""
        return "ProcessBoundaries" >> (
            beam.Map(self._core_fn.get_boundaries)
            | beam.GroupBy(self._core_fn.boundaries_key)
            | beam.ParDo(self._core_fn.process_boundaries)
        )

    def join_outputs(self):
        """Returns the JoinOutputs pTransform."""
        return "JoinOutputs" >> beam.Flatten().with_output_types(self._core_fn.type())
