"""Module with Core beam transform, a unique step between Sources and Sinks."""

import logging
import apache_beam as beam

from pipe_gaps.pipeline import CoreProcess

logger = logging.getLogger(__name__)


class Core(beam.PTransform):
    def __init__(self, core_process: CoreProcess):
        """A core beam transform for pipelines.

        This pTransform will:
            1. Groups input p-collection by a key defined in core_process.
            2. Process groups in parallel, applying core_process.
            3. Process boundaries in parallel, applying core_process.
            4. Join outputs from groups and boundaries and assigns
                the output schema defined in core_process.

        Args:
            core_process: The class that defines the core process.
        """
        self._core_process = core_process

    def expand(self, pcoll):
        groups = pcoll | self.group_by()

        outputs = (
            groups | self.process_groups(),
            groups | self.process_boundaries(),
        )

        return outputs | self.join_outputs()

    def group_by(self):
        """Returns the GroupBy pTransform."""
        key_class = self._core_process.processing_unit_key()

        return f"GroupBy{key_class.name()}" >> beam.GroupBy(key_class.from_dict)

    def process_groups(self):
        """Returns the ProcessGroups pTransform."""
        return "ProcessGroups" >> beam.FlatMap(self._core_process.process_group)

    def process_boundaries(self):
        """Returns the ProcessBoundaries pTransform."""
        return "ProcessBoundaries" >> (
            beam.Map(self._core_process.get_group_boundaries)
            | beam.GroupBy(self._core_process.boundaries_key)
            | beam.FlatMap(self._core_process.process_boundaries)
        )

    def join_outputs(self):
        """Returns the JoinOutputs pTransform."""
        return "JoinOutputs" >> beam.Flatten().with_output_types(self._core_process.type())
