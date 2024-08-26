"""Module with Core beam transform, a unique step between Sources and Sinks."""

import logging
import apache_beam as beam

from pipe_gaps.pipeline import CoreProcess

logger = logging.getLogger(__name__)


class Core(beam.PTransform):
    def __init__(self, core_process: CoreProcess, side_inputs=None):
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
        self._process = core_process
        self._side_inputs = side_inputs

    def set_side_inputs(self, side_inputs):
        self._side_inputs = side_inputs

    def expand(self, pcoll):
        groups = pcoll | self.group_by()

        outputs = (
            groups | self.process_groups(),
            groups | self.process_boundaries(),
        )

        return outputs | self.join_outputs()

    def group_by(self):
        """Returns the GroupBy pTransform."""
        groups_key = self._process.groups_key()
        return f"GroupBy{groups_key.__name__}" >> beam.GroupBy(groups_key.from_dict)

    def process_groups(self):
        """Returns the ProcessGroups pTransform."""
        return "ProcessGroups" >> beam.FlatMap(self._process.process_group)

    def process_boundaries(self):
        """Returns the ProcessBoundaries pTransform."""
        return "ProcessBoundaries" >> self._process_boundaries()

    def join_outputs(self):
        """Returns the JoinOutputs pTransform."""
        return "JoinOutputs" >> beam.Flatten().with_output_types(self._process.type())

    def _process_boundaries(self):
        side_inputs = None
        if self._side_inputs is not None:
            side_inputs = beam.pvalue.AsList(self._side_inputs)

        return (
            beam.Map(self._process.get_group_boundary)
            | beam.GroupBy(self._process.boundaries_key().from_dict)
            | beam.FlatMap(self._process.process_boundaries, side_inputs=side_inputs)
        )
