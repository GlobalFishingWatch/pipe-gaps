"""Module with Core beam transform, a unique step between Sources and Sinks."""

import logging

import apache_beam as beam

from pipe_gaps.pipeline.processes import CoreProcess

logger = logging.getLogger(__name__)


class Core(beam.PTransform):
    def __init__(self, core_process: CoreProcess, side_inputs=None):
        """A core beam transform for pipelines.

        This pTransform will:
            1. Groups input p-collection into different closed sets,
                using key and time window defined in core_process.
            2. Process the interior of the sets obtained in 1.
            3. Create boundary objects of each set obtained in 1 and apply a global window.
            4. Process the boundary objects.
            4. Join outputs from interior boundaries and assigns
                the output schema defined in core_process.

        Args:
            core_process: The instance that defines the core process.
            side_inputs: A p-collection with side inputs that will be used for process boundaries.
        """
        self._process = core_process
        self._side_inputs = side_inputs

    def set_side_inputs(self, side_inputs):
        self._side_inputs = side_inputs

    def expand(self, pcoll):
        logger.info(f"Grouping inputs by keys: {self._process.group_by_key()}.")

        groups = (
            pcoll
            | self.assign_sliding_windows()
            | self.group_by()
        )

        out_boundaries = groups | self.process_boundaries()
        out_groups = groups | self.process_groups()

        return (out_groups, out_boundaries) | self.join_outputs()

    def assign_sliding_windows(self):
        """Returns the SlidingWindows pTransform."""
        period, offset = self._process.time_window_period_and_offset()
        size = period + offset

        return "SlidingWindows" >> (
            beam.Map(lambda e: beam.window.TimestampedValue(e, e["timestamp"]))
            | beam.WindowInto(beam.window.SlidingWindows(size=size, period=period, offset=offset))
        )

    def group_by(self):
        """Returns the GroupBy pTransform."""
        key = self._process.group_by_key()
        return f"GroupBy{key.name()}" >> beam.GroupBy(key.func)

    def process_groups(self):
        """Returns the ProcessGroups pTransform."""
        return "ProcessGroups" >> (
            beam.FlatMap(self._process.process_group)
            | beam.WindowInto(beam.window.GlobalWindows())
        )

    def process_boundaries(self):
        """Returns the ProcessBoundaries pTransform."""
        return "ProcessBoundaries" >> self._process_boundaries()

    def join_outputs(self):
        """Returns the JoinOutputs pTransform."""
        return "JoinOutputs" >> beam.Flatten().with_output_types(self._process.output_type())

    def _process_boundaries(self):
        side_inputs = None
        if self._side_inputs is not None:
            side_inputs = beam.pvalue.AsMultiMap(
                self._side_inputs | beam.GroupBy(self._process.group_by_key().func)
            )

        tr = (
            beam.Map(self._process.get_group_boundary)
            | beam.WindowInto(beam.window.GlobalWindows())
            | self.group_by()
            | beam.FlatMap(self._process.process_boundaries, side_inputs=side_inputs)
        )

        return tr
