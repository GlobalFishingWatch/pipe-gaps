"""Module with core PTransform, a unique processing step between sources and sinks."""

import logging

import apache_beam as beam

from pipe_gaps.pipeline.processes import CoreProcess

logger = logging.getLogger(__name__)


class Core(beam.PTransform):
    def __init__(self, core_process: CoreProcess, side_inputs=None):
        """A core PTransform for pipelines.

        This is meant to be a unique processing step between sources and sinks.

        This PTransform will:
            1. Group input PCollection into consecutive closed sets,
                (that may or may not have overlap) using the grouping key
                and time window defined in core_process.
            2. Process the interior of the sets obtained in 1.
            3. Process the union of the boundaries of each pair of consecutive sets.
            4. Join outputs from 2 and 3 and assigns the output schema defined in core_process.

        Args:
            core_process: The instance that defines the core process.
            side_inputs: A PCollection with side inputs that will be used
                to process the unoin of the boundaries.
        """
        self._process = core_process
        self._side_inputs = side_inputs

    def set_side_inputs(self, side_inputs):
        self._side_inputs = side_inputs

    def expand(self, pcoll):
        logger.info(f"Grouping inputs by keys: {self._process.grouping_key()}.")

        groups = pcoll | self.group_by_key_and_timestamp()

        out_boundaries = groups | self.process_boundaries()
        out_groups = groups | self.process_groups()

        return (out_groups, out_boundaries) | self.join_outputs()

    def assign_sliding_windows(self):
        """Returns the SlidingWindows PTransform."""
        period, offset = self._process.time_window_period_and_offset()
        size = period + offset

        return "SlidingWindows" >> (
            beam.Map(lambda e: beam.window.TimestampedValue(e, e["timestamp"]))
            | beam.WindowInto(beam.window.SlidingWindows(size=size, period=period, offset=offset))
        )

    def group_by_key_and_timestamp(self):
        """Returns the GroupByKeyAndTime PTransform."""
        key = self._process.grouping_key()

        return f"GroupBy{key.name()}AndTime" >> (
            self.assign_sliding_windows()
            | self.group_by_key()
        )

    def group_by_key(self):
        """Returns the GroupByKey PTransform."""
        key = self._process.grouping_key()

        return f"GroupBy{key.name()}" >> beam.GroupBy(**key.func)

    def process_groups(self):
        """Returns the ProcessGroups PTransform."""
        return "ProcessGroups" >> (
            beam.FlatMap(self._process.process_group)
            | beam.WindowInto(beam.window.GlobalWindows())
        )

    def process_boundaries(self):
        """Returns the ProcessBoundaries PTransform."""
        return "ProcessBoundaries" >> self._process_boundaries()

    def join_outputs(self):
        """Returns the JoinOutputs PTransform."""
        return "JoinOutputs" >> beam.Flatten().with_output_types(self._process.output_type())

    def _process_boundaries(self):
        side_inputs = None
        if self._side_inputs is not None:
            side_inputs = beam.pvalue.AsMultiMap(
                self._side_inputs | self.group_by_key()
            )

        tr = (
            beam.Map(self._process.get_group_boundary)
            | beam.WindowInto(beam.window.GlobalWindows())
            | self.group_by_key()
            | beam.FlatMap(self._process.process_boundaries, side_inputs=side_inputs)
        )

        return tr
