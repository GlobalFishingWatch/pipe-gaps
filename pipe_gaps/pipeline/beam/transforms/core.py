"""Module with core PTransform, a unique processing step between sources and sinks."""

import logging

import apache_beam as beam

from pipe_gaps.pipeline.processes import CoreProcess

from pipe_gaps.common.beam.transforms import (
    ApplySlidingWindows,
    GroupBy,
    FilterWindowsByDateRange,
    Conditional
)

from pipe_gaps.pipeline.beam.fns.process_group import ProcessGroup
from pipe_gaps.pipeline.beam.fns.process_boundaries import ProcessBoundaries

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
                to process the union of the boundaries.
        """
        super().__init__()
        self._process = core_process
        self._side_inputs = side_inputs

        self._key = self._process.grouping_key()
        self._period, self._offset = self._process.time_window_period_and_offset()
        self._date_range = self._process._date_range
        self._gd = self._process._gd

        self._window_period_d = self._process._window_period_d
        self._window_offset_h = self._process._window_offset_h
        self._eval_last = self._process._eval_last

    def set_side_inputs(self, side_inputs):
        self._side_inputs = side_inputs

    def expand(self, pcoll):
        process_group = ProcessGroup(
            gap_detector=self._gd,
            key=self._key,
            window_offset_h=self._window_offset_h,
            date_range=self._date_range
        )

        process_boundaries = ProcessBoundaries(
            gap_detector=self._gd,
            key=self._key,
            eval_last=self._eval_last,
            date_range=self._date_range
        )

        # Group pcollection by a configured key and time window.
        groups = (
            pcoll
            | ApplySlidingWindows(period=self._period, offset=self._offset, assign_timestamps=True)
            | GroupBy(self._key, label="Messages")
            | Conditional(
                FilterWindowsByDateRange(self._date_range, offset=self._offset),
                condition=self._date_range is not None
            )
        )

        # Open side inputs if they exist, and grouped them by the same key.
        side_inputs = None
        if self._side_inputs is not None:
            side_inputs = beam.pvalue.AsMultiMap(
                self._side_inputs
                | GroupBy(self._key, label="SideInputs")
            )

        # Process the boundaries of the groups
        output_in_boundaries = (
            groups
            | beam.Map(self._process.get_group_boundary)
            | "GlobalWindow1" >> beam.WindowInto(beam.window.GlobalWindows())
            | GroupBy(self._key, label="Boundaries")
            | beam.ParDo(process_boundaries, side_inputs=side_inputs)
        )

        # Process the interior the groups
        output_in_groups = (
            groups
            | beam.ParDo(process_group)
            | "GlobalWindow2" >> beam.WindowInto(beam.window.GlobalWindows())
        )

        # Join the results from interior and boundaries
        return (
            (output_in_groups, output_in_boundaries)
            | beam.Flatten().with_output_types(self._process.output_type())
        )
