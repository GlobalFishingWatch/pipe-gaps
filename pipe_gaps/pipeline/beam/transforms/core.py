"""Module with Core beam transform, a unique step between Sources and Sinks."""

import logging
from datetime import timedelta

import apache_beam as beam
from apache_beam.transforms.window import IntervalWindow
from apache_beam.transforms.core import DoFn

from pipe_gaps.pipeline.processes import CoreProcess

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

        self._date_range = core_process.date_range

    def set_side_inputs(self, side_inputs):
        self._side_inputs = side_inputs

    def expand(self, pcoll):
        groups = (
            pcoll
            | self.assign_sliding_windows()
            | self.group_by()
        )

        out_boundaries = groups | self.process_boundaries()

        # if self._date_range is not None:
        #    groups = groups | self.filter_groups()

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
        groups_key = self._process.groups_key()
        return f"GroupBy{groups_key.name()}" >> beam.GroupBy(groups_key.func())

    def filter_groups(self):
        """Returns the FilterGroups pTransform."""
        return "FilterGroups" >> beam.Filter(
            lambda _, window=DoFn.WindowParam: self._is_window_in_range(self._date_range, window))

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
                self._side_inputs | beam.GroupBy(self._process.boundaries_key().func())
            )

        tr = (
            beam.Map(self._process.get_group_boundary)
            | beam.WindowInto(beam.window.GlobalWindows())
            | beam.GroupBy(self._process.boundaries_key().func())
            | beam.FlatMap(self._process.process_boundaries, side_inputs=side_inputs)
        )

        """
        if self._date_range is not None:
            _, offset = self._process.time_window_period_and_offset()
            filter_tr = beam.Filter(
                lambda _,
                window=DoFn.WindowParam: self._is_window_in_range(
                    self._date_range, window, start_buffer=offset))

            tr = filter_tr | tr
        """
        return tr

    def _is_window_in_range(self, date_range, window: IntervalWindow, start_buffer: int = 0):

        window_last_day = window.end.to_utc_datetime(has_tz=True) - timedelta(days=1)
        window_first_day = window.start.to_utc_datetime(has_tz=True)

        start, end = date_range

        first_day_in_range = (start <= window_first_day < end)
        last_day_in_range = (start <= window_last_day < end)

        return first_day_in_range or last_day_in_range
