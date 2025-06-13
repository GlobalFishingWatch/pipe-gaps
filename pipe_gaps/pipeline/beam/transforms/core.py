"""Module with core PTransform, a unique processing step between sources and sinks."""

import logging
from datetime import date, timedelta

import apache_beam as beam
from apache_beam.transforms.window import IntervalWindow


from pipe_gaps.pipeline.processes import CoreProcess
from pipe_gaps.common.beam.transforms import ApplySlidingWindows

logger = logging.getLogger(__name__)


def window_intersects_with_range(
    date_range: tuple[date, date], window: IntervalWindow, offset: int = 0
):
    """Checks whether a window is intersecting with a given range."""

    # window_end = window.end.to_utc_datetime(has_tz=True).date() - timedelta(days=1)
    window_start = (window.start.to_utc_datetime(has_tz=True) + timedelta(seconds=offset)).date()

    return window_start < date_range[1]


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

    def group_by_key_and_timestamp(self):
        """Returns the GroupByKeyAndTime PTransform."""
        key = self._process.grouping_key()
        period, offset = self._process.time_window_period_and_offset()
        date_range = self._process._date_range

        tr = (
            ApplySlidingWindows(period=period, offset=offset)
            | self.group_by_key()
        )

        if date_range is not None:
            tr = tr | self._filter_windows_out_of_range(date_range)

        return f"GroupBy{key.name()}AndTime" >> tr

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

    def _filter_windows_out_of_range(self, date_range):
        _, offset = self._process.time_window_period_and_offset()

        return beam.Filter(
            lambda _,
            window=beam.DoFn.WindowParam: window_intersects_with_range(
                date_range, window, offset=offset)
        )
