"""Module with core PTransform, a unique processing step between sources and sinks."""

import logging
from datetime import date
from functools import cached_property

import apache_beam as beam

from pipe_gaps.core import GapDetector
from pipe_gaps.common.key import Key

from pipe_gaps.common.beam.transforms import (
    ApplySlidingWindows,
    GroupBy,
    FilterWindowsByDateRange,
    Conditional
)

from pipe_gaps.pipeline.fns.process_group import ProcessGroup
from pipe_gaps.pipeline.fns.process_boundaries import ProcessBoundaries
from pipe_gaps.pipeline.fns.extract_group_boundary import ExtractGroupBoundary


logger = logging.getLogger(__name__)

MAX_WINDOW_PERIOD_D = 180  # Max. window period in days. Requires further testing. Could be higher.


class DetectGaps(beam.PTransform):
    KEY_TIMESTAMP = GapDetector.KEY_TIMESTAMP
    KEY_SSVID = GapDetector.KEY_SSVID
    KEY_GAP_ID = GapDetector.KEY_GAP_ID

    def __init__(
        self,
        gap_detector: GapDetector,
        key: Key = None,
        eval_last: bool = False,
        window_period_d: int = None,
        window_offset_h: int = 12,
        date_range: tuple[str, str] = None,
        side_inputs=None
    ):
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
        self._gap_detector = gap_detector
        self._key = key or Key([self.KEY_SSVID])
        self._eval_last = eval_last
        self._window_period_d = window_period_d
        self._window_offset_h = window_offset_h
        self._date_range = date_range
        self._side_inputs = side_inputs

    def set_side_inputs(self, side_inputs):
        self._side_inputs = side_inputs

    @cached_property
    def date_range(self):
        date_range = None
        if self._date_range is not None:
            date_range = [date.fromisoformat(x) for x in self._date_range]

        return date_range

    @cached_property
    def window_period_d(self):
        window_period_d = self._window_period_d

        if window_period_d is None:
            if self.date_range is not None:
                logger.debug("Window period is None. Will be adjusted to provided date range.")
                date_range_size = (self.date_range[1] - self.date_range[0]).days
                window_period_d = min(date_range_size, MAX_WINDOW_PERIOD_D)
            else:
                window_period_d = MAX_WINDOW_PERIOD_D
        elif window_period_d > MAX_WINDOW_PERIOD_D:
            logger.warning(
                "window period {} surpassed maximum of {}."
                .format(window_period_d, MAX_WINDOW_PERIOD_D)
            )
            logger.warning("Max value will be used.")
            window_period_d = MAX_WINDOW_PERIOD_D

        logger.debug("Using window period of {} day(s)".format(window_period_d))
        return window_period_d

    @cached_property
    def period_s(self):
        return self.window_period_d * 24 * 60 * 60

    @cached_property
    def offset_s(self):
        return self._window_offset_h * 60 * 60

    def expand(self, pcoll):
        process_group = ProcessGroup(
            gap_detector=self._gap_detector,
            key=self._key,
            window_offset_h=self._window_offset_h,
            date_range=self.date_range
        )

        process_boundaries = ProcessBoundaries(
            gap_detector=self._gap_detector,
            key=self._key,
            eval_last=self._eval_last,
            date_range=self.date_range
        )

        extract_group_boundary = ExtractGroupBoundary(window_offset_s=self.offset_s)

        # Group pcollection by a configured key and time window.
        groups = (
            pcoll
            | ApplySlidingWindows(self.period_s, self.offset_s, assign_timestamps=True)
            | GroupBy(self._key, label="Messages")
            | "FilterWindows" >> Conditional(
                FilterWindowsByDateRange(self.date_range, offset=self.offset_s),
                condition=self.date_range is not None
            )
        )

        # Open side inputs if they exist, and grouped them by the same key.
        side_inputs = None
        if self._side_inputs is not None:
            side_inputs = beam.pvalue.AsMultiMap(
                self._side_inputs | GroupBy(self._key, label="OpenGaps")
            )

        # Process the boundaries of the groups
        output_in_boundaries = groups | "ProcessBoundaries" >> (
            beam.ParDo(extract_group_boundary)
            | "GlobalWindow" >> beam.WindowInto(beam.window.GlobalWindows())
            | GroupBy(self._key, label="Boundaries")
            | beam.ParDo(process_boundaries, side_inputs=side_inputs)
        )

        # Process the interior the groups
        output_in_groups = groups | "ProcessGroups" >> (
            beam.ParDo(process_group)
            | "GlobalWindow" >> beam.WindowInto(beam.window.GlobalWindows())
        )

        # Join the results from interior and boundaries
        return (
            (output_in_groups, output_in_boundaries)
            | beam.Flatten().with_output_types(dict)
        )
