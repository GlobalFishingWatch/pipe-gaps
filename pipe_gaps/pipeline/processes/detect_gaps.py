import logging
from datetime import timedelta, date
from typing import Iterable, Optional, Any

from apache_beam.transforms.window import IntervalWindow
from apache_beam.transforms.core import DoFn

from pipe_gaps.core import GapDetector
from pipe_gaps.utils import datetime_from_date, datetime_from_ts
from pipe_gaps.common.key import Key


from .base import CoreProcess
from .common import Boundary, Boundaries


logger = logging.getLogger(__name__)


MAX_WINDOW_PERIOD_D = 180  # Max. window period in days. Requires further testing. Could be higher.


class DetectGapsError(Exception):
    pass


class DetectGaps(CoreProcess):
    """Defines the gap detection process step of the "gaps pipeline".

    Args:
        gd: core gap detector.
        group_by: Operation to use when grouping messages processed by this class.
        eval_last: If True, evaluates last message of each vessel to create an open gap.
        window_period_d: period for the time window in days.
        window_offset_h: offset for the time window in hours.
        date_range: only detect gaps within this date range.
    """

    KEY_TIMESTAMP = GapDetector.KEY_TIMESTAMP
    KEY_SSVID = GapDetector.KEY_SSVID
    KEY_GAP_ID = GapDetector.KEY_GAP_ID

    def __init__(
        self,
        gd: GapDetector,
        grouping_key: Key,
        eval_last: bool = False,
        window_period_d: int = MAX_WINDOW_PERIOD_D,
        window_offset_h: int = 12,
        date_range: tuple[date, date] = None,
    ):
        self._gd = gd
        self._grouping_key = grouping_key
        self._eval_last = eval_last
        self._window_period_d = window_period_d
        self._window_offset_h = window_offset_h
        self._date_range = date_range

    @classmethod
    def build(
        cls,
        date_range: tuple = None,
        eval_last: bool = False,
        window_period_d: int = None,
        window_offset_h: int = 12,
        **config
    ) -> "DetectGaps":
        if date_range is not None:
            date_range = [date.fromisoformat(x) for x in date_range]

        if window_period_d is None:
            window_period_d = MAX_WINDOW_PERIOD_D
            if date_range is not None:
                logger.debug("Window period not provided. Will be adjusted to date range.")
                date_range_size = (date_range[1] - date_range[0]).days
                window_period_d = min(date_range_size, MAX_WINDOW_PERIOD_D)
        else:
            if window_period_d > MAX_WINDOW_PERIOD_D:
                logger.warning(
                    "window period {} surpassed maximum of {}"
                    .format(window_period_d, MAX_WINDOW_PERIOD_D)
                )
                logger.warning("Max value will be used.")
                window_period_d = MAX_WINDOW_PERIOD_D

        logger.debug("Using window period of {} day(s)".format(window_period_d))

        return cls(
            gd=GapDetector(**config),
            grouping_key=Key([cls.KEY_SSVID]),
            eval_last=eval_last,
            window_period_d=window_period_d,
            window_offset_h=window_offset_h,
            date_range=date_range,
        )

    def process_boundaries(
        self,
        group: tuple[Any, Iterable[Boundary]],
        side_inputs: Optional[dict[Any, Iterable]] = None
    ) -> Iterable[dict]:
        key_value, boundaries_it = group

        boundaries = Boundaries(boundaries_it)
        formatted_key = self._grouping_key.format(key_value)

        gaps = {}

        # Step one.
        # If open gap exists, close it.
        open_gap = self._load_open_gap(side_inputs, key_value)
        open_gap_on_m = boundaries.get_first_message_inside_range(self._date_range)

        if open_gap is not None and open_gap_on_m is not None:
            open_gap_id = open_gap[self.KEY_GAP_ID]
            logger.debug(f"Closing existing open gap for {formatted_key}")
            logger.debug(f"{self.KEY_GAP_ID}={open_gap_id}")

            closed_gap = self._close_open_gap(open_gap, open_gap_on_m)
            gaps[closed_gap[self.KEY_GAP_ID]] = closed_gap
            self._debug_gap(closed_gap)

        # Step two:
        # detect potential gap between last message of a group and first message of next group.
        for left, right in boundaries.consecutive_boundaries():
            messages = left.end + right.start

            start_dt = datetime_from_ts(left.last_message()[self.KEY_TIMESTAMP])

            if not self._is_message_in_range(left.last_message()):
                # Otherwise should be an open gap and we handle those in step one.
                continue

            for g in self._gd.detect(messages, start_time=start_dt):
                gaps[g[self.KEY_GAP_ID]] = g
                self._debug_gap(g)

        # Step three:
        # Create open gap if last message of last group met condition.
        if self._eval_last:
            last_boundary = boundaries.last_boundary()
            last_message = last_boundary.last_message()

            last_message_dt = datetime_from_ts(last_message["timestamp"])

            comparison_date = last_message_dt.date()
            if self._date_range is not None:
                comparison_date = self._date_range[1] - timedelta(days=1)

            last_message_in_range = self._is_message_in_range(last_message)
            open_gap_condition_is_met = self._gd.eval_open_gap(last_message, comparison_date)

            logger.debug("Comparison dt: {}".format(comparison_date))
            logger.debug("Last message dt: {}".format(last_message_dt))
            logger.debug("Is in range: {}".format(last_message_in_range))
            logger.debug("Open gap condition: {}".format(open_gap_condition_is_met))

            if last_message_in_range and open_gap_condition_is_met:
                logger.debug(f"Creating new open gap for {formatted_key}...")
                new_open_gap = self._gd.create_gap(
                    off_m=last_message,
                    previous_positions=last_boundary.end[:-1]
                )
                gaps[new_open_gap[self.KEY_GAP_ID]] = new_open_gap
                self._debug_gap(new_open_gap)

        logger.debug(f"Found {len(gaps)} gap(s) for boundaries {formatted_key}...")

        for g in gaps.values():
            yield g

    def get_group_boundary(
        self, group: tuple[Any, Iterable[dict]],
        window: IntervalWindow = DoFn.WindowParam
    ) -> Boundary:
        _, offset = self.time_window_period_and_offset()

        start_time = None
        if isinstance(window, IntervalWindow):
            start_time = window.start.seconds() + offset

        key, messages = group
        messages = list(messages)  # On dataflow, this is a _ConcatSequence object.

        return Boundary.from_group(
            (key.ssvid, messages),
            offset=offset,
            start_time=start_time,
            timestamp_key=self.KEY_TIMESTAMP)

    def sorting_key(self):
        """Callable to use as sorting key."""
        return lambda x: (x[self.KEY_SSVID], x[self.KEY_TIMESTAMP])

    def grouping_key(self) -> Key:
        return self._grouping_key

    def time_window_period_and_offset(self):
        """Returns period and offset for sliding windows in seconds."""
        period_s = self._window_period_d * 24 * 60 * 60
        offset_s = self._window_offset_h * 60 * 60

        return period_s, offset_s

    def _load_open_gap(self, side_inputs, key):
        side_inputs_list = self._load_side_inputs(side_inputs, key)

        if len(side_inputs_list) > 0:
            open_gap = side_inputs_list[0]

            if not isinstance(open_gap, dict):
                # beam.MultiMap encapsulates value in an iterable of iterables (wtf?).
                open_gap = [x for x in open_gap][0]

            return open_gap
        else:
            logger.debug("Open gap was not found for key {}.".format(key))

        return None

    def _load_side_inputs(self, side_inputs, key):
        side_inputs_list = []
        if side_inputs is not None:
            try:
                side_inputs_list = list(side_inputs[key])
            except KeyError:
                logger.debug("Key {} was not found in side inputs.".format(key))

        return side_inputs_list

    def _close_open_gap(self, open_gap, on_m):
        off_m = self._gd.off_message_from_gap(open_gap)

        # Re-order off-message using on-message keys.
        off_m = {k: off_m[k] for k in on_m.keys() if k in off_m}

        return self._gd.create_gap(
            off_m=off_m,
            on_m=on_m,
            gap_id=open_gap[self.KEY_GAP_ID],
            base_gap=open_gap
        )

    def _is_message_in_range(self, message: dict, buffer: bool = True):
        message_ts = message[self.KEY_TIMESTAMP]
        message_dt = datetime_from_ts(message_ts)

        if self._date_range is not None:
            start_dt = datetime_from_date(self._date_range[0])
            if buffer:
                start_dt -= self._gd.min_gap_length

            return message_dt >= start_dt

        return True

    def _debug_gap(self, g: dict):
        end_dt = None

        try:
            start_ts = g["OFF"][self.KEY_TIMESTAMP]
            end_ts = g["ON"][self.KEY_TIMESTAMP]
        except KeyError:
            start_ts = g[f"start_{self.KEY_TIMESTAMP}"]
            end_ts = g[f"end_{self.KEY_TIMESTAMP}"]

        start_dt = datetime_from_ts(start_ts)
        if end_ts is not None:
            end_dt = datetime_from_ts(end_ts)

        logger.debug("----------------------------------")
        logger.debug("Gap OFF: {}".format(start_dt))
        logger.debug("Gap  ON: {}".format(end_dt))
        logger.debug("----------------------------------")
