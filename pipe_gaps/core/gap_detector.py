"""This module encapsulates the gap detection core algorithm."""
import logging
import hashlib
import operator

from typing import Union
from datetime import datetime, timedelta, timezone

from rich.progress import track
from geopy.distance import geodesic

from pipe_gaps.utils import pairwise, list_sort

logger = logging.getLogger(__name__)


class GapDetectionError(Exception):
    pass


class GapDetector:
    """Detects time gaps between AIS position messages.

    Args:
        threshold: Any gap whose (end-start) is less than this threshold is discarded.
            Can be an int or float number indicating the amount of hours, or a timedelta object.
        n_hours_before: count positions this amount of hours before each gap.
        show_progress: If True, renders a progress bar.
        sort_method: the algorithm to use when sorting messages. One of ["timsort", "heapsort"].
        normalize_output: If True, normalizes the output.

    """
    THRESHOLD = timedelta(hours=12, minutes=0, seconds=0)
    PROGRESS_BAR_DESCRIPTION = "Detecting gaps:"

    KEY_DISTANCE_FROM_SHORE = "distance_from_shore_m"
    KEY_LAT = "lat"
    KEY_LON = "lon"
    KEY_SSVID = "ssvid"
    KEY_TIMESTAMP = "timestamp"
    KEY_HOURS_BEFORE = "positions_hours_before"
    KEY_HOURS_BEFORE_TER = "positions_hours_before_ter"
    KEY_HOURS_BEFORE_SAT = "positions_hours_before_sat"
    KEY_RECEIVER_TYPE = "receiver_type"

    def __init__(
        self,
        threshold: Union[int, float, timedelta] = THRESHOLD,
        n_hours_before: int = 12,
        show_progress: bool = False,
        sort_method: str = "timsort",
        normalize_output: bool = False
    ):
        if isinstance(threshold, (int, float)):
            threshold = timedelta(hours=threshold)

        self._threshold = threshold.total_seconds()
        self._n_hours_before = n_hours_before
        self._show_progress = show_progress
        self._sort_method = sort_method
        self._normalize_output = normalize_output

        self._creation_time = int(datetime.now(tz=timezone.utc).timestamp())
        self._n_seconds_before = self._n_hours_before * 3600

    @classmethod
    def mandatory_keys(cls):
        """Returns properties that input messages must have."""
        return [
            cls.KEY_TIMESTAMP,
            cls.KEY_LON,
            cls.KEY_LAT,
        ]

    def detect(self, messages: list[dict]) -> list[dict]:
        """Detects time gaps between AIS position messages from a single vessel.

        Currently takes (1.75 ± 0.01) seconds to process 10M messages (i7-1355U 5.0GHz).
        TODO: benchmark must be run again with a more representative input (more gaps found).

        Args:
            messages: List of AIS messages.

        Returns:
            List of gaps. A gap object is a dictionary with form:
                {
                    "OFF": <the AIS position message when the gap starts>
                    "ON": <the AIS position message when the gap ends>
                }

        Raises:
            GapDetectionError: When input messages are missing a mandatory key.
        """

        logger.debug("Using threshold: {}".format(self._threshold))
        try:
            logger.debug(f"Sorting messages by timestamp ({self._sort_method} algorithm)...")
            self._sort_messages(messages)

            gaps = pairwise(messages)

            if self._show_progress:
                gaps = self._build_progress_bar(gaps, total=len(messages) - 1)

            logger.debug("Detecting gaps...")
            gaps = list(
                self.create_gap(
                    start, end, previous_positions=self._previous_positions(messages, start)
                )
                for start, end in gaps
                if self._gap_condition(start, end)
            )
        except KeyError as e:
            raise GapDetectionError("Missing key in input messages: '{}'".format(e.args[0]))

        return gaps

    def eval_open_gap(self, message: dict):
        """Evaluates a single message and returns and open gap."""
        last_m_datetime = datetime.utcfromtimestamp(message[self.KEY_TIMESTAMP])
        next_m_date = last_m_datetime.date() + timedelta(days=1)
        next_m_datetime = datetime.combine(next_m_date, datetime.min.time(), tzinfo=timezone.utc)

        next_test_message = {
            self.KEY_TIMESTAMP: next_m_datetime.timestamp(),
        }

        if self._gap_condition(message, next_test_message):
            return self.create_gap(off_m=message, on_m=None)

        return None

    def create_gap(self, off_m: dict, on_m: dict = None, gap_id=None, previous_positions=None):
        """Creates a gap as a dictionary.

        Args:
            off_m: OFF message. When the AIS reception went OFF.
            on_m: ON message. When the AIS reception went ON. If not provided,
                an open gap will be created.
            gap_id: unique identifier for the gap. If not provided,
                will be generated from [ssvid, timestamp, lat, lon] of the OFF message.
            previous_positions: previous positions N hours before the gap begins.

        Returns:
            The resultant gap. A dictionary containing:
                * gap_id
                * gap_ssvid
                * gap_version
                * gap_distance_m
                * gap_duration_h
                * gap_implied_speed_knots
                * gap_start_* (all OFF message fields)
                * gap_end_* (all ON message fields)
                * is_closed
        """
        ssvid = off_m[self.KEY_SSVID]

        if gap_id is None:
            gap_id = self._generate_gap_id(off_m)

        is_closed = on_m is not None

        distance_m = None
        duration_h = None
        implied_speed_knots = None

        if is_closed:
            distance_m = self._gap_distance_meters(off_m, on_m)
            duration_h = self._gap_duration_seconds(off_m, on_m) / 60 / 60
            implied_speed_knots = self._gap_implied_speed_knots(distance_m, duration_h)

        else:
            on_m = {k: None for k in off_m}

        gap = dict(
            gap_ssvid=ssvid,
            gap_id=gap_id,
            gap_version=self._creation_time,
            gap_distance_m=distance_m,
            gap_duration_h=duration_h,
            gap_implied_speed_knots=implied_speed_knots,
            is_closed=is_closed,
        )

        if previous_positions is not None:
            gap[self.KEY_HOURS_BEFORE] = len(previous_positions)
            gap[self.KEY_HOURS_BEFORE_TER] = self._count_messages_terrestrial(previous_positions)
            gap[self.KEY_HOURS_BEFORE_SAT] = self._count_messages_satellite(previous_positions)

        if not self._normalize_output:
            off_on_messages = dict(
                OFF=off_m,
                ON=on_m
            )
        else:
            def _msg_fields(msg_type, msg):
                return {f"gap_{msg_type}_{k}": v for k, v in msg.items() if k != self.KEY_SSVID}

            off_on_messages = {
                **_msg_fields("start", off_m),
                **_msg_fields("end", on_m)
            }

        return {
            **gap,
            **off_on_messages
        }

    # @profile  # noqa  # Uncomment to run memory profiler
    def _sort_messages(self, messages):
        key = operator.itemgetter(self.KEY_TIMESTAMP)
        list_sort(messages, key=key, method=self._sort_method)

    def _build_progress_bar(self, gaps, total):
        return track(gaps, total=total, description=self.PROGRESS_BAR_DESCRIPTION)

    def _previous_positions(self, messages, off_m):
        end_timestamp = off_m[self.KEY_TIMESTAMP]
        start_timestamp = end_timestamp - self._n_seconds_before

        return [
            m for m in messages
            if m[self.KEY_TIMESTAMP] >= start_timestamp and m[self.KEY_TIMESTAMP] < end_timestamp
        ]

    def _gap_condition(self, off_m: dict, on_m: dict) -> bool:
        return self._gap_duration_seconds(off_m, on_m) > self._threshold

    def _generate_gap_id(self, message: dict):
        s = "{}|{}|{}|{}".format(
            message[self.KEY_SSVID],
            message[self.KEY_TIMESTAMP],
            message[self.KEY_LAT] or 0.0,
            message[self.KEY_LON] or 0.0
        )

        return hashlib.md5(s.encode('utf-8')).hexdigest()

    def _gap_distance_meters(self, off_m: dict, on_m: dict) -> float:
        def _latlon_point(message):
            return (message[self.KEY_LAT], message[self.KEY_LON])

        off_point = _latlon_point(off_m)
        on_point = _latlon_point(on_m)

        try:
            distance = geodesic(off_point, on_point).meters
        except ValueError:  # Happens when any of the coordinates is NULL.
            distance = None

        return distance

    def _gap_duration_seconds(self, off_m: dict, on_m: dict) -> float:
        return on_m[self.KEY_TIMESTAMP] - off_m[self.KEY_TIMESTAMP]

    def _gap_implied_speed_knots(self, gap_distance_m: float, gap_duration_h: float) -> float:
        try:
            implied_speed_knots = (gap_distance_m / gap_duration_h) / 1852
        except TypeError:  # Happens when gap_distance_m is NULL.
            implied_speed_knots = None

        return implied_speed_knots

    def _count_messages_terrestrial(self, messages):
        return self._count_messages_by_receiver_type(messages, "terrestrial")

    def _count_messages_satellite(self, messages):
        return self._count_messages_by_receiver_type(messages, "satellite")

    def _count_messages_by_receiver_type(self, messages, receiver_type):
        return sum(1 for m in messages if m[self.KEY_RECEIVER_TYPE] == receiver_type)
