"""This module encapsulates the gap detection core algorithm."""
import logging
import hashlib
import operator
from itertools import islice

from typing import Union, Generator
from datetime import datetime, timedelta, timezone

from rich.progress import track
from geopy.distance import geodesic

from pipe_gaps.utils import pairwise, list_sort

logger = logging.getLogger(__name__)


FACTOR_SECONDS_TO_HOURS = 1 / 3600


def copy_dict_without(dictionary: dict, keys: list) -> dict:
    """Copies a dictionary removing given keys."""
    dictionary = dictionary.copy()
    for k in keys:
        dictionary.pop(k)

    return dictionary


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
    KEY_HOURS_BEFORE_DYN = "positions_hours_before_dyn"
    KEY_RECEIVER_TYPE = "receiver_type"

    KEY_TERRESTRIAL = "terrestrial"
    KEY_SATELLITE = "satellite"
    KEY_DYNAMIC = "dynamic"

    KEY_TOTAL = "total"

    FIELDS_PREFIX_OFF = "start"
    FIELDS_PREFIX_ON = "end"

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

        self._n_seconds_before = self._n_hours_before * 3600

    @classmethod
    def mandatory_keys(cls) -> list[str]:
        """Returns properties that input messages must have."""
        return [
            cls.KEY_SSVID,
            cls.KEY_TIMESTAMP,
            cls.KEY_LON,
            cls.KEY_LAT,
        ]

    @classmethod
    def receiver_type_keys(cls) -> list[str]:
        """Returns valid receiver types."""
        return [
            cls.KEY_TERRESTRIAL,
            cls.KEY_SATELLITE,
            cls.KEY_DYNAMIC
        ]

    @classmethod
    def generate_gap_id(cls, message: dict) -> str:
        s = "{}|{}|{}|{}".format(
            message[cls.KEY_SSVID],
            message[cls.KEY_TIMESTAMP],
            message[cls.KEY_LAT] or 0.0,
            message[cls.KEY_LON] or 0.0
        )

        return hashlib.md5(s.encode('utf-8')).hexdigest()

    def detect(self, messages: list[dict], start_time: datetime = None) -> list[dict]:
        """Detects time gaps between AIS position messages from a single vessel.

        Currently takes (1.75 ± 0.01) seconds to process 10M messages (i7-1355U 5.0GHz).
        TODO: benchmark must be run again with a more representative input (more gaps found).

        Args:
            messages: List of AIS messages.
            start_time: only detect gaps after this time (inclusive). Previous messages
                will be used only to calculate messages N hours before gap.

        Returns:
            List of gaps. Each gap follows the structure documented in the create_gap method.

        Raises:
            GapDetectionError: When input messages are missing a mandatory key.
        """

        logger.debug("Using threshold: {}".format(self._threshold))
        try:
            logger.debug(f"Sorting messages by timestamp ({self._sort_method} algorithm)...")
            self._sort_messages(messages)

            start_idx = 0
            if start_time is not None:
                start_idx = self._get_index_for_start_time(messages, start_time)

            gaps = pairwise(islice(messages, start_idx, None))

            if self._show_progress:
                gaps = self._build_progress_bar(gaps, total=len(messages) - 1 - start_idx)

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

    def eval_open_gap(self, message: dict) -> bool:
        """Evaluates if a message constitutes an open gap.

        The condition to create an open gap is that the time between the message's timestamp
        and end-of-day's timestamp surpasses the configured threshold.

        Args:
            message: position message to evaluate.

        Returns:
            A boolean indicating if the condition for open gap is met.
        """
        last_m_datetime = datetime.utcfromtimestamp(message[self.KEY_TIMESTAMP])
        next_m_date = last_m_datetime.date() + timedelta(days=1)
        next_m_datetime = datetime.combine(next_m_date, datetime.min.time(), tzinfo=timezone.utc)

        next_test_message = {
            self.KEY_TIMESTAMP: next_m_datetime.timestamp(),
        }

        return self._gap_condition(message, next_test_message)

    def create_gap(
        self, off_m: dict, on_m: dict = None, gap_id: str = None, previous_positions: list = ()
    ) -> dict:
        """Creates a gap as a dictionary.

        Args:
            off_m: OFF message. When the AIS reception went OFF.
            on_m: ON message. When the AIS reception went ON. If not provided,
                an open gap will be created.
            gap_id: unique identifier for the gap. If not provided,
                will be generated from [ssvid, timestamp, lat, lon] of the OFF message.
            previous_positions: list of previous positions before the gap begins.
                If provided, will be used when counting positions N hours before gap,
                differentiating satellite and terrestrial receivers.

        Returns:
            The resultant gap. A dictionary containing
                * gap_id
                * gap_ssvid
                * gap_version
                * gap_distance_m
                * gap_duration_h
                * gap_implied_speed_knots
                * gap_start_* (all OFF message fields)
                * gap_end_* (all ON message fields)
                * is_closed
            if the output is normalized.

            If not normalized, the ON/OFF messages will be on its own keys:
                * "OFF": dict with AIS position message when the gap starts.
                * "ON": dict AIS position message when the gap ends.
        """
        ssvid = off_m[self.KEY_SSVID]

        if gap_id is None:
            gap_id = self.generate_gap_id(off_m)

        is_closed = on_m is not None

        distance_m = None
        duration_h = None
        implied_speed_knots = None

        if is_closed:
            distance_m = self._gap_distance_meters(off_m, on_m)
            duration_h = self._gap_duration_seconds(off_m, on_m) * FACTOR_SECONDS_TO_HOURS
            implied_speed_knots = self._gap_implied_speed_knots(distance_m, duration_h)
        else:
            on_m = {k: None for k in off_m}

        gap = dict(
            gap_id=gap_id,
            ssvid=ssvid,
            version=int(datetime.now(tz=timezone.utc).timestamp()),
            distance_m=distance_m,
            duration_h=duration_h,
            implied_speed_knots=implied_speed_knots,
            is_closed=is_closed,
        )

        count = self._count_messages_before_gap(previous_positions)
        gap[self.KEY_HOURS_BEFORE] = count[self.KEY_TOTAL]
        gap[self.KEY_HOURS_BEFORE_TER] = count[self.KEY_TERRESTRIAL]
        gap[self.KEY_HOURS_BEFORE_SAT] = count[self.KEY_SATELLITE]
        gap[self.KEY_HOURS_BEFORE_DYN] = count[self.KEY_DYNAMIC]

        off_m = copy_dict_without(off_m, keys=[self.KEY_SSVID])
        on_m = copy_dict_without(on_m, keys=[self.KEY_SSVID])

        if not self._normalize_output:
            off_on_messages = dict(OFF=off_m, ON=on_m)
        else:
            off_on_messages = self._normalize_off_on_messages(off_m, on_m)

        gap.update(off_on_messages)

        return gap

    # @profile  # noqa  # Uncomment to run memory profiler
    def _sort_messages(self, messages: list) -> None:
        key = operator.itemgetter(self.KEY_TIMESTAMP)
        list_sort(messages, key=key, method=self._sort_method)

    def _get_index_for_start_time(self, messages: list, start_time: datetime) -> Union[int, None]:
        start_timestamp = start_time.timestamp()

        for i, m in enumerate(messages):
            ts = m[self.KEY_TIMESTAMP]
            if ts >= start_timestamp:
                return i

        return None

    def _build_progress_bar(self, gaps: list, total: int) -> Generator:
        return track(gaps, total=total, description=self.PROGRESS_BAR_DESCRIPTION)

    def _previous_positions(self, messages: list, off_m: dict) -> list[dict]:
        end_timestamp = off_m[self.KEY_TIMESTAMP]
        start_timestamp = end_timestamp - self._n_seconds_before

        for m in messages:
            if m[self.KEY_TIMESTAMP] >= start_timestamp and m[self.KEY_TIMESTAMP] < end_timestamp:
                yield m

            if m[self.KEY_TIMESTAMP] >= end_timestamp:
                break

    def _gap_condition(self, off_m: dict, on_m: dict) -> bool:
        return self._gap_duration_seconds(off_m, on_m) > self._threshold

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
        except (TypeError, ZeroDivisionError):
            # Happens when gap_distance_m is NULL.
            # Or gap_duration_h is zero.
            implied_speed_knots = None

        return implied_speed_knots

    def _count_messages_before_gap(self, messages: Generator = ()):
        count = {}

        for k in self.receiver_type_keys():
            count[k] = 0

        for m in messages:
            count[m[self.KEY_RECEIVER_TYPE]] += 1

        count[self.KEY_TOTAL] = sum(count.values())

        return count

    def _normalize_off_on_messages(self, off_m: dict, on_m: dict) -> dict:
        def _normalized_off_on_messages(msg_type: str, m: dict):
            return {f"{msg_type}_{k}": v for k, v in m.items()}

        off_on_messages = {
            **_normalized_off_on_messages(self.FIELDS_PREFIX_OFF, off_m),
            **_normalized_off_on_messages(self.FIELDS_PREFIX_ON, on_m)
        }

        return off_on_messages
