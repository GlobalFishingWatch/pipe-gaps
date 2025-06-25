"""
This module encapsulates the core algorithm for detecting time gaps in AIS position messages.

The main class, `GapDetector`, provides functionality to identify intervals
where AIS data reception was interrupted or missing beyond a configurable threshold.

Features:
- Detect gaps between consecutive AIS messages based on time thresholds.
- Support for multiple receiver types: terrestrial, satellite, and dynamic.
- Calculation of gap distance, duration, and implied vessel speed.
- Optional output normalization for easier downstream processing.
- Progress bar visualization for long-running gap detection.

Example usage:

    from datetime import datetime, timedelta
    from gap_detector import GapDetector

    # Example list of AIS messages (each message is a dict with required keys)
    messages = [
        {
            "ssvid": "12345",
            "timestamp": 1687000000,
            "lat": 37.7749,
            "lon": -122.4194,
            "receiver_type": "terrestrial",
        },
        {
            "ssvid": "12345",
            "timestamp": 1687003600,  # 1 hour later
            "lat": 37.7750,
            "lon": -122.4195,
            "receiver_type": "terrestrial",
        },
        # ... more messages ...
    ]

    detector = GapDetector(threshold=timedelta(hours=2))
    gaps = detector.detect(messages)

    for gap in gaps:
        print(gap)

Raises:
    GapDetectionError: When input messages are missing any mandatory key.
"""
import logging
import hashlib
import operator
from itertools import chain, pairwise
from collections import defaultdict

from typing import Union, Generator
from datetime import datetime, date, timedelta, timezone

from rich.progress import track
from geopy.distance import geodesic

from pipe_gaps.common.dictionaries import copy_dict_without
from pipe_gaps.common.iterables import binary_search_first_ge

logger = logging.getLogger(__name__)


FACTOR_SECONDS_TO_HOURS = 1 / 3600


class GapDetectionError(Exception):
    pass


class GapDetector:
    """Detects time gaps between AIS position messages.

    Args:
        threshold:
            Minimum gap duration in hours. Any gap less than this threshold is discarded.
            Can be an int, float number or a timedelta object.

        n_hours_before:
            Count positions this amount of hours before each gap.

        show_progress:
            If True, renders a progress bar.

        normalize_output:
            If True, normalizes the output, i.e., the output is a flatten dictionary
            with all the OFF/ON properties at the same level. If False, the output will contain
            a key for the OFF message and another key for the ON message.
    """
    THRESHOLD = timedelta(hours=12, minutes=0, seconds=0)
    PROGRESS_BAR_DESCRIPTION = "Detecting gaps:"

    KEY_GAP_ID = "gap_id"
    KEY_VERSION = "version"
    KEY_DISTANCE_M = "distance_m"
    KEY_DURATION_H = "duration_h"
    KEY_IMPLIED_SPEED_KNOTS = "implied_speed_knots"
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
    KEY_IS_CLOSED = "is_closed"

    KEY_TERRESTRIAL = "terrestrial"
    KEY_SATELLITE = "satellite"
    KEY_DYNAMIC = "dynamic"

    KEY_TOTAL = "total"

    MESSAGE_PREFIX_OFF = "start_"
    MESSAGE_PREFIX_ON = "end_"

    METERS_PER_NAUTICAL_MILE = 1852

    def __init__(
        self,
        threshold: Union[int, float, timedelta] = THRESHOLD,
        n_hours_before: int = 12,
        show_progress: bool = False,
        normalize_output: bool = False
    ):
        if isinstance(threshold, (int, float)):
            threshold = timedelta(hours=threshold)

        if isinstance(n_hours_before, (int, float)):
            n_hours_before = timedelta(hours=n_hours_before)

        self._threshold_h = threshold
        self._n_hours_before = n_hours_before
        self._show_progress = show_progress
        self._normalize_output = normalize_output

        self._n_seconds_before = self._n_hours_before.total_seconds()
        self._threshold_s = threshold.total_seconds()

    @classmethod
    def mandatory_keys(cls) -> list[str]:
        """Returns properties that input messages must have."""
        return [
            cls.KEY_SSVID,
            cls.KEY_TIMESTAMP,
            cls.KEY_LAT,
            cls.KEY_LON,
            cls.KEY_RECEIVER_TYPE,
        ]

    @classmethod
    def receiver_type_keys(cls) -> list[str]:
        """Returns valid receiver types."""
        return [
            cls.KEY_TERRESTRIAL,
            cls.KEY_SATELLITE,
            cls.KEY_DYNAMIC,
        ]

    @classmethod
    def generate_gap_id(cls, message: dict) -> str:
        s = "{}|{}|{}|{}".format(
            message[cls.KEY_SSVID],
            message[cls.KEY_TIMESTAMP],
            message[cls.KEY_LAT] or 0.0,
            message[cls.KEY_LON] or 0.0,
        )

        return hashlib.md5(s.encode('utf-8')).hexdigest()

    @property
    def min_gap_length(self):
        return self._threshold_h

    def detect(self, messages: list[dict], start_time: datetime = None) -> list[dict]:
        """Detect time gaps between AIS position messages from a single vessel.

            The method automatically sorts the input messages by timestamp before processing.
            Users do not need to provide pre-sorted messages.

            Args:
                messages:
                    List of AIS position messages. Each dict must
                    contain mandatory keys as defined by `mandatory_keys()`.

                start_time:
                    Only detect gaps starting from this time (inclusive).
                    Messages before this time are used solely to count positions preceding a gap.

            Returns:
                List of gap dictionaries with detailed gap information.

            Raises:
                GapDetectionError:
                    If any message is missing a mandatory key.
            """

        try:
            self._sort_messages(messages)

            start_idx = 0
            if start_time is not None:
                start_idx = self._get_index_for_start_time(messages, start_time)
                if start_idx == -1:
                    return []

            gaps = pairwise(messages[start_idx:])

            if self._show_progress and len(messages) - start_idx > 1:
                gaps = self._build_progress_bar(gaps, total=len(messages) - 1 - start_idx)

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

    def eval_open_gap(self, message: dict, today: date = None) -> bool:
        """Evaluates if a message constitutes an open gap.

        The condition to create an open gap is that the time between the message's timestamp
        and end-of-day's timestamp surpasses the configured threshold.

        Args:
            message: Position message to evaluate.
            today: the date representing the current day. Useful when the current day of processing
                is not anymore the date of the message being evaluated.

        Returns:
            A boolean indicating if the condition for open gap is met.
        """
        if today is None:
            today = datetime.fromtimestamp(message[self.KEY_TIMESTAMP],  tz=timezone.utc).date()

        tomorrow = today + timedelta(days=1)
        tomorrow_dt = datetime.combine(tomorrow, datetime.min.time(), tzinfo=timezone.utc)

        next_test_message = {
            self.KEY_TIMESTAMP: tomorrow_dt.timestamp(),
        }

        return self._gap_condition(message, next_test_message)

    def create_gap(
        self, off_m: dict, on_m: dict = None, gap_id: str = None, previous_positions: list = None,
        base_gap: dict = None
    ) -> dict:
        """Creates a gap as a dictionary.

        Args:
            off_m:
                OFF message. When the AIS reception went OFF.

            on_m:
                ON message. When the AIS reception went ON. If not provided,
                an open gap will be created.

            gap_id:
                Unique identifier for the gap. If not provided,
                will be generated from [ssvid, timestamp, lat, lon] of the OFF message.

            previous_positions:
                List of previous positions before the gap begins.
                If provided, will be used when counting positions N hours before gap,
                differentiating satellite, terrestrial and dynamic receivers.

            base_gap:
                Gap to use as base. This gap will be updated with the new properties.
                Useful when you need to reuse properties of an existing gap (like an open gap).

        Returns:
            The resultant gap. A dictionary containing
                * gap_id
                * ssvid
                * version
                * positions_hours_before
                * positions_hours_before_ter
                * positions_hours_before_sat
                * positions_hours_before_dyn
                * distance_m
                * duration_h
                * implied_speed_knots
                * start_* (all OFF message fields)
                * end_* (all ON message fields)
                * is_closed
            if the output is normalized.

            If not normalized, the ON/OFF messages properties will be on its own keys:
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

        gap = {}

        if base_gap is not None:
            gap = base_gap

        if self.KEY_HOURS_BEFORE not in gap and previous_positions is not None:
            previous_positions = chain(previous_positions, [off_m])

            count = self._count_messages_before_gap(previous_positions)
            gap[self.KEY_HOURS_BEFORE] = count[self.KEY_TOTAL]
            gap[self.KEY_HOURS_BEFORE_TER] = count[self.KEY_TERRESTRIAL]
            gap[self.KEY_HOURS_BEFORE_SAT] = count[self.KEY_SATELLITE]
            gap[self.KEY_HOURS_BEFORE_DYN] = count[self.KEY_DYNAMIC]

        gap.update({
            self.KEY_GAP_ID: gap_id,
            self.KEY_SSVID: ssvid,
            self.KEY_VERSION: int(datetime.now(tz=timezone.utc).timestamp()),
            self.KEY_DISTANCE_M: distance_m,
            self.KEY_DURATION_H: duration_h,
            self.KEY_IMPLIED_SPEED_KNOTS: implied_speed_knots,
            self.KEY_IS_CLOSED: is_closed,
        })

        off_m = copy_dict_without(off_m, keys=[self.KEY_SSVID])
        on_m = copy_dict_without(on_m, keys=[self.KEY_SSVID])

        if not self._normalize_output:
            off_on_messages = dict(OFF=off_m, ON=on_m)
        else:
            off_on_messages = self._normalize_off_on_messages(off_m, on_m)

        gap.update(off_on_messages)

        return gap

    def off_message_from_gap(self, gap: dict):
        """Extracts off message from gap object."""

        off_message = {
            key.replace(self.MESSAGE_PREFIX_OFF, ""): v
            for key, v in gap.items()
            if self.MESSAGE_PREFIX_OFF in key
        }

        off_message[self.KEY_SSVID] = gap[self.KEY_SSVID]

        return off_message

    # @profile  # noqa  # Uncomment to run memory profiler
    def _sort_messages(self, messages: list) -> None:
        key = operator.itemgetter(self.KEY_TIMESTAMP)
        messages.sort(key=key)

    def _get_index_for_start_time(self, messages: list, start_time: datetime) -> Union[int, None]:
        return binary_search_first_ge(
            messages,
            start_time.timestamp() if isinstance(start_time, datetime) else start_time,
            key=lambda m: m[self.KEY_TIMESTAMP]
        )

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
        return self._gap_duration_seconds(off_m, on_m) > self._threshold_s

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
            implied_speed_knots = (gap_distance_m / gap_duration_h) / self.METERS_PER_NAUTICAL_MILE
        except (TypeError, ZeroDivisionError):
            # Happens when gap_distance_m is NULL.
            # Or gap_duration_h is zero.
            implied_speed_knots = None

        return implied_speed_knots

    def _count_messages_before_gap(self, messages: Generator = ()):
        count = defaultdict(int)

        for k in self.receiver_type_keys():
            count[k] = 0

        for m in messages:
            count[m[self.KEY_RECEIVER_TYPE]] += 1

        count[self.KEY_TOTAL] = sum(count.values())

        return count

    def _normalize_off_on_messages(self, off_m: dict, on_m: dict) -> dict:
        def _normalized_off_on_messages(msg_prefix: str, m: dict):
            return {f"{msg_prefix}{k}": v for k, v in m.items()}

        off_on_messages = {
            **_normalized_off_on_messages(self.MESSAGE_PREFIX_OFF, off_m),
            **_normalized_off_on_messages(self.MESSAGE_PREFIX_ON, on_m)
        }

        return off_on_messages
