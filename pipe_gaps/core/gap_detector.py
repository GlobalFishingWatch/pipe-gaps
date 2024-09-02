"""This module encapsulates the gap detection core algorithm."""
import logging
import hashlib

from typing import Union
from datetime import datetime, timedelta
from rich.progress import track

from pipe_gaps.utils import pairwise, list_sort

logger = logging.getLogger(__name__)


class GapDetectionError(Exception):
    pass


class GapDetector:
    """Detects time gaps between AIS position messages.

    Args:
        threshold: Any gap whose (end-start) is less than this threshold is discarded.
            Can be an int or float number indicating the amount of hours, or a timedelta object.
        show_progress: If True, renders a progress bar.
        sort_method: the algorithm to use when sorting messages. One of ["timsort", "heapsort"].
        normalize_output: If True, normalizes the output.

    """
    THRESHOLD = timedelta(hours=12, minutes=0, seconds=0)
    PROGRESS_BAR_DESCRIPTION = "Detecting gaps:"
    KEY_TIMESTAMP = "timestamp"
    KEY_DISTANCE_FROM_SHORE = "distance_from_shore_m"

    def __init__(
        self,
        threshold: Union[int, float, timedelta] = THRESHOLD,
        show_progress: bool = False,
        sort_method: str = "timsort",
        normalize_output: bool = False
    ):
        if isinstance(threshold, (int, float)):
            threshold = timedelta(hours=threshold)

        self._threshold = threshold.total_seconds()
        self._show_progress = show_progress
        self._sort_method = sort_method
        self._normalize_output = normalize_output

        self.create_gap = self._create_gap_factory()

    @classmethod
    def mandatory_keys(cls):
        """Returns properties that input messages must have."""
        return [cls.KEY_TIMESTAMP, cls.KEY_DISTANCE_FROM_SHORE]

    def detect(self, messages: list[dict]) -> list[dict]:
        """Detects time gaps between AIS position messages from a single vessel.

        Currently takes (1.75 ± 0.01) seconds to process 10M messages (i7-1355U 5.0GHz).

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
                self.create_gap(start, end)
                for start, end in gaps
                if self._filter_condition(start, end)
            )
        except KeyError as e:
            raise GapDetectionError("Missing key in input messages: '{}'".format(e.args[0]))

        return gaps

    def eval_open_gap(self, message: dict):
        """Evaluates a single message and returns and open gap."""
        last_m_date = datetime.fromtimestamp(message[self.KEY_TIMESTAMP]).date()
        next_m_date = last_m_date + timedelta(days=1)
        next_m_datetime = datetime.combine(next_m_date, datetime.min.time())

        next_test_message = {
            self.KEY_TIMESTAMP: next_m_datetime.timestamp(),
            "distance_from_shore_m": 1,
        }

        if self._filter_condition(message, next_test_message):
            null_msg = {k: None for k in message}
            return self.create_gap(off_m=message, on_m=null_msg, is_closed=False)

        return None

    # @profile  # noqa  # Uncomment to run memory profiler
    def _sort_messages(self, messages):
        list_sort(messages, key=self.KEY_TIMESTAMP, method=self._sort_method)

    def _build_progress_bar(self, gaps, total):
        return track(gaps, total=total, description=self.PROGRESS_BAR_DESCRIPTION)

    def _filter_condition(self, off_m: dict, on_m: dict) -> bool:
        off_distance_from_shore = off_m[self.KEY_DISTANCE_FROM_SHORE]
        on_distance_from_shore = on_m[self.KEY_DISTANCE_FROM_SHORE]

        return (
            (on_m[self.KEY_TIMESTAMP] - off_m[self.KEY_TIMESTAMP]) > self._threshold
            and (on_distance_from_shore is None or on_distance_from_shore > 0)
            and (off_distance_from_shore is None or off_distance_from_shore > 0)
        )

    def _create_gap_factory(self):
        if self._normalize_output:
            return self._create_gap_normalized

        return self._create_gap_unnormalized

    def _create_gap_normalized(self, off_m, on_m, is_closed=True):
        gap_id = self._generate_gap_id(off_m)

        off_m_copy = off_m.copy()
        on_m_copy = on_m.copy()

        ssvid = off_m_copy.pop("ssvid")
        on_m_copy.pop("ssvid")

        gap = dict(gap_id=gap_id, ssvid=ssvid, is_closed=is_closed)

        def _msg_fields(msg_type, msg):
            return {f"gap_{msg_type}_{k}": v for k, v in msg.items()}

        return {
            **gap,
            **_msg_fields("start", off_m_copy),
            **_msg_fields("end", on_m_copy)
        }

    def _create_gap_unnormalized(self, off_m, on_m, is_closed=True):
        gap_id = self._generate_gap_id(off_m)
        return dict(gap_id=gap_id, ssvid=off_m["ssvid"], is_closed=is_closed, OFF=off_m, ON=on_m)

    def _generate_gap_id(self, message):
        s = "{}|{}|{}|{}".format(
            message["ssvid"],
            message["timestamp"],
            message["lat"] or 0.0,
            message["lon"] or 0.0
        )

        return hashlib.md5(s.encode('utf-8')).hexdigest()
