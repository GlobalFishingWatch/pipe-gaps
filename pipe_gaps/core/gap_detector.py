"""This module encapsulates the gap detection core algorithm."""
import logging
import operator
from datetime import timedelta

from typing import Union

from rich.progress import track

logger = logging.getLogger(__name__)


THRESHOLD = timedelta(hours=12, minutes=0, seconds=0)
PROGRESS_BAR_DESCRIPTION = "Detecting gaps:"
KEY_TIMESTAMP = "timestamp"
KEY_DISTANCE_FROM_SHORE = "distance_from_shore_m"


class GapDetectionError(Exception):
    pass


def mandatory_keys():
    return [KEY_TIMESTAMP, KEY_DISTANCE_FROM_SHORE]


def detect(
    messages: list[dict],
    threshold: Union[int, float, timedelta] = THRESHOLD,
    show_progress: bool = False,
) -> list[dict]:
    """Detects time gaps between AIS position messages.

    Currently takes (1.75 ± 0.01) seconds to process 10M messages (i7-1355U 5.0GHz).

    Args:
        messages: List of AIS messages.
        threshold: Any gap whose (end-start) is less than this threshold is discarded.
            Can be an int or float number indicating the amount of hours, or a timedelta object.
        show_progress: If True, renders a progress bar.

    Returns:
        List of gaps. A gap object is a dictionary with form:
            {
                "OFF": <the AIS position message when the gap starts>
                "ON": <the AIS position message when the gap ends>
            }

    Raises:
        GapDetectionError: When input messages are missing a mandatory key.
    """

    if isinstance(threshold, (int, float)):
        threshold = timedelta(hours=threshold)

    logger.debug("Using threshold: {}".format(threshold))
    try:
        logger.debug("Sorting messages by timestamp...")
        timestamp_key = operator.itemgetter(KEY_TIMESTAMP)

        messages_sorted = sorted(messages, key=timestamp_key)
        threshold_in_seconds = threshold.total_seconds()
        gaps = zip(messages_sorted[:-1], messages_sorted[1:])

        if show_progress:
            gaps = _build_progress_bar(gaps, len(messages_sorted) - 1)

        logger.debug("Detecting gaps...")
        gaps = list(
            dict(OFF=start, ON=end)
            for start, end in gaps
            if _filter_condition((start, end), threshold_in_seconds)
        )
    except KeyError as e:
        raise GapDetectionError("Missing key in input messages: '{}'".format(e.args[0]))

    return gaps


def _build_progress_bar(gaps, total):
    return track(gaps, total=total, description=PROGRESS_BAR_DESCRIPTION)


def _filter_condition(gap: tuple[dict, dict], threshold: float) -> bool:
    on_distance_from_shore = gap[0][KEY_DISTANCE_FROM_SHORE]
    off_distance_from_shore = gap[1][KEY_DISTANCE_FROM_SHORE]

    return (
        (gap[1][KEY_TIMESTAMP] - gap[0][KEY_TIMESTAMP]) > threshold
        and (on_distance_from_shore is None or on_distance_from_shore > 0)
        and (off_distance_from_shore is None or off_distance_from_shore > 0)
    )
