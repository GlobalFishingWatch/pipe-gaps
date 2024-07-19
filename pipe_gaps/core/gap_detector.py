"""This module encapsulates the gap detection core algorithm."""
import logging
import operator
from datetime import timedelta

from typing import Union

from rich.progress import track

logger = logging.getLogger(__name__)


THRESHOLD = timedelta(hours=12, minutes=0, seconds=0)
PROGRESS_BAR_DESCRIPTION = "Detecting gaps:"
TIMESTAMP_KEY = "timestamp"


def detect(
    messages: list[dict],
    threshold: Union[int, float, timedelta] = THRESHOLD,
    show_progress: bool = False,
) -> dict:
    """Detects time gaps between AIS position messages.

    Currently takes (1.75 ± 0.01) seconds to process 10M messages (i7-1355U 5.0GHz).

    Args:
        messages: List of AIS messages.
        threshold: Any gap whose (end-start) is less than this threshold is discarded.
            Can be hours as a float number or a timedelta object.
        show_progress: If true, renders a progress bar.

    Returns:
        Result of gap detection process.
    """

    if isinstance(threshold, (int, float)):
        threshold = timedelta(hours=threshold)

    logger.debug("Sorting messages by timestamp...")
    timestamp_key = operator.itemgetter(TIMESTAMP_KEY)
    messages_sorted = sorted(messages, key=timestamp_key)

    threshold_in_seconds = threshold.total_seconds()
    gaps = zip(messages_sorted[:-1], messages_sorted[1:])

    if show_progress:
        gaps = _build_progress_bar(gaps, len(messages_sorted))

    logger.debug("Detecting gaps with time diff greater than threshold: {}...".format(threshold))
    gaps = list(
        dict(OFF=start, ON=end)
        for start, end in gaps
        if _filter_condition((start, end), threshold_in_seconds)
    )

    return gaps


def _build_progress_bar(gaps, total):
    return track(gaps, total=total, description=PROGRESS_BAR_DESCRIPTION)


def _filter_condition(gap: tuple[dict, dict], threshold: float) -> bool:
    return (
        (gap[1][TIMESTAMP_KEY] - gap[0][TIMESTAMP_KEY]) > threshold
        and gap[0]["distance_from_shore_m"] > 0
        and gap[1]["distance_from_shore_m"] > 0
    )
