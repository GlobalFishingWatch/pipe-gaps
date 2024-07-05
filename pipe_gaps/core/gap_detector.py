"""This module encapsulates the gap detection core algorithm."""
import logging
from datetime import timedelta

from rich.progress import track

logger = logging.getLogger(__name__)


THRESHOLD = timedelta(hours=12, minutes=0, seconds=0)
PROGRESS_BAR_DESCRIPTION = "Detecting gaps:"


def detect(
    messages: list[dict], threshold: timedelta = THRESHOLD, show_progress: bool = False
) -> list[tuple[dict, dict]]:
    """Detects time gaps between AIS position messages.

    Currently takes (1.90 ± 0.01) seconds to process 10M messages (i7-1355U 5.0GHz).

    Args:
        messages: List of AIS messages.
        threshold: Any gap whose (end-start) is less than this threshold is discarded.
        show_progress: If true, renders a progress bar.

    Returns:
        list: gaps as 2d-tuple with (start, end) messages.
    """
    n = len(messages)
    logger.debug("Amount of messages: {}".format(n))

    logger.debug("Sorting messages by timestamp...")
    messages_sorted = sorted(messages, key=lambda x: x["timestamp"])

    threshold_in_seconds = threshold.total_seconds()
    gaps = zip(messages_sorted[:-1], messages_sorted[1:])

    if show_progress:
        gaps = _build_progress_bar(gaps, n)

    gaps = tuple(gap for gap in gaps if _filter_condition(gap, threshold_in_seconds))

    logger.debug("Amount of gaps found: {}".format(len(gaps)))

    return gaps


def _build_progress_bar(gaps, total):
    return track(
        gaps, total=total, description=PROGRESS_BAR_DESCRIPTION)


def _filter_condition(gap: tuple[dict, dict], threshold: float) -> bool:
    return (gap[1]["timestamp"]) - gap[0]["timestamp"] > threshold
