"""This module encapsulates the gap detection core algorithm."""
import logging
from datetime import timedelta

from rich.progress import track

logger = logging.getLogger()


THRESHOLD = timedelta(hours=12, minutes=0, seconds=0)


def detect(messages: list[dict], threshold: timedelta = THRESHOLD) -> list[tuple[dict, dict]]:
    """Detects time gaps between AIS position messages.

    Currently takes about 2 seconds to process 10M messages (i7-1355U 5.0GHz).

    Args:
        messages: List of AIS messages.
        threshold: Any gap whose (end-start) is less than this threshold is discarded.

    Returns:
        list: gaps as 2d-tuple with (start, end) messages.
    """
    n = len(messages)
    logger.info("Amount of messages: {}".format(n))

    logger.info("Sorting messages by timestamp...")
    messages_sorted = sorted(messages, key=lambda x: x["timestamp"])

    logger.info("Detecting gaps...")
    gaps = zip(messages_sorted[:-1], messages_sorted[1:])

    threshold_in_seconds = threshold.total_seconds()
    gaps = tuple(
        gap for gap in track(gaps, total=n, description="Filtering gaps:")
        if _filter_condition(gap, threshold_in_seconds)
    )

    logger.info("Amount of gaps found: {}".format(len(gaps)))

    return gaps


def _filter_condition(gap: tuple[dict, dict], threshold: float) -> bool:
    return (gap[1]["timestamp"]) - gap[0]["timestamp"] > threshold
