"""This module encapsulates the gap detection core algorithm."""
import logging
from datetime import timedelta, datetime

from rich.progress import track

logger = logging.getLogger()


THRESHOLD = timedelta(hours=12, minutes=0, seconds=0)
DATE_FORMAT = "%Y-%m-%d %H:%M:%S.%f %Z"


def detect(messages: list[dict], threshold: timedelta = THRESHOLD) -> list[tuple[dict, dict]]:
    """Detects time gaps between AIS position messages.

    Currently takes about 1 minute for 7M messages (i7-1355U 5.0GHz).

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
    gaps = tuple(
        gap for gap in track(gaps, total=n, description="Filtering gaps:")
        if _filter_condition(gap, threshold)
    )

    logger.info("Amount of gaps found: {}".format(len(gaps)))

    return gaps


def _filter_condition(gap: tuple[dict, dict], threshold: timedelta) -> bool:
    start_dt = datetime.strptime(gap[0]["timestamp"], DATE_FORMAT)
    end_dt = datetime.strptime(gap[1]["timestamp"], DATE_FORMAT)

    return (end_dt - start_dt) > threshold
