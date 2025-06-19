"""Utility functions for working with datetime objects and timezones."""

from datetime import datetime, timezone, date


def datetime_from_date(date_: date, tz=timezone.utc):
    """Creates datetime from date, using 00:00:00 hs."""
    return datetime.combine(date_, datetime.min.time(), tzinfo=tz)
