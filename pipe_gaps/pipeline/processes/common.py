"""Module with re-usable subclass implementations."""
import logging
from datetime import datetime, timezone
from dataclasses import dataclass

from .base import Key
from pipe_gaps.queries import Message

logger = logging.getLogger(__name__)


def ts_to_year(ts):
    """Extracts year from unix timestamp.

    This is ~2-times faster than datetime.fromtimestamp(ts, tz=timezone.utc).year,
    but needs further testing/validation.
    """
    return int(ts / 60 / 60 / 24 / 365) + 1970


def ssvid_and_year_key(item):
    return (item["ssvid"], datetime.fromtimestamp(item["timestamp"], tz=timezone.utc).year)


def ssvid_key(item):
    return item["ssvid"]


def ssvid_and_year_key2(item):
    return (item["ssvid"], ts_to_year(item["timestamp"]))


class SsvidAndYear(Key):
    @staticmethod
    def keynames():
        return ["SSVID", "YEAR"]

    @staticmethod
    def func():
        return ssvid_and_year_key


class Ssvid(Key):
    @staticmethod
    def keynames():
        return ["SSVID"]

    @staticmethod
    def func():
        return ssvid_key


@dataclass(eq=True, frozen=True)
class YearBoundary:
    """Defines first and last AIS messages for a specific year and ssvid."""
    ssvid: str
    year: str
    start: Message
    end: Message

    def __getitem__(self, key):
        return self.__dict__[key]

    @classmethod
    def from_group(cls, element, timestamp_key="timestamp"):
        (ssvid, year), messages = element

        start = min(messages, key=lambda x: x[timestamp_key])
        end = max(messages, key=lambda x: x[timestamp_key])

        return cls(ssvid=ssvid, year=year, start=start, end=end)
