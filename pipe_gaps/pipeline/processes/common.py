"""Module with re-usable subclass implementations."""
import logging
from datetime import datetime, timezone
from dataclasses import dataclass

from .base import Key
from pipe_gaps.queries import Message

logger = logging.getLogger(__name__)


def ssvid_and_year_key(item):
    return (str(item["ssvid"]), datetime.fromtimestamp(item["timestamp"], tz=timezone.utc).year)


def ssvid_key(item):
    return str(item["ssvid"])


@dataclass(eq=True, frozen=True)
class SsvidAndYear(Key):
    ssvid: str
    year: str

    @classmethod
    def from_dict(cls, item: dict) -> "SsvidAndYear":
        return cls(*ssvid_and_year_key(item))

    @staticmethod
    def func():
        return ssvid_and_year_key


@dataclass(eq=True, frozen=True)
class Ssvid(Key):
    ssvid: str

    @classmethod
    def from_dict(cls, item: dict) -> "Ssvid":
        return cls(ssvid=str(item["ssvid"]))

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
