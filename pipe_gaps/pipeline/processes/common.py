"""Module with re-usable subclass implementations."""
import logging
import operator

from datetime import date, datetime, timezone
from dataclasses import dataclass

from .base import Key

logger = logging.getLogger(__name__)


def ts_to_year(ts):
    """Extracts year from unix timestamp.

    This is ~2-times faster than datetime.fromtimestamp(ts, tz=timezone.utc).year,
    but needs further testing/validation.
    """
    return int(ts / 60 / 60 / 24 / 365) + 1970


def ssvid_and_year_key(item):
    return (item["ssvid"], datetime.utcfromtimestamp(item["timestamp"]).year)


def ssvid_and_day_key(item):
    return (
        item["ssvid"],
        datetime.utcfromtimestamp(item["timestamp"]).date().isoformat())


def ssvid_key(item):
    return item["ssvid"]


def ssvid_and_year_key2(item):
    return (item["ssvid"], ts_to_year(item["timestamp"]))


def date_from_year(year):
    return datetime(year=year, month=1, day=1, tzinfo=timezone.utc).date()


def date_from_day(day):
    return date.fromisoformat(day)


class SsvidAndYear(Key):
    @staticmethod
    def keynames():
        return ["SSVID", "YEAR"]

    @staticmethod
    def func():
        return ssvid_and_year_key

    @staticmethod
    def parse_date_func():
        return date_from_year


class SsvidAndDay(Key):
    @staticmethod
    def keynames():
        return ["SSVID", "DAY"]

    @staticmethod
    def func():
        return ssvid_and_day_key

    @staticmethod
    def parse_date_func():
        return date_from_day


class Ssvid(Key):
    @staticmethod
    def keynames():
        return ["SSVID"]

    @staticmethod
    def func():
        return ssvid_key


KEY_CLASSES_MAP = {
    "ssvid_year": SsvidAndYear,
    "ssvid_day": SsvidAndDay,
    "ssvid": Ssvid
}


def key_factory(name, **kwargs):
    if name not in KEY_CLASSES_MAP:
        raise NotImplementedError(f"key with name {name} not implemented")

    return KEY_CLASSES_MAP[name](**kwargs)


class Boundaries:
    """Container for Boundary objects."""
    def __init__(self, boundaries):
        self._boundaries = sorted(boundaries, key=operator.attrgetter("time_interval"))

    def messages(self):
        boundaries_pairs = list(zip(self._boundaries[:-1], self._boundaries[1:]))
        messages_pairs = [[left.end, right.start] for left, right in boundaries_pairs]

        return messages_pairs

    def last_message(self):
        return max(self._boundaries, key=operator.attrgetter("time_interval")).end

    def first_message(self):
        return min(self._boundaries, key=operator.attrgetter("time_interval")).start


@dataclass(eq=True, frozen=True)
class Boundary:
    """Encapsulates first and last AIS position messages for a specific ssvid and time interval."""
    ssvid: str
    time_interval: str
    start: dict
    end: dict

    def __getitem__(self, key):
        return self.__dict__[key]

    @classmethod
    def from_group(cls, group, timestamp_key="timestamp"):
        (ssvid, time_interval), messages = group

        start = min(messages, key=lambda x: x[timestamp_key])
        end = max(messages, key=lambda x: x[timestamp_key])

        return cls(ssvid=ssvid, time_interval=time_interval, start=start, end=end)
