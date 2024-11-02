"""Module with re-usable subclass implementations."""
import logging

from datetime import date, datetime, timezone, timedelta
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
        self._boundaries = sorted(boundaries, key=lambda x: x.first_message()["timestamp"])

    def consecutive_boundaries(self):
        return list(zip(self._boundaries[:-1], self._boundaries[1:]))

    def last_message(self):
        return self._boundaries[-1].last_message()

    def first_message(self):
        return self._boundaries[0].first_message()


@dataclass(eq=True, frozen=True)
class Boundary:
    """Encapsulates first N and last M AIS position messages for an ssvid and time interval.

    Args:
        ssvid: id for the vessel.
        start: first message of the time interval.
        end: last message of the time interval.
    """
    ssvid: str
    start: list
    end: list

    def __getitem__(self, key):
        return self.__dict__[key]

    @classmethod
    def from_group(
        cls, group: tuple, end_h: int = 12, start_h: int = None, timestamp_key="timestamp"
    ):
        """Instantiates a Boundary object from a group.

        Args:
            group: tuple with (key, messages).
            timestamp_key: name for the key containing the message timestamp.
        """
        ssvid, messages = group

        first_msg = min(messages, key=lambda x: x[timestamp_key])
        start = [first_msg]

        last_msg = max(messages, key=lambda x: x[timestamp_key])
        time_n_hours_before_last = last_msg[timestamp_key] - timedelta(hours=end_h).total_seconds()
        end = [m for m in messages if m[timestamp_key] >= time_n_hours_before_last]

        return cls(ssvid=ssvid, start=start, end=end)

    def last_message(self):
        return self.end[-1]

    def first_message(self):
        return self.start[0]
