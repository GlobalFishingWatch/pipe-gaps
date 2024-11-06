"""Module with re-usable subclass implementations."""
import logging

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
        self._boundaries = sorted(boundaries, key=lambda x: x.first_message()["timestamp"])

    def consecutive_boundaries(self):
        return list(zip(self._boundaries[:-1], self._boundaries[1:]))

    def first_boundary(self):
        return self._boundaries[0]

    def last_boundary(self):
        return self._boundaries[-1]

    def first_message(self):
        return self.first_boundary().first_message()

    def last_message(self):
        return self.last_boundary().last_message()


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
        cls, group: tuple, offset: int, start_time: int = None, timestamp_key="timestamp"
    ):
        """Instantiates a Boundary object from a group.

        Args:
            group: tuple with (key, messages).
            timestamp_key: name for the key containing the message timestamp.
        """
        ssvid, messages = group

        messages.sort(key=lambda x: x[timestamp_key])

        first_msg_index = 0
        if start_time is not None:
            first_msg_index = cls.get_index_for_start_time(messages, start_time)
            assert first_msg_index is not None, "first msg index was none."

        start = [messages[first_msg_index]]
        end = cls.get_last_messages(messages, offset)

        return cls(ssvid=ssvid, start=start, end=end)

    @classmethod
    def get_index_for_start_time(cls, messages, start_time):
        # TODO: move to utils. Already implemented in GapDetector.
        for i, m in enumerate(messages):
            if m["timestamp"] >= start_time:
                return i

        return None

    @classmethod
    def get_last_messages(cls, messages, offset=0):
        # We get all messages within a period of time before the last message.

        last_msg_timestamp = messages[-1]["timestamp"]
        n_hours_before = last_msg_timestamp - offset

        i = len(messages) - 1
        for m in reversed(messages):
            if m["timestamp"] == last_msg_timestamp:
                continue

            if m["timestamp"] < n_hours_before:
                break

            i -= 1

        return messages[i:]

    def last_message(self):
        return self.end[-1]

    def first_message(self):
        return self.start[0]
