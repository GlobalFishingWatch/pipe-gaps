import timeit
from typing import NamedTuple
from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(eq=True, frozen=True)
class SsvidAndYear:
    ssvid: str
    year: str

    @classmethod
    def from_dict(cls, item: dict) -> "SsvidAndYear":
        return cls(*fromtimestamp(item))


class SsvidAndYear2(NamedTuple):
    ssvid: str
    year: str


def object_from_dict(message):
    return SsvidAndYear.from_dict(message)


def object_constructor(message):
    return SsvidAndYear(*fromtimestamp(message))


def fromtimestamp(message):
    return (
        message["ssvid"],
        datetime.fromtimestamp(message["timestamp"], tz=timezone.utc).year)


def ts_to_year(ts):
    return int(ts / 60 / 60 / 24 / 365) + 1970


def direct_calc(message):
    return (message["ssvid"], ts_to_year(message["timestamp"]))


foo_dt = datetime(2024, 1, 1, 1, 1, 1, tzinfo=timezone.utc)
foo_ts = foo_dt.timestamp()
message = {"ssvid": 123456789, "timestamp": foo_ts}

key1 = fromtimestamp(message)
print(key1)

key2 = direct_calc(message)
print(key2)

setup = """
import math
import time
import timeit
from typing import NamedTuple
from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(eq=True, frozen=True)
class SsvidAndYear:
    ssvid: str
    year: str

    @classmethod
    def from_dict(cls, item: dict) -> "SsvidAndYear":
        return cls(*fromtimestamp(item))


class SsvidAndYear2(NamedTuple):
    ssvid: str
    year: str


def object_from_dict(message):
    return SsvidAndYear.from_dict(message)


def object_constructor(message):
    return SsvidAndYear(*fromtimestamp(message))


def fromtimestamp(message):
    return (
        message["ssvid"],
        datetime.fromtimestamp(message["timestamp"], tz=timezone.utc).year)


def ts_to_year(ts):
    return int(ts / 60 / 60 / 24 / 365) + 1970


def direct_calc(message):
    return (message["ssvid"], ts_to_year(message["timestamp"]))


foo_dt = datetime(2024, 1, 1, 1, 1, 1, tzinfo=timezone.utc)
foo_ts = foo_dt.timestamp()
message = {"ssvid": 123456789, "timestamp": foo_ts}

"""
number = int(1e6)


methods = [
    'object_from_dict(message)',
    'object_constructor(message)',
    'fromtimestamp(message)',
    'direct_calc(message)',
]

for m in methods:
    elapsed = timeit.timeit(m, setup=setup, number=number)
    print("{:<20} {:<20}".format(elapsed, m))
