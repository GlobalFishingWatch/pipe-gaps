"""Utilities package."""
import json
from datetime import date, timezone as tz

from dateutil.parser import parse as dateutil_parse

from .logger import setup_logger
from .timing import timing

__all__ = [setup_logger, timing]  # functions importable directly from package.


def json_load(path, lines=False) -> dict:
    """Opens JSON file."""

    if not lines:
        with open(path) as file:
            return json.load(file)

    with open(path, "r") as f2r:
        return [json.loads(each_line) for each_line in f2r]


def json_save(data, path, indent=4) -> dict:
    """Writes JSON file."""
    with open(path, mode="w") as file:
        return json.dump(data, file, indent=indent)


def date_from_string(s, tzinfo=tz.utc) -> date:
    """Parses a string into date object."""
    return dateutil_parse(s).replace(tzinfo=tzinfo).date()
