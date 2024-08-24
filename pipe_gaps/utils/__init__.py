"""Utilities package."""
import json
from datetime import date, timezone as tz

from dateutil.parser import parse as dateutil_parse

from .logger import setup_logger
from .timing import timing
from .collections import pairwise, list_sort


__all__ = [  # functions importable directly from package.
    setup_logger, timing, pairwise, list_sort,
]


def json_load(path, lines=False) -> dict:
    """Opens JSON file.

    Args:
        path: the filepath.
        lines: If True, treats the file as a JSON Lines file.
    """

    if not lines:
        with open(path) as file:
            return json.load(file)

    with open(path, "r") as file:
        return [json.loads(each_line) for each_line in file]


def json_save(data, path, indent=4) -> dict:
    """Writes JSON file."""
    with open(path, mode="w") as file:
        return json.dump(data, file, indent=indent)


def date_from_string(s, tzinfo=tz.utc) -> date:
    """Parses a string into date object."""
    return dateutil_parse(s).replace(tzinfo=tzinfo).date()
