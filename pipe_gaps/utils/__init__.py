"""Utilities package."""
import json
from pathlib import Path
from datetime import date, timezone as tz
from dateutil.parser import parse as dateutil_parse

from .logger import setup_logger
from .timing import timing
from .collections import pairwise, list_sort


__all__ = [  # functions importable directly from package.
    setup_logger, timing, pairwise, list_sort,
]


def json_load(path: Path, lines: bool = False, coder=None) -> dict:
    """Opens JSON file.

    Args:
        path: the filepath.
        lines: If True, treats the file as a JSON Lines.
    """

    if not lines:
        with open(path) as file:
            return json.load(file)

    if coder is None:
        coder = dict

    with open(path, "r") as file:
        return [json.loads(each_line, object_hook=lambda d: coder(**d)) for each_line in file]


def json_save(data: list, path: Path, indent: int = 4, lines: bool = False) -> dict:
    """Writes JSON file.

    Args:
        path: the filepath.
        lines: If True, treats the file as a JSON Lines.
    """

    if not lines:
        with open(path, mode="w") as file:
            return json.dump(data, file, indent=indent)

    with open(path, mode='w') as f:
        for item in data:
            f.write(json.dumps(item) + "\n")


def date_from_string(s, tzinfo=tz.utc) -> date:
    """Parses a string into date object."""
    return dateutil_parse(s).replace(tzinfo=tzinfo).date()
