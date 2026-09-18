from importlib.resources import files
from typing import Any, cast

from gfw.common.io import json_load


def get_schema(filename: str) -> list[dict[str, Any]]:
    """Loads a BigQuery schema (a JSON array of field dicts) from `pipe_gaps.assets.schemas`."""
    return cast(
        "list[dict[str, Any]]", json_load(files("pipe_gaps.assets.schemas").joinpath(filename))
    )
