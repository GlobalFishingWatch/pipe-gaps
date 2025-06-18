from importlib.resources import files

from pipe_gaps.common.io import json_load


def get_schema(filename: str):
    return json_load(files("pipe_gaps.assets.schemas").joinpath("ais-gaps.json"))
