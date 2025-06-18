"""Pacakge with sample JSON data."""

from importlib_resources import files

from gfw.common.io import json_load


def sample_messages_path():
    """Returns path to sample input messages."""
    return files("pipe_gaps.data").joinpath("sample_messages.json")


def get_sample_messages() -> dict:
    """Opens sample input messages."""
    return json_load(sample_messages_path())


def sample_open_gaps_path():
    """Returns path to sample input messages."""
    return files("pipe_gaps.data").joinpath("open_gaps.json")


def get_sample_open_gaps() -> dict:
    """Opens sample input messages."""
    return json_load(sample_open_gaps_path())
