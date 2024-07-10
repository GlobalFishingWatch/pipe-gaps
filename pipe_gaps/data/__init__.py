from importlib_resources import files

from pipe_gaps.utils import json_load


def sample_messages_path():
    """Returns path to sample input messages."""
    return files("pipe_gaps.data").joinpath("sample_messages.json")


def get_sample_messages() -> dict:
    """Opens sample input messages."""
    return json_load(sample_messages_path())
