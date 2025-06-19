import json
from typing import Callable
from pathlib import Path


def json_load(path: Path, lines: bool = False, coder: Callable = None) -> dict:
    """Opens JSON file.

    Args:
        path:
            The source path.

        lines:
            If True, expects JSON Lines format.

        coder:
            Coder to use when reading JSON records.
    """

    if not lines:
        with open(path) as file:
            return json.load(file)

    if coder is None:
        coder = dict

    with open(path, "r") as file:
        return [json.loads(each_line, object_hook=lambda d: coder(**d)) for each_line in file]


def json_save(path: Path, data: list, indent: int = 4, lines: bool = False) -> Path:
    """Writes JSON file.

    Args:
        path:
            The destination path.

        data:
            List of records to write.

        indent:
            Amount of indentation.

        lines:
            If True, writes in JSON Lines format.
    """

    if not lines:
        with open(path, mode="w") as file:
            json.dump(data, file, indent=indent)
            return path

    with open(path, mode='w') as f:
        for item in data:
            f.write(json.dumps(item) + "\n")

    return path
