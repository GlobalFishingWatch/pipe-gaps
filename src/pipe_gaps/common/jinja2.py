import glob
from typing import Any
from jinja2 import Environment, FileSystemLoader


def create_environment(
    folder_pattern: str = "**/assets/*",
    **kwargs: Any
) -> Environment:
    """Creates a jinja2.environment."""
    template_folders = glob.glob(folder_pattern, recursive=True)

    if not template_folders:
        raise ValueError(
            f"No query templates folder found for pattern: {folder_pattern}")

    defaults = {
        "autoescape": False,
        "trim_blocks": True,
        "lstrip_blocks": True,
    }

    defaults.update(kwargs)

    return Environment(
        loader=FileSystemLoader(searchpath=template_folders),
        **defaults
    )
