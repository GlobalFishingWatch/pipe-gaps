from typing import Any

from jinja2 import Environment, PackageLoader


class EnvironmentLoader:
    def __init__(self, **defaults: Any) -> None:
        self.defaults = {
            "autoescape": False,
            "trim_blocks": True,
            "lstrip_blocks": True,
        }

        self.defaults.update(defaults)

    def from_package(self, package: str, path: str, **kwargs: Any) -> Environment:
        return Environment(
            loader=PackageLoader(
                package_name=package,
                package_path=path
            ),
            **self.defaults
        )
