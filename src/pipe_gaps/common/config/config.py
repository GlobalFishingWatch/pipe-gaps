from typing import Any
from datetime import date
from types import SimpleNamespace
from functools import cached_property
from dataclasses import dataclass, field, asdict

from jinja2 import Environment

from pipe_gaps.common.jinja2 import EnvironmentLoader

ERROR_DATE = "Dates must be in ISO format. Got: {}."


class ConfigError(Exception):
    pass


@dataclass
class Config:
    """Configuration object for data pipeline execution."""
    date_range: tuple[str, str]
    unknown_parsed_args: dict[str, Any] = field(default_factory=dict)
    unknown_unparsed_args: list[str] = field(default_factory=list)
    jinja_folder: str = "assets/queries"

    @classmethod
    def from_namespace(cls, ns: SimpleNamespace):
        return cls(**vars(ns))

    @cached_property
    def parsed_date_range(self) -> tuple[date, date]:
        try:
            return tuple(map(date.fromisoformat, self.date_range))
        except ValueError:
            raise ConfigError(ERROR_DATE.format(self.date_range))

    @cached_property
    def top_level_package(self):
        module = self.__class__.__module__
        package = module.split(".")[0]

        return package

    @cached_property
    def jinja_env(self) -> Environment:
        return EnvironmentLoader().from_package(
            package=self.top_level_package,
            path=self.jinja_folder
        )

    @property
    def start_date(self) -> date:
        return self.parsed_date_range[0]

    @property
    def end_date(self) -> date:
        return self.parsed_date_range[1]

    def to_dict(self) -> dict:
        return asdict(self)
