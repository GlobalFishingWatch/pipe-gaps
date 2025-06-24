from dataclasses import dataclass, field, asdict
from typing import Any
from datetime import date


@dataclass
class PipelineConfig:
    """Base config for Beam-based pipelines."""
    date_range: tuple[str, str]
    unknown_parsed_args: dict[str, Any] = field(default_factory=dict)
    unknown_unparsed_args: list[str] = field(default_factory=list)

    @property
    def parsed_date_range(self) -> tuple[date, date]:
        return tuple(map(date.fromisoformat, self.date_range))

    def to_dict(self):
        return asdict(self)
