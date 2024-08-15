"""Module with re-usable subclass implementations."""
from datetime import datetime
from dataclasses import dataclass

from pipe_gaps.pipeline.base import ProcessingUnitKey


@dataclass(eq=True, frozen=True)
class SsvidAndYearKey(ProcessingUnitKey):
    ssvid: str
    year: str

    @staticmethod
    def name():
        return "SsvidAndYear"

    @classmethod
    def from_dict(cls, item: dict) -> "SsvidAndYearKey":
        return cls(ssvid=item["ssvid"], year=str(datetime.fromtimestamp(item["timestamp"]).year))
