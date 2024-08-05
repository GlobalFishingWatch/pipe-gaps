from abc import ABC, abstractmethod
from datetime import datetime
from dataclasses import dataclass, fields


@dataclass(eq=True, frozen=True)
class ProcessingUnitKey(ABC):
    """Defines a key to group inputs by rocessing units."""

    @classmethod
    @abstractmethod
    def from_dict(cls, item: dict) -> "ProcessingUnitKey":
        """Creates an instance from a dictionary."""

    @classmethod
    def attributes(cls):
        """Returns a list with the names of the attributes in the class."""
        return [x.name for x in fields(cls)]


@dataclass(eq=True, frozen=True)
class SsvidAndYearKey(ProcessingUnitKey):
    ssvid: str
    year: str

    @classmethod
    def from_dict(cls, item: dict) -> "SsvidAndYearKey":
        return cls(ssvid=item["ssvid"], year=str(datetime.fromtimestamp(item["timestamp"]).year))
