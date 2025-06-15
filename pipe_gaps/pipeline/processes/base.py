import logging

from abc import ABC, abstractmethod
from typing import Type, Iterable, Any

logger = logging.getLogger(__name__)


class CoreProcess(ABC):
    """Base class to define the core step of the pipeline.

    This class provides different abstract methods that help to parallelize the process
        and must be defined in subclasses.
    """

    @abstractmethod
    def get_group_boundary(self, group: tuple[Any, Iterable]) -> Any:
        """Receives elements inside a group (grouped by group_by_key)
            and returns the group's boundary elements."""

    @staticmethod
    def output_type():
        """Returns the final output type of the process."""
        return dict

    @staticmethod
    @abstractmethod
    def grouping_key() -> Type:
        """Returns the key to use when grouping input items."""

    @staticmethod
    @abstractmethod
    def time_window_period_and_offset() -> tuple[float, float]:
        """Returns the period and offset for time windowing, in seconds."""
