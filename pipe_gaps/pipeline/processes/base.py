import logging
import itertools

from abc import ABC, abstractmethod
from typing import Type, Iterable
from dataclasses import dataclass, fields, asdict

logger = logging.getLogger(__name__)


@dataclass(eq=True, frozen=True)
class Key(ABC):
    """Defines a key to order or group elements by processing units."""

    def __str__(self):
        return str(asdict(self))

    @classmethod
    @abstractmethod
    def from_dict(cls, item: dict) -> "Key":
        """Creates an instance from a dictionary."""

    @classmethod
    def attributes(cls) -> list[str]:
        """Returns a list with the names of the attributes in the class."""
        return [x.name for x in fields(cls)]


class CoreProcess(ABC):
    """Base class to define the core step of the pipeline.

    This class provides different abstract methods that help to parallelize the process
        and must be defined in subclasses.
    """

    def process(self, elements: Iterable, side_inputs: Iterable = None) -> Iterable:
        """Receives list of elements and process them linearly, without parallelization.

        This method uses subclass implementations of abstract methods.

        Args:
            elements: Main inputs of the process.
            side_inputs: Optional side inputs of the process.

        Returns:
            The iterable of outputs.

        """
        logger.info("Total amount of input elements: {}".format(len(elements)))

        logger.info("Sorting inputs...")
        sorted_messages = sorted(elements, key=self.sorting_key())

        logger.info(f"Grouping inputs by {self.groups_key().attributes()}...")
        grouped_messages = [
            (k, list(v))
            for k, v in itertools.groupby(sorted_messages, key=self.groups_key().from_dict)
        ]

        logger.info("Processing groups...")
        outputs = []
        for key, messages in grouped_messages:
            outputs_in_groups = self.process_group((key, messages))
            outputs.extend(outputs_in_groups)

        logger.info("Processing boundaries...")
        boundaries = [self.get_group_boundary(g) for g in grouped_messages]

        grouped_boundaries = itertools.groupby(boundaries, key=self.boundaries_key().from_dict)
        for k, v in grouped_boundaries:
            outputs_in_boundaries = self.process_boundaries((k, v), side_inputs=side_inputs)
            outputs.extend(outputs_in_boundaries)

        return outputs

    @abstractmethod
    def process_group(self, group: tuple[Key, Iterable[Type]]) -> Iterable[Type]:
        """Receives elements inside a group (grouped by groups_key) and process them."""

    @abstractmethod
    def get_group_boundary(self, group: tuple[Key, Iterable[Type]]) -> Type:
        """Receives elements inside a group (grouped by groups_key)
            and returns the group's boundary elements."""

    @abstractmethod
    def process_boundaries(self, group: tuple[Key, Iterable[Type]]) -> Iterable[Type]:
        """Receives a group of boundary elements (grouped by boundaries_key) and process them."""

    @staticmethod
    def type() -> Type:
        """Returns the final output type of the process."""

    @staticmethod
    def groups_key() -> Type[Key]:
        """Returns the key to group by input items."""

    @staticmethod
    def boundaries_key() -> Type[Key]:
        """Returns the key to group boundary elements."""
