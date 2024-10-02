import logging
import itertools

from abc import ABC, abstractmethod
from typing import Type, Iterable, Optional, Any

logger = logging.getLogger(__name__)


class Key(ABC):
    """Defines a key function to order or group elements by processing units."""

    def name(self):
        return self.__class__.__name__

    @classmethod
    def format(cls, key):
        if not isinstance(key, list):
            key = [key]

        return "({})".format(
            ', '.join([f'{k}={v}' for k, v in zip(cls.keynames(), key)])
        )

    @staticmethod
    @abstractmethod
    def func():
        """Returns callable to obtain keys to group by."""


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

        logger.info(f"Grouping inputs by {self.groups_key().name()}...")
        grouped_messages = [
            (k, list(v))
            for k, v in itertools.groupby(sorted_messages, key=self.groups_key().func())
        ]

        side_inputs_dict = {}
        if side_inputs is not None:
            grouped_side_inputs = itertools.groupby(side_inputs, key=self.boundaries_key().func())
            for k, group in grouped_side_inputs:
                side_inputs_dict[k] = list(group)

        logger.info("Processing groups...")
        outputs = []
        for key, messages in grouped_messages:
            outputs_in_groups = self.process_group((key, messages))
            outputs.extend(outputs_in_groups)

        logger.info("Processing boundaries...")
        boundaries = [self.get_group_boundary(g) for g in grouped_messages]

        grouped_boundaries = itertools.groupby(boundaries, key=self.boundaries_key().func())
        for k, v in grouped_boundaries:
            outputs_in_boundaries = self.process_boundaries((k, v), side_inputs=side_inputs_dict)
            outputs.extend(outputs_in_boundaries)

        return outputs

    @abstractmethod
    def process_group(self, group: tuple[Key, Iterable]) -> Iterable:
        """Receives elements inside a group (grouped by groups_key) and process them."""

    @abstractmethod
    def get_group_boundary(self, group: tuple[Key, Iterable]) -> Any:
        """Receives elements inside a group (grouped by groups_key)
            and returns the group's boundary elements."""

    @abstractmethod
    def process_boundaries(
        self, group: tuple[Key, Iterable], side_inputs: Optional[Iterable]
    ) -> Iterable:
        """Receives a group of boundary elements (grouped by boundaries_key) and process them."""

    @staticmethod
    def output_type():
        """Returns the final output type of the process."""
        return dict

    @staticmethod
    @abstractmethod
    def groups_key() -> Type[Key]:
        """Returns the key to group by input items."""

    @staticmethod
    @abstractmethod
    def boundaries_key() -> Type[Key]:
        """Returns the key to group boundary elements."""
