import logging
import itertools
from collections import namedtuple

from abc import ABC, abstractmethod
from typing import Type, Iterable, Optional, Any

logger = logging.getLogger(__name__)


class CoreProcess(ABC):
    """Base class to define the core step of the pipeline.

    This class provides different abstract methods that help to parallelize the process
        and must be defined in subclasses.
    """

    def process(self, elements: Iterable, side_inputs: Iterable = None) -> Iterable:
        """Receives list of elements, performs group by operation and
        processes groups linearly (without parallelization).

        This method uses subclass implementations of abstract methods.

        Args:
            elements: Main inputs of the process.
            side_inputs: Optional side inputs of the process.

        Returns:
            The iterable of outputs.

        """
        logger.info("Total amount of input elements: {}".format(len(elements)))

        grouping_key = self.grouping_key()

        key_class = namedtuple("Key", grouping_key.keys)

        def grouping_func(x):
            values = [x[k] for k in grouping_key.keys]
            return key_class(*values)

        logger.info("Sorting inputs...")
        sorted_messages = sorted(elements, key=self.sorting_key())
        logger.info(f"Grouping inputs by {grouping_key}...")
        grouped_messages = [
            (k, list(v))
            for k, v in itertools.groupby(sorted_messages, key=grouping_func)
        ]

        side_inputs_dict = {}
        if side_inputs is not None:
            grouped_side_inputs = itertools.groupby(side_inputs, key=grouping_func)
            for k, group in grouped_side_inputs:
                side_inputs_dict[k] = list(group)

        logger.info("Processing groups...")
        outputs = []
        for key, messages in grouped_messages:
            outputs_in_groups = self.process_group((key, messages))
            outputs.extend(outputs_in_groups)

        logger.info("Processing boundaries...")
        boundaries = [self.get_group_boundary(g) for g in grouped_messages]

        grouped_boundaries = itertools.groupby(boundaries, key=grouping_func)
        for k, v in grouped_boundaries:
            outputs_in_boundaries = self.process_boundaries((k, v), side_inputs=side_inputs_dict)
            outputs.extend(outputs_in_boundaries)

        return outputs

    @abstractmethod
    def process_group(self, group: tuple[Any, Iterable]) -> Iterable:
        """Receives elements inside a group (grouped by group_by_key) and process them."""

    @abstractmethod
    def get_group_boundary(self, group: tuple[Any, Iterable]) -> Any:
        """Receives elements inside a group (grouped by group_by_key)
            and returns the group's boundary elements."""

    @abstractmethod
    def process_boundaries(
        self, group: tuple[Any, Iterable], side_inputs: Optional[Iterable]
    ) -> Iterable:
        """Receives a group of boundary elements (grouped by group_by_key) and process them."""

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
