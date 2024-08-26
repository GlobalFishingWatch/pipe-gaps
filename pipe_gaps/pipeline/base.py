"""Module with base classes for pipelines."""
from __future__ import annotations

import os
import logging
import itertools

from typing import Type, Iterable
from abc import ABC, abstractmethod
from dataclasses import dataclass, fields, asdict

from pipe_gaps.pipeline.config import PipelineConfig

logger = logging.getLogger(__name__)


class PipelineError(Exception):
    pass


class NoInputsFound(PipelineError):
    pass


class Pipeline(ABC):
    """Base class for pipelines."""

    @classmethod
    def build(cls, config: PipelineConfig = PipelineConfig(), **kwargs) -> Pipeline:
        """Builds a Pipeline instance from a config.

        Args:
            config: object with pipeline configuration.
            **kwargs: keyword arguments for PipelineConfig class.

        Returns:
            Pipeline: the built instance.
        """
        config = config.model_copy(update=kwargs)
        config.validate()

        logger.info("Using following configuration: ")
        logger.info(config.to_json())

        logger.info("Creating working directory {}...".format(config.work_dir.resolve()))
        os.makedirs(config.work_dir, exist_ok=True)

        return cls._build(config)

    @property
    def output_path(self):
        if self._output_path is None:
            raise PipelineError("You didn't configured the pipeline to save the output.")

        return self._output_path

    @property
    def output_path_stats(self):
        if self._output_path_stats is None:
            raise PipelineError("You didn't configured the pipeline to save statistics.")

        return self._output_path_stats

    @abstractmethod
    def run(self):
        """Runs the pipeline."""

    @classmethod
    @abstractmethod
    def _build(cls, config: PipelineConfig = PipelineConfig()):
        raise NotImplementedError(
            "You can't call directly build method from base class. Use one of its subclasses."
        )


class CoreProcess(ABC):
    """Base class to define the core step of the pipeline."""

    def process(self, elements: list[Type], side_inputs: list[Type] = None) -> Iterable:
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
