"""Module with base classes for pipelines."""
from __future__ import annotations

import os
import logging
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

    @abstractmethod
    def run(self):
        """Runs the pipeline."""

    @classmethod
    def _build(cls, config: PipelineConfig = PipelineConfig()):
        raise NotImplementedError(
            "You can't call directly build method from base class. Use one of its subclasses."
        )


class CoreProcess(ABC):
    """Base class to define the core step of the pipeline."""

    @abstractmethod
    def process(self, elements: list[Type]) -> dict:
        """Receives list of elements and process them linearly, without parallelization."""

    @abstractmethod
    def process_group(self, group: tuple[Key, Iterable[Type]]) -> Iterable[Type]:
        """Receives elements inside a group (grouped by groups_key) and process them."""

    @abstractmethod
    def get_group_boundaries(self, group: tuple[Key, Iterable[Type]]) -> Type:
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
