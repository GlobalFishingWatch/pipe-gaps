"""Module with base classes for pipelines."""
from __future__ import annotations

import os
import logging
from typing import Type
from abc import ABC, abstractmethod
from dataclasses import dataclass, fields

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
    def process(self, items):
        """Receives list of items and process them linearly, without parallelization."""

    @abstractmethod
    def process_group(self, element: tuple, *args, **kwargs) -> list:
        """Receives items inside a group (grouped by groups_key) and process them."""

    @abstractmethod
    def get_group_boundaries(self, element: tuple) -> Type:
        """Receives items inside a group (grouped by groups_key) and returns needed boundaries."""

    @abstractmethod
    def process_boundaries(self, element: tuple) -> list:
        """Receives a group of boundaries (grouped by boundaries_key) and process them."""

    @staticmethod
    def type() -> Type:
        """Returns the final output type of the core process."""

    @staticmethod
    def groups_key() -> Type[ProcessingUnitKey]:
        """Returns the key to group by input items."""

    @staticmethod
    def boundaries_key(item) -> str:
        """Returns the key to  output type of the core process."""


@dataclass(eq=True, frozen=True)
class ProcessingUnitKey(ABC):
    """Defines a key to group inputs by processing units."""

    @classmethod
    @abstractmethod
    def from_dict(cls, item: dict) -> "ProcessingUnitKey":
        """Creates an instance from a dictionary."""

    @classmethod
    def attributes(cls):
        """Returns a list with the names of the attributes in the class."""
        return [x.name for x in fields(cls)]
