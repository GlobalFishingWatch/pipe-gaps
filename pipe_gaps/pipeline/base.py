from __future__ import annotations

import os
import logging

from abc import ABC, abstractmethod
from dataclasses import dataclass, fields

# from typing import Unpack  # Supported from python 3.11

from pipe_gaps.pipeline.config import Config

logger = logging.getLogger(__name__)


class PipelineError(Exception):
    pass


class NoInputsFound(PipelineError):
    pass


class Pipeline(ABC):
    """Base class for pipelines."""

    #  def build(cls, **kwargs: Unpack[Config]) -> Pipeline:  # Supported from python 3.11
    @classmethod
    def build(cls, **kwargs: dict) -> Pipeline:
        """Builds a Pipeline instance.

        Args:
            **kwargs: keyword arguments for Config class.

        Returns:
            Pipeline: the built instance.
        """
        config = Config(**kwargs)
        config.validate()

        logger.info("Using following configuration: ")
        logger.info(config.to_json())

        logger.info("Creating working directory {}...".format(config.work_dir.resolve()))
        os.makedirs(config.work_dir, exist_ok=True)

        return cls._build(config)

    @abstractmethod
    def run(self):
        """Runs the pipeline."""

    @classmethod
    def _build(cls, config: Config = Config()):
        raise NotImplementedError(
            "You can't call directly build method from base class. Use one of its subclasses."
        )


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
