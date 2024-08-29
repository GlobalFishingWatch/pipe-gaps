"""Module with base classes for pipelines."""
from __future__ import annotations

import os
import logging
from abc import ABC, abstractmethod

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
