from __future__ import annotations

import os
import logging

from pathlib import Path

from abc import ABC, abstractmethod
from dataclasses import field

from pydantic import BaseModel

from pipe_gaps import constants as ct


logger = logging.getLogger(__name__)


class PipelineError(Exception):
    pass


class NoMessagesFound(PipelineError):
    pass


class ConfigError(PipelineError):
    pass


class Config(BaseModel):
    """Encapsulates pipeline configuration.

    Args:
        input_file: Input file to process.
        query_params: Query parameters. Ignored if input_file exists.
        mock_db_client: If True, mocks the DB client. Useful for development and testing.
        work_dir: Working directory to use for saving outputs.
        save_json: If True, saves the results in JSON file.
        save_stats: If True, computes and saves basic statistics.
        core: Extra arguments for the core process.
    """

    input_file: Path = None
    work_dir: Path = Path(ct.WORK_DIR)
    mock_db_client: bool = False
    save_json: bool = False
    save_stats: bool = False
    query_params: dict = None
    core: dict = field(default_factory=dict)
    options: dict = field(default_factory=dict)

    def validate(self):
        if self.input_file is None and self.query_params is None:
            raise ConfigError("You need to provide input_file OR query parameters.")

        if self.input_file is None:
            self.validate_query_params(**self.query_params)

    def to_json(self):
        return self.model_dump_json(indent=4)

    @staticmethod
    def validate_query_params(start_date=None, end_date=None, ssvids=None):
        if start_date is None or end_date is None:
            raise ConfigError("You need to provide both start_date and end_date parameters.")


class Pipeline(ABC):
    """Base class for pipelines."""

    @classmethod
    def build(cls, **kwargs) -> Pipeline:
        """Builds a Pipeline instance.

        Args:
            **kwargs: arguments for Config class.

        Returns:
            Pipeline: Description
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
        raise NotImplementedError("Implement this in subclass.")
