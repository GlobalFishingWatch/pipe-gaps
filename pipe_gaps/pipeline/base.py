import os
import json
import logging

from typing import Union
from pathlib import Path
from copy import deepcopy
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, replace

from pipe_gaps import pipeline
from pipe_gaps import constants as ct

logger = logging.getLogger(__name__)


class PipelineError(Exception):
    pass


class NoMessagesFound(PipelineError):
    pass


class ConfigError(PipelineError):
    pass


@dataclass
class Config:
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

    input_file: Union[str, Path] = None
    work_dir: Union[str, Path] = ct.WORK_DIR
    mock_db_client: bool = False
    save_json: bool = False
    save_stats: bool = False
    query_params: dict = None
    core: dict = field(default_factory=dict)
    options: dict = field(default_factory=dict)

    def __post_init__(self):
        if isinstance(self.input_file, str):
            self.input_file = Path(self.input_file)

        self.work_dir = Path(self.work_dir)

    def validate(self):
        if self.input_file is None and self.query_params is None:
            raise ConfigError("You need to provide input_file OR query parameters.")

        if self.input_file is None:
            self.validate_query_params(**self.query_params)

    def to_json(self):
        output = deepcopy(self)
        output.work_dir = str(output.work_dir)
        if output.input_file is not None:
            output.input_file = str(output.input_file)

        return json.dumps(output.__dict__, indent=4)

    @staticmethod
    def validate_query_params(start_date=None, end_date=None, ssvids=None):
        if start_date is None or end_date is None:
            raise ConfigError("You need to provide both start_date and end_date parameters.")


class Pipeline(ABC):
    """Base class for pipelines."""

    @classmethod
    def subclasses_map(cls):
        """Returns name attribute of subclasses."""
        subclasses = {}
        for subclass in cls.__subclasses__():
            subclasses[subclass.name] = subclass

        return subclasses

    @classmethod
    def create(cls, pipe_type="naive", config: Config = Config(), **kwargs):
        """Builds an instance of Pipeline."""
        subclasses = cls.subclasses_map()

        if pipe_type == "beam" and not pipeline.is_beam_installed:
            raise PipelineError("apache-beam not installed.")

        if pipe_type not in subclasses:
            raise PipelineError("Pipeline type '{}' not implemented".format(pipe_type))

        config = replace(config, **kwargs)
        config.validate()

        logger.info("Using following configuration: ")
        logger.info(config.to_json())

        logger.info("Creating working directory {}...".format(config.work_dir.resolve()))
        os.makedirs(config.work_dir, exist_ok=True)

        return subclasses[pipe_type].build(config)

    @abstractmethod
    def build(cls, config: Config = Config(), **kwargs):
        """Builds the pipeline instance."""

    @abstractmethod
    def run(self):
        """Runs the pipeline."""


def run(config: dict):
    """Pipeline entry point."""
    return Pipeline.create(**config).run()
