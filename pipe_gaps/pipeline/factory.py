"""Factory for Pipeline subclasses."""
import logging
from pydantic import BaseModel, ConfigDict

from pipe_gaps import pipeline
from pipe_gaps.pipeline import Pipeline, PipelineError, PipelineConfig


logger = logging.getLogger(__name__)


class PipelineFactoryError(PipelineError):
    pass


class PipelineFactoryConfig(BaseModel):
    model_config = ConfigDict(extra='forbid')

    pipe_type: str = "naive"
    pipe_config: PipelineConfig


def get_subclasses_map():
    """Returns a map of name -> subclass of Pipeline."""
    subclasses = {}
    for subclass in Pipeline.__subclasses__():
        subclasses[subclass.name] = subclass

    return subclasses


def from_config(config: dict):
    config = PipelineFactoryConfig.model_validate(config)

    return create(config.pipe_type, **config.pipe_config.__dict__)


def create(pipe_type: str = "naive", **pipe_config) -> Pipeline:
    """Instantiates a Pipeline subclass.

    Args:
        pipe_type: the pipeline type.
        **pipe_config: arguments for Pipeline.build method.

    Returns:
        The subclass of Pipeline.

    Raises:
        PipelineFactoryError: When
            - pipe_type is not implemented.
            - pipe_type is "beam" and apache-beam is not installed.
    """
    subclasses_map = get_subclasses_map()

    logger.info(f"Using '{pipe_type}' pipeline implementation.")

    if pipe_type == "beam" and not pipeline.is_beam_installed:
        raise PipelineFactoryError("apache-beam not installed.")

    if pipe_type not in subclasses_map:
        raise PipelineFactoryError(
            "Pipeline type '{}' not implemented. Valid pipeline types are {}".format(
                pipe_type, list(subclasses_map.keys())
            )
        )

    return subclasses_map[pipe_type].build(**pipe_config)
