from pipe_gaps import pipeline
from pipe_gaps.pipeline import Pipeline, PipelineError


class PipelineFactoryError(PipelineError):
    pass


def get_subclasses_map():
    """Returns a map of name -> subclass."""
    subclasses = {}
    for subclass in Pipeline.__subclasses__():
        subclasses[subclass.name] = subclass

    return subclasses


def create(pipe_type="naive", **kwargs) -> Pipeline:
    """Factory for Pipeline subclasses.

    Args:
        pipe_type (str, optional): the pipeline type.
        **kwargs: arguments for build method.

    Returns:
        The subclass of Pipeline.

    Raises:
        PipelineFactoryError: When
            - pipe_type is not implemented.
            - pipe_type is "beam" and apache-beam is not installed.
    """
    subclasses_map = get_subclasses_map()

    if pipe_type == "beam" and not pipeline.is_beam_installed:
        raise PipelineFactoryError("apache-beam not installed.")

    if pipe_type not in subclasses_map:
        raise PipelineFactoryError(
            "Pipeline type '{}' not implemented. Valid pipeline types are {}".format(
                pipe_type, list(subclasses_map.keys())
            )
        )

    return subclasses_map[pipe_type].build(**kwargs)
