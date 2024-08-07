import warnings

from pipe_gaps.pipeline.base import Pipeline, PipelineError, NoInputsFound
from pipe_gaps.pipeline.config import PipelineConfig, PipeConfigError
from pipe_gaps.pipeline.factory import create, PipelineFactoryConfig, PipelineFactoryError
from pipe_gaps.pipeline.pipe_naive import NaivePipeline

__all__ = [
    create,
    PipelineFactoryConfig,
    PipelineFactoryError,
    Pipeline,
    PipelineError,
    NoInputsFound,
    PipelineConfig,
    PipeConfigError,
    NaivePipeline,
]

try:
    import apache_beam  # noqa
except ImportError:
    warnings.warn(
        "Apache Beam not found. Install pipe-gaps[beam] to enable beam integration.", stacklevel=1
    )
    is_beam_installed = False
else:
    from pipe_gaps.pipeline.pipe_beam import BeamPipeline

    is_beam_installed = True
    __all__.append(BeamPipeline)
