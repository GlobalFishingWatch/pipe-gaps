import warnings

from pipe_gaps.pipeline.base import run, Pipeline, PipelineError, NoMessagesFound, Config
from pipe_gaps.pipeline.pipe_naive import NaivePipeline

__all__ = [run, Pipeline, PipelineError, NoMessagesFound, Config, NaivePipeline]

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
