"""This module encapsulates the apache beam DoFn gap process."""
import logging

from pipe_gaps.core import gap_detector as gd
from pipe_gaps.pipeline.schemas import Gap
from pipe_gaps.pipeline.beam.fns import base
from pipe_gaps.pipeline.common import SsvidAndYearKey

logger = logging.getLogger(__name__)


class DetectGapsFn(base.BaseFn):
    """Fn to wire core gap detection process with apache beam.

    Args:
        **config: keyword arguments for the core process.
    """

    def __init__(self, **config):
        self._config = config

    def process(self, element: tuple) -> list[tuple[SsvidAndYearKey, list[Gap]]]:
        key, messages = element

        gaps = gd.detect(messages=messages, **self._config)
        logger.info("Found {} gaps for key={}".format(len(gaps), key))

        yield key, gaps

    @staticmethod
    def type():
        return Gap

    @staticmethod
    def parallelization_unit(item) -> SsvidAndYearKey:
        return SsvidAndYearKey.from_dict(item)
