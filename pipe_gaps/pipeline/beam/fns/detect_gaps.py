"""This module encapsulates the apache beam DoFn gap process."""
import logging
from datetime import datetime
from dataclasses import dataclass

from pipe_gaps.core import gap_detector as gd
from pipe_gaps.pipeline.schemas import Gap
from pipe_gaps.pipeline.beam.fns import base

logger = logging.getLogger(__name__)


@dataclass(eq=True, frozen=True)
class SsvidTimestamp(base.ProcessingUnit):
    """Encapsulates the key to group by processing units."""

    ssvid: str
    year: str

    @classmethod
    def from_dict(cls, item: dict) -> "SsvidTimestamp":
        return cls(ssvid=item["ssvid"], year=str(datetime.fromtimestamp(item["timestamp"]).year))


class DetectGapsFn(base.BaseFn):
    """Fn to wire core gap detection process with apache beam.

    Args:
        **config: keyword arguments for the core process.
    """

    def __init__(self, **config):
        self._config = config

    def process(self, element: tuple) -> list[tuple[base.ProcessingUnit, list[Gap]]]:
        key, messages = element

        gaps = gd.detect(messages=messages, **self._config)
        logger.info("Found {} gaps for key={}".format(len(gaps), key))

        yield key, gaps

    @staticmethod
    def type():
        return Gap

    @staticmethod
    def parallelization_unit(item) -> base.ProcessingUnit:
        return SsvidTimestamp.from_dict(item)
