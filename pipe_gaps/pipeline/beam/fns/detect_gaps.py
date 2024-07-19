"""This module encapsulates the apache beam DoFn gap process."""
import logging

from datetime import datetime
from dataclasses import dataclass

import apache_beam as beam

from pipe_gaps.core import gap_detector as gd
from pipe_gaps.pipeline.schemas import Gap

logger = logging.getLogger(__name__)


@dataclass(eq=True, frozen=True)
class ProcessingUnit:
    """Encapsulates the key to group by processing units."""

    ssvid: str
    year: str

    @classmethod
    def from_dict(cls, item: dict) -> "ProcessingUnit":
        return cls(ssvid=item["ssvid"], year=str(datetime.fromtimestamp(item["timestamp"]).year))


class DetectGapsFn(beam.DoFn):
    """Fn to wire core gap detection process with apache beam."""

    def __init__(self, **config):
        self.config = config

    def process(self, element: tuple) -> list[tuple[ProcessingUnit, list[Gap]]]:
        """Process elements of a keyed p-collection.

        Args:
            element: Processing unit (key, inputs).

        Returns:
            Processed items (key, outputs).
        """
        key, messages = element

        gaps = gd.detect(messages=messages, **self.config)
        logger.info("Found {} gaps for key={}".format(len(gaps), key))

        yield key, gaps

    @staticmethod
    def type():
        return Gap

    @staticmethod
    def parallelization_unit(item):
        return ProcessingUnit.from_dict(item)
