"""This module encapsulates the apache beam DoFn gap process."""
import logging
import operator
from typing import Type
from dataclasses import dataclass

from pipe_gaps.core import gap_detector as gd
from pipe_gaps.pipeline.schemas import Gap, Message
from pipe_gaps.pipeline.beam.fns import base
from pipe_gaps.pipeline.common import SsvidAndYearKey


logger = logging.getLogger(__name__)


@dataclass(eq=True, frozen=True)
class YearBoundaryMessages:
    """Defines start and end messages for a specific year and ssvid."""
    ssvid: str
    year: str
    start: Message
    end: Message


class DetectGapsFn(base.BaseFn):
    """Fn to wire core gap detection process with apache beam.

    TODO: this could be done differently. We could
    1. build all gaps (pairs of consecutive messages) from groups and boundaries.
    2. filter gaps without any grouping (beam decides, order is not important anymore),
        based on threshold condition.

    Args:
        **config: keyword arguments for the core process.
    """

    def __init__(self, **config):
        self._config = config

    def process(self, element: tuple) -> list[Gap]:
        """Receives AIS messages grouped by (ssvid, year) and returns gaps inside those groups."""
        key, messages = element

        gaps = gd.detect(messages=messages, **self._config)
        logger.info("Found {} gaps for key={}".format(len(gaps), key))

        for gap in gaps:
            yield gap

    def get_boundaries(self, element: tuple) -> YearBoundaryMessages:
        """Receives messages grouped by (ssvid, year) and returns a YearBoundaryMessages object."""
        key, messages = element
        start = min(messages, key=lambda x: x["timestamp"])
        end = max(messages, key=lambda x: x["timestamp"])

        return YearBoundaryMessages(ssvid=key.ssvid, year=key.year, start=start, end=end)

    def process_boundaries(self, element: tuple) -> list[Gap]:
        """Receives YearBoundaryMessages objects grouped by ssvid and returns gaps."""
        key, year_boundaries = element

        year_boundaries = sorted(year_boundaries, key=operator.attrgetter("year"))
        consecutive_years = list(zip(year_boundaries[:-1], year_boundaries[1:]))

        boundaries_messages = [[left.end, right.start] for left, right in consecutive_years]

        gaps = []
        for messages_pair in boundaries_messages:
            gaps.extend(gd.detect(messages_pair, **self._config))

        logger.info(f"Found {len(gaps)} gaps analyzing year boundaries for key={key}...")

        for gap in gaps:
            yield gap

    @staticmethod
    def type():
        return Gap

    @staticmethod
    def processing_unit_key() -> Type[SsvidAndYearKey]:
        return SsvidAndYearKey

    @staticmethod
    def boundaries_key(item) -> str:
        return item.ssvid
