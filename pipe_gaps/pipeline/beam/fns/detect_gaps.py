"""This module encapsulates the apache beam DoFn gap process."""
import logging
import operator
from typing import Type
from dataclasses import dataclass

from pipe_gaps.core import GapDetector
from pipe_gaps.pipeline.schemas import Gap, Message
from pipe_gaps.pipeline.beam.fns import base
from pipe_gaps.pipeline.common import SsvidAndYearKey


logger = logging.getLogger(__name__)


@dataclass(eq=True, frozen=True)
class YearBoundary:
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
        eval_last: if True, evaluates last message of each ssvid to create an open gap.
        **config: keyword arguments for the core process.
    """

    def __init__(self, eval_last=False, **config):
        self._eval_last = eval_last
        self._gd = GapDetector(**config)

    def process(self, element: tuple) -> list[Gap]:
        """Receives AIS messages grouped by (ssvid, year) and returns gaps inside those groups."""
        key, messages = element

        gaps = self._gd.detect(messages=messages)
        logger.info("Found {} gaps for key={}".format(len(gaps), key))

        for gap in gaps:
            yield gap

    def get_boundaries(self, element: tuple) -> YearBoundary:
        """Receives messages grouped by (ssvid, year) and returns a YearBoundary object."""
        key, messages = element
        start = min(messages, key=lambda x: x[self._gd.KEY_TIMESTAMP])
        end = max(messages, key=lambda x: x[self._gd.KEY_TIMESTAMP])

        return YearBoundary(ssvid=key.ssvid, year=key.year, start=start, end=end)

    def process_boundaries(self, element: tuple) -> list[Gap]:
        """Receives YearBoundary objects grouped by ssvid and returns gaps."""
        key, year_boundaries = element

        year_boundaries = sorted(year_boundaries, key=operator.attrgetter("year"))
        consecutive_years = list(zip(year_boundaries[:-1], year_boundaries[1:]))

        boundaries_messages = [[left.end, right.start] for left, right in consecutive_years]

        gaps = []
        for messages_pair in boundaries_messages:
            gaps.extend(self._gd.detect(messages_pair))

        logger.info(f"Found {len(gaps)} gaps analyzing year boundaries for key={key}...")

        if self._eval_last:
            last_m = max(year_boundaries, key=operator.attrgetter("year")).end
            open_gap = self._gd.eval_open_gap(last_m)

            if open_gap is not None:
                logger.info(f"Found 1 open gap for key={key}...")
                gaps.append(open_gap)

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
