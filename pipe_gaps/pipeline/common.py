"""Module with re-usable subclass implementations."""
import logging
import operator
import itertools
from typing import Type, Iterable
from datetime import datetime
from dataclasses import dataclass

from pipe_gaps.pipeline.base import CoreProcess, Key
from pipe_gaps.pipeline.schemas import Message

from pipe_gaps.core import GapDetector
from pipe_gaps.pipeline.schemas import Gap

logger = logging.getLogger(__name__)


@dataclass(eq=True, frozen=True)
class SsvidAndYear(Key):
    ssvid: str
    year: str

    @classmethod
    def from_dict(cls, item: dict) -> "SsvidAndYear":
        return cls(ssvid=item["ssvid"], year=str(datetime.fromtimestamp(item["timestamp"]).year))


@dataclass(eq=True, frozen=True)
class Ssvid(Key):
    ssvid: str

    @classmethod
    def from_dict(cls, item: dict) -> "Ssvid":
        return cls(ssvid=item["ssvid"])


@dataclass(eq=True, frozen=True)
class YearBoundary:
    """Defines first and last AIS messages for a specific year and ssvid."""
    ssvid: str
    year: str
    start: Message
    end: Message

    def __getitem__(self, key):
        return self.__dict__[key]

    @classmethod
    def from_group(cls, element, timestamp_key="timestamp"):
        key, messages = element

        start = min(messages, key=lambda x: x[timestamp_key])
        end = max(messages, key=lambda x: x[timestamp_key])

        return cls(ssvid=key.ssvid, year=key.year, start=start, end=end)


class DetectGaps(CoreProcess):
    """Defines the gaps detection process step of the Gaps Pipeline.

    Args:
        gd: core gap detector.
        eval_last: If True, evaluates last message of each vessel to create an open gap.
    """

    def __init__(self, gd: GapDetector, eval_last=False):
        self._gd = gd
        self._eval_last = eval_last

    @classmethod
    def build(cls, eval_last=False, **config):
        gd = GapDetector(**config)
        return cls(gd=gd, eval_last=eval_last)

    def process(self, elements: list) -> dict[str, list[Gap]]:
        messages = elements
        logger.info("Total amount of input messages: {}".format(len(messages)))

        logger.info("Sorting messages...")
        sorted_messages = sorted(messages, key=lambda x: (x["ssvid"], x[self._gd.KEY_TIMESTAMP]))

        logger.info(f"Grouping messages by {self.groups_key().attributes()}...")
        grouped_messages = [
            (k, list(v))
            for k, v in itertools.groupby(sorted_messages, key=self.groups_key().from_dict)
        ]

        logger.info("Detecting gaps in groups...")
        gaps_by_ssvid = {}
        for key, messages in grouped_messages:
            gaps = self.process_group((key, messages))
            gaps_by_ssvid.setdefault(key.ssvid, []).extend(gaps)

        logger.info("Detecting gaps in boundaries...")
        boundaries = [
            YearBoundary.from_group(group, timestamp_key=self._gd.KEY_TIMESTAMP)
            for group in grouped_messages
        ]

        grouped_boundaries = itertools.groupby(boundaries, key=self.boundaries_key().from_dict)
        for key, boundaries in grouped_boundaries:
            gaps_in_boundaries = self.process_boundaries((key.ssvid, boundaries))
            gaps_by_ssvid[key.ssvid].extend(gaps_in_boundaries)

        return gaps_by_ssvid

    def process_group(self, element: tuple, *args, **kwargs) -> Iterable[Gap]:
        key, messages = element
        gaps = self._gd.detect(messages=messages)

        logger.info("Found {} gaps for key={}".format(len(gaps), key))

        for gap in gaps:
            yield gap

    def process_boundaries(self, element: tuple) -> Iterable[Gap]:
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

    def get_group_boundaries(self, element: tuple) -> YearBoundary:
        return YearBoundary.from_group(element, timestamp_key=self._gd.KEY_TIMESTAMP)

    @staticmethod
    def type():
        return Gap

    @staticmethod
    def groups_key() -> Type[SsvidAndYear]:
        return SsvidAndYear

    @staticmethod
    def boundaries_key() -> Type[Ssvid]:
        return Ssvid
