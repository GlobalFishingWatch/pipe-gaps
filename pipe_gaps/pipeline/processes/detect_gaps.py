import logging
import operator
from typing import Type, Iterable, Optional

from pipe_gaps.core import GapDetector
from pipe_gaps.pipeline.schemas import Gap

from .base import CoreProcess
from .common import SsvidAndYear, Message, Ssvid, YearBoundary

logger = logging.getLogger(__name__)


def off_message_from_gap(gap: dict):
    return dict(
        ssvid=gap["ssvid"],
        seg_id=gap["gap_start_seg_id"],
        msgid=gap["gap_start_msgid"],
        timestamp=gap["gap_start"],
        distance_from_shore_m=gap["gap_start_distance_from_shore_m"]
    )


class DetectGaps(CoreProcess):
    """Defines the gap detection process step of the "gaps pipeline".

    Args:
        gd: core gap detector.
        eval_last: If True, evaluates last message of each vessel to create an open gap.
    """

    def __init__(self, gd: GapDetector, eval_last: bool = False):
        self._gd = gd
        self._eval_last = eval_last

    @classmethod
    def build(cls, eval_last: bool = False, **config) -> "DetectGaps":
        gd = GapDetector(**config)
        return cls(gd=gd, eval_last=eval_last)

    def process_group(self, group: tuple[SsvidAndYear, Iterable[Message]]) -> Iterable[Gap]:
        key, messages = group

        gaps = self._gd.detect(messages=messages)

        logger.info("Found {} gaps for key={}".format(len(gaps), key))

        for gap in gaps:
            yield gap

    def process_boundaries(
        self,
        group: tuple[Ssvid, Iterable[YearBoundary]],
        side_inputs: Optional[list[Gap]] = None
    ) -> Iterable[Gap]:
        key, year_boundaries = group

        year_boundaries = sorted(year_boundaries, key=operator.attrgetter("year"))
        consecutive_years = list(zip(year_boundaries[:-1], year_boundaries[1:]))

        boundaries_messages = [[left.end, right.start] for left, right in consecutive_years]

        gaps = []
        for messages_pair in boundaries_messages:
            gaps.extend(self._gd.detect(messages_pair))

        logger.info(f"Found {len(gaps)} gaps analyzing year boundaries for key={key}...")

        if self._eval_last:
            last_m = max(year_boundaries, key=operator.attrgetter("year")).end
            new_open_gap = self._gd.eval_open_gap(last_m)

            if new_open_gap is not None:
                logger.info(f"Creating 1 open gap for key={key}...")
                gaps.append(new_open_gap)

        if side_inputs is not None:
            # TODO: make input p-collection to have objects instead of dicts?
            open_gaps = {g["ssvid"]: g for g in side_inputs}
            if key.ssvid in open_gaps:
                logger.info(f"Closing 1 open gap found for key={key}")
                off_message = off_message_from_gap(open_gaps[key.ssvid])
                on_message = min(year_boundaries, key=operator.attrgetter("year")).start
                closed_gap = dict(OFF=off_message, ON=on_message)
                gaps.append(closed_gap)

        for gap in gaps:
            yield gap

    def get_group_boundary(self, group: tuple[SsvidAndYear, Iterable[Message]]) -> YearBoundary:
        return YearBoundary.from_group(group, timestamp_key=self._gd.KEY_TIMESTAMP)

    @staticmethod
    def type():
        return Gap

    @staticmethod
    def groups_key() -> Type[SsvidAndYear]:
        return SsvidAndYear

    @staticmethod
    def boundaries_key() -> Type[Ssvid]:
        return Ssvid

    def sorting_key(self):
        return lambda x: (x["ssvid"], x[self._gd.KEY_TIMESTAMP])
