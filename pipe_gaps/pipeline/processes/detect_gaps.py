import logging
import operator
from typing import Type, Iterable, Optional

from pipe_gaps.core import GapDetector

from .base import CoreProcess
from .common import SsvidAndYear, Ssvid, YearBoundary

logger = logging.getLogger(__name__)


def off_message_from_gap(gap: dict):
    """Extracts off message from gap object."""
    to_remove = "gap_start_"
    off_message = {
        k.replace(to_remove, ""): v
        for k, v in gap.items()
        if to_remove in k
    }
    off_message["ssvid"] = gap["ssvid"]

    return off_message


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

    def process_group(self, group: tuple[tuple[str, int], Iterable[dict]]) -> Iterable[dict]:
        key, messages = group

        gaps = self._gd.detect(messages=list(messages))

        logger.info("Found {} gaps for {}".format(len(gaps), self.groups_key().format(key)))

        for gap in gaps:
            yield gap

    def process_boundaries(
        self,
        group: tuple[str, Iterable[YearBoundary]],
        side_inputs: Optional[dict[str, Iterable]] = None
    ) -> Iterable[dict]:
        key, year_boundaries = group

        year_boundaries = sorted(year_boundaries, key=operator.attrgetter("year"))
        consecutive_years = list(zip(year_boundaries[:-1], year_boundaries[1:]))
        boundaries_messages = [[left.end, right.start] for left, right in consecutive_years]

        formatted_key = self.boundaries_key().format(key)

        gaps = []
        for messages_pair in boundaries_messages:
            gaps.extend(self._gd.detect(messages_pair))

        logger.info(f"Found {len(gaps)} gaps analyzing boundaries for {formatted_key}...")
        for g in gaps:
            yield g

        if self._eval_last:
            last_m = max(year_boundaries, key=operator.attrgetter("year")).end
            new_open_gap = self._gd.eval_open_gap(last_m)

            if new_open_gap is not None:
                logger.info(f"Creating 1 new open gap for {formatted_key}...")
                yield new_open_gap

        side_inputs_list = []
        if side_inputs is not None:
            try:
                side_inputs_list = list(side_inputs[key])
            except KeyError:
                # A key was not found for this group.
                pass

        if len(side_inputs_list) > 0:
            open_gap = side_inputs_list[0]

            if not isinstance(open_gap, dict):
                # beam.MultiMap encapsulates value in an iterable of iterables (wtf?).
                open_gap = [x for x in open_gap][0]

            logger.info(f"Closing 1 existing open gap found for {formatted_key}")
            logger.info("gap_id={}".format(open_gap["gap_id"]))

            yield self._close_open_gap(open_gap, year_boundaries)

    def get_group_boundary(self, group: tuple[tuple[str, int], Iterable[dict]]) -> YearBoundary:
        return YearBoundary.from_group(group, timestamp_key=self._gd.KEY_TIMESTAMP)

    def sorting_key(self):
        return lambda x: (x["ssvid"], x["timestamp"])

    def _close_open_gap(self, open_gap, year_boundaries):
        off_m = off_message_from_gap(open_gap)
        on_m = min(year_boundaries, key=operator.attrgetter("year")).start

        # Re-order off-message using on-message keys.
        off_m = {k: off_m[k] for k in on_m.keys() if k in off_m}

        return self._gd.create_gap(off_m=off_m, on_m=on_m, gap_id=open_gap["gap_id"])

    @staticmethod
    def groups_key() -> Type[SsvidAndYear]:
        return SsvidAndYear

    @staticmethod
    def boundaries_key() -> Type[Ssvid]:
        return Ssvid
