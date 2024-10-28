import logging
from datetime import date
from typing import Type, Iterable, Optional, Any

from pipe_gaps.core import GapDetector

from .base import CoreProcess, Key
from .common import Boundary, Boundaries, key_factory

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
        gk: groups Key object. Used to group input messages.
        bk: boundaries Key object. Used to group boundaries.
        eval_last: If True, evaluates last message of each vessel to create an open gap.
    """

    def __init__(
        self,
        gd: GapDetector, gk: Key, bk: Key, eval_last: bool = False, filter_range: tuple = None,
    ):
        self._gd = gd
        self._gk = gk
        self._bk = bk
        self._eval_last = eval_last
        self._filter_range = filter_range

    @classmethod
    def build(
        cls,
        groups_key: str = "ssvid_year",
        boundaries_key: str = "ssvid",
        filter_range: tuple = None,
        eval_last: bool = False,
        **config
    ) -> "DetectGaps":
        return cls(
            gd=GapDetector(**config),
            gk=key_factory(groups_key),
            bk=key_factory(boundaries_key),
            eval_last=eval_last,
            filter_range=filter_range,
        )

    def process_group(self, group: tuple[Any, Iterable[dict]]) -> Iterable[dict]:
        key, messages = group

        gaps = self._gd.detect(messages=list(messages))

        logger.info(
            "Found {} gap(s) for group {}".format(len(gaps), self.groups_key().format(key)))

        for gap in gaps:
            yield gap

    def process_boundaries(
        self,
        group: tuple[Any, Iterable[Boundary]],
        side_inputs: Optional[dict[Any, Iterable]] = None
    ) -> Iterable[dict]:
        key, boundaries_it = group

        boundaries = Boundaries(boundaries_it)

        formatted_key = self.boundaries_key().format(key)

        gaps = {}
        for pair in boundaries.messages():
            for g in self._gd.detect(pair):
                gaps[g["gap_id"]] = g

        if self._eval_last:
            last_message = boundaries.last_message()

            if self._gd.eval_open_gap(last_message):
                logger.info(f"Creating new open gap for {formatted_key}...")
                new_open_gap = self._gd.create_gap(off_m=last_message)
                gaps[new_open_gap["gap_id"]] = new_open_gap

        open_gap = self._load_open_gap(side_inputs, key)

        if open_gap is not None:
            open_gap_id = open_gap["gap_id"]
            logger.info("gap_id={}".format(open_gap_id))
            logger.info(f"Closing existing open gap for {formatted_key}")

            if open_gap_id not in gaps:
                closed_gap = self._close_open_gap(open_gap, boundaries)
                gaps[closed_gap["gap_id"]] = closed_gap

        logger.info(f"Found {len(gaps)} gap(s) for boundaries {formatted_key}...")

        for g in gaps.values():
            yield g

    def get_group_boundary(self, group: tuple[Any, Iterable[dict]]) -> Boundary:
        return Boundary.from_group(group, timestamp_key=self._gd.KEY_TIMESTAMP)

    def sorting_key(self):
        return lambda x: (x["ssvid"], x["timestamp"])

    def filter_range(self):
        if self._filter_range is not None:
            start, end = self._filter_range
            return (date.fromisoformat(start), date.fromisoformat(end))

        return None

    def groups_key(self) -> Type[Key]:
        return self._gk

    def boundaries_key(self) -> Type[Key]:
        return self._bk

    def _load_side_inputs(self, side_inputs, key):
        side_inputs_list = []
        if side_inputs is not None:
            try:
                side_inputs_list = list(side_inputs[key])
            except KeyError:
                # A key was not found for this group.
                pass

        return side_inputs_list

    def _load_open_gap(self, side_inputs, key):
        side_inputs_list = self._load_side_inputs(side_inputs, key)

        if len(side_inputs_list) > 0:
            open_gap = side_inputs_list[0]

            if not isinstance(open_gap, dict):
                # beam.MultiMap encapsulates value in an iterable of iterables (wtf?).
                open_gap = [x for x in open_gap][0]

            return open_gap

        return None

    def _close_open_gap(self, open_gap, boundaries):
        off_m = off_message_from_gap(open_gap)
        on_m = boundaries.first_message()

        # Re-order off-message using on-message keys.
        off_m = {k: off_m[k] for k in on_m.keys() if k in off_m}

        return self._gd.create_gap(off_m=off_m, on_m=on_m, gap_id=open_gap["gap_id"])
