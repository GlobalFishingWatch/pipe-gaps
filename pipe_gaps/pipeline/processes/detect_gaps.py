import logging
from datetime import date

from pipe_gaps.core import GapDetector
from pipe_gaps.common.key import Key


from .base import CoreProcess

logger = logging.getLogger(__name__)


MAX_WINDOW_PERIOD_D = 180  # Max. window period in days. Requires further testing. Could be higher.


class DetectGaps(CoreProcess):
    """Defines the gap detection process step of the "gaps pipeline".

    Args:
        gd: core gap detector.
        group_by: Operation to use when grouping messages processed by this class.
        eval_last: If True, evaluates last message of each vessel to create an open gap.
        window_period_d: period for the time window in days.
        window_offset_h: offset for the time window in hours.
        date_range: only detect gaps within this date range.
    """

    KEY_TIMESTAMP = GapDetector.KEY_TIMESTAMP
    KEY_SSVID = GapDetector.KEY_SSVID
    KEY_GAP_ID = GapDetector.KEY_GAP_ID

    def __init__(
        self,
        gd: GapDetector,
        grouping_key: Key,
        eval_last: bool = False,
        window_period_d: int = MAX_WINDOW_PERIOD_D,
        window_offset_h: int = 12,
        date_range: tuple[date, date] = None,
    ):
        self._gd = gd
        self._grouping_key = grouping_key
        self._eval_last = eval_last
        self._window_period_d = window_period_d
        self._window_offset_h = window_offset_h
        self._date_range = date_range

    @classmethod
    def build(
        cls,
        date_range: tuple = None,
        eval_last: bool = False,
        window_period_d: int = None,
        window_offset_h: int = 12,
        **config
    ) -> "DetectGaps":

        return cls(
            gd=GapDetector(**config),
            grouping_key=Key([cls.KEY_SSVID]),
            eval_last=eval_last,
            window_period_d=window_period_d,
            window_offset_h=window_offset_h,
            date_range=date_range,
        )
