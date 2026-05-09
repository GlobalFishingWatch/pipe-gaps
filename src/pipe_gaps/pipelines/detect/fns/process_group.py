import logging
from typing import Iterable, Any, Optional
from datetime import timedelta, date

from apache_beam import DoFn
from apache_beam.transforms.window import IntervalWindow

from gfw.common.datetime import datetime_from_timestamp, datetime_from_date
from gfw.common.iterables import binary_search_first_ge

from pipe_gaps.core import GapDetector
from pipe_gaps.common.key import Key
from pipe_gaps.common.beam.side_inputs import SideInputs

logger = logging.getLogger(__name__)


class ProcessGroup(DoFn):
    KEY_TIMESTAMP = GapDetector.KEY_TIMESTAMP
    KEY_SSVID = GapDetector.KEY_SSVID
    KEY_GAP_ID = GapDetector.KEY_GAP_ID

    def __init__(
        self,
        gap_detector: GapDetector,
        key: Key,
        window_offset_h: int = 12,
        date_range: tuple[date, date] = None,
    ):
        self._gd = gap_detector
        self._key = key
        self._window_offset_h = window_offset_h
        self._date_range = date_range

    def process(
        self,
        group: tuple[Any, Iterable[dict]],
        window: IntervalWindow = DoFn.WindowParam,
        side_inputs: Optional[dict[Any, Iterable]] = None
    ):
        key, messages = group

        messages = list(messages)  # On dataflow, this is a _ConcatSequence object.
        messages.sort(key=lambda x: x[self.KEY_TIMESTAMP])

        window_start_time = window.start.to_utc_datetime(has_tz=True)
        window_end_time = window.end.to_utc_datetime(has_tz=True)

        logger.debug("Processing window [{}, {}]".format(window_start_time, window_end_time))

        start_time = window_start_time + timedelta(hours=self._window_offset_h)
        if self._date_range is not None:
            range_start_time = datetime_from_date(self._date_range[0])

            start_idx = self._get_index_for_time(messages, range_start_time)
            if start_idx > 0:
                # Step back one message to include context from before the range boundary.
                # This only applies to the first window, which has no previous boundary.
                # Subsequent windows will evaluate this previous message in ProcessBoundaries.
                # So no duplicate should appear from this.
                start_idx = start_idx - 1

            effective_start_time = datetime_from_timestamp(messages[start_idx][self.KEY_TIMESTAMP])
            start_time = max(start_time, effective_start_time)

        gaps = self._gd.detect(messages=messages, start_time=start_time)

        logger.debug(
            "Found {} gap(s) for {} in range [{}, {}]"
            .format(
                len(gaps),
                self._key.format(key),
                start_time.date(),
                window_end_time.date(),
            )
        )

        for gap in gaps:
            self._debug_gap(gap)

            # Emit open version if daily mode would have created one on any day between OFF and ON.
            # This ensures range processing produces the same table state as daily processing,
            # so that a subsequent reprocess can always reconstruct the gap from the open version.
            off_m = self._gd.off_message_from_gap(gap)
            off_m_ts = datetime_from_timestamp(off_m[self.KEY_TIMESTAMP])

            off_date = off_m_ts.date()
            on_date = datetime_from_timestamp(gap[f"end_{self.KEY_TIMESTAMP}"]).date()
            days_spanned = (on_date - off_date).days  # excludes ON day naturally

            should_emit_open = any(
                self._gd.eval_open_gap(off_m, off_date + timedelta(days=i))
                for i in range(days_spanned)
            )

            # Don't emit open v1 if OFF is before range start - it's handled via ProcessBoundaries.
            is_before_range = self._date_range is not None and off_m_ts < range_start_time

            if should_emit_open and not is_before_range:
                logger.debug("Emitting open gap for recovery...")
                open_gap = self._gd.create_gap(
                    off_m=off_m,
                    gap_id=gap[self.KEY_GAP_ID],
                    base_gap=self._gd.previous_positions_from_gap(gap))

                self._debug_gap(open_gap)
                yield open_gap

            # Don't yield gap if OFF is before range start AND an open gap exists
            # in side inputs — ProcessBoundaries Step 2 will close it.
            # This avoids dupicates.
            open_gap = SideInputs(side_inputs).get_first(key) if side_inputs else None
            is_handled_by_process_boundaries = (
                is_before_range and open_gap is not None and
                open_gap[self.KEY_GAP_ID] == gap[self.KEY_GAP_ID]
            )

            if not is_handled_by_process_boundaries:
                yield gap

    def _get_index_for_time(self, messages: list, time):
        return binary_search_first_ge(
            messages,
            time.timestamp(),
            key=lambda m: m[self.KEY_TIMESTAMP]
        )

    def _debug_gap(self, g: dict):
        # TODO: move this elsewhere. It is duplicated.
        try:
            start_ts = g["OFF"][self.KEY_TIMESTAMP]
            end_ts = g["ON"][self.KEY_TIMESTAMP]
        except KeyError:
            start_ts = g[f"start_{self.KEY_TIMESTAMP}"]
            end_ts = g.get(f"end_{self.KEY_TIMESTAMP}")

        start_dt = datetime_from_timestamp(start_ts)
        end_dt = datetime_from_timestamp(end_ts) if end_ts is not None else None

        logger.debug("----------------------------------")
        logger.debug("Gap OFF: {}".format(start_dt))
        logger.debug("Gap  ON: {}".format(end_dt))
        logger.debug("----------------------------------")
