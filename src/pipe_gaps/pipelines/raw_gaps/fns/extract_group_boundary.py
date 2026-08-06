from typing import Iterable, Any
from dataclasses import dataclass

from apache_beam.transforms.core import DoFn
from apache_beam.transforms.window import IntervalWindow
from gfw.common.datetime import datetime_from_date

from pipe_gaps.pipelines.raw_gaps.messages import Messages


@dataclass(eq=True, frozen=True)
class Boundary:
    """Encapsulates first and last position messages for an ssvid within a time window.

    Args:
        ssvid:
            Id for the vessel.

        start:
            First message at or after window_start + offset.

        end:
            Last messages of the time interval, i.e. all messages within
            offset seconds before the window's last message.

        first_message_in_range:
            First message at or after the processing range start, if provided.
            Used by ProcessBoundaries to find the correct ON message when closing open gaps.
    """
    ssvid: str
    start: dict
    end: list[dict]
    first_message_in_range: dict | None = None

    def __getitem__(self, key):
        return self.__dict__[key]

    def first_message(self):
        return self.start

    def last_message(self):
        return self.end[-1]


class ExtractGroupBoundary(DoFn):
    """Apache Beam DoFn that extracts boundary messages from each windowed group.

    For each window, retains only the first and last messages needed by
    ProcessBoundaries to detect cross-window gaps and close open gaps.

    Args:
        window_offset_s:
            Offset in seconds used to determine window start time and
            the lookback period for selecting last messages.

        timestamp_key:
            Key used to extract timestamps from messages. Defaults to "timestamp".

        date_range:
            Optional tuple of (start_date, end_date). When provided, the boundary
            will also retain the first message at or after start_date, used by
            ProcessBoundaries to correctly close open gaps.
    """

    def __init__(
        self,
        window_offset_s: int,
        timestamp_key: str = "timestamp",
        date_range=None,
    ):
        self._window_offset_s = window_offset_s
        self._timestamp_key = timestamp_key
        self._date_range = date_range

    def process(
        self, group: tuple[Any, Iterable[dict]], window: IntervalWindow = DoFn.WindowParam
    ) -> Iterable[Boundary]:
        key, raw_messages = group
        messages = Messages(list(raw_messages), self._timestamp_key)

        start_time = window.start.seconds() + self._window_offset_s

        first_message_in_range = None
        if self._date_range is not None:
            # TODO: should we make the date_range mandatory for the pipeline?
            range_start_ts = datetime_from_date(self._date_range[0]).timestamp()
            first_message_in_range = messages.first_message_at_or_after(timestamp=range_start_ts)

        yield Boundary(
            ssvid=key.ssvid,
            start=messages.first_message_at_or_after(timestamp=start_time),
            end=messages.last_messages(offset=self._window_offset_s),
            first_message_in_range=first_message_in_range,
        )
