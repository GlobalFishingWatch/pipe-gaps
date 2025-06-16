from typing import Iterable, Any

from apache_beam.transforms.window import IntervalWindow
from apache_beam.transforms.core import DoFn

from pipe_gaps.pipeline.processes.common import Boundary


class ExtractGroupBoundary(DoFn):
    def __init__(self, window_offset_s: int, timestamp_key: str = "timestamp"):
        self._window_offset_s = window_offset_s
        self._timestamp_key = timestamp_key

    def process(
        self,
        group: tuple[Any, Iterable[dict]],
        window: IntervalWindow = DoFn.WindowParam
    ) -> Iterable[Boundary]:
        start_time = None
        if isinstance(window, IntervalWindow):
            start_time = window.start.seconds() + self._window_offset_s

        key, messages = group
        messages = list(messages)  # On dataflow, this is a _ConcatSequence object.

        yield Boundary.from_group(
            (key.ssvid, messages),
            offset=self._window_offset_s,
            start_time=start_time,
            timestamp_key=self._timestamp_key
        )
