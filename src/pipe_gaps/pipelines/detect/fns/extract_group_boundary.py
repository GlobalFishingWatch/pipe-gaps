from typing import Iterable, Any
from dataclasses import dataclass

from apache_beam.transforms.core import DoFn
from apache_beam.transforms.window import IntervalWindow

from gfw.common.iterables import binary_search_first_ge


@dataclass(eq=True, frozen=True)
class Boundary:
    """Encapsulates first N and last M position messages for an ssvid and time interval.

    Args:
        ssvid:
            Id for the vessel.

        start:
            First N messages of the time interval.

        end:
            Last message of the time interval.
    """
    ssvid: str
    start: list[dict]
    end: list[dict]

    def __getitem__(self, key):
        return self.__dict__[key]

    @classmethod
    def from_group(
        cls, group: tuple, offset: int, start_time: int = None, timestamp_key="timestamp"
    ):
        """Instantiates a Boundary object from a group.

        Args:
            group:
                A tuple (key, messages), where `key` is typically the SSVID and `messages`
                is a list of dicts containing vessel messages.

            offset:
                The offset in seconds used to determine how far back to look when selecting
                the last messages in the group.

            start_time:
                Optional Unix timestamp (in seconds) used to skip initial messages before
                this threshold. If None, all messages are considered.

            timestamp_key:
                The key used to extract timestamps from messages. Defaults to "timestamp".
        """
        ssvid, messages = group

        messages.sort(key=lambda x: x[timestamp_key])

        first_msg_index = 0
        if start_time is not None:
            first_msg_index = cls.get_index_for_start_time(messages, start_time)

        start = [messages[first_msg_index]]

        end = cls.get_last_messages(messages, offset)

        return cls(ssvid=ssvid, start=start, end=end)

    @classmethod
    def get_index_for_start_time(cls, messages: list[dict], start_time: int):
        idx = binary_search_first_ge(
            messages,
            start_time,
            key=lambda m: m["timestamp"]
        )

        if idx < 0:
            idx = 0

        return idx

    @classmethod
    def get_last_messages(cls, messages: list[dict], offset: int = 0):
        # We get all messages within a period of time before the last message.

        last_msg_timestamp = messages[-1]["timestamp"]
        n_hours_before = last_msg_timestamp - offset

        i = len(messages) - 1
        for m in reversed(messages):
            if m["timestamp"] == last_msg_timestamp:
                continue

            if m["timestamp"] < n_hours_before:
                break

            i -= 1

        return messages[i:]

    def last_message(self):
        return self.end[-1]

    def first_message(self):
        return self.start[0]


class ExtractGroupBoundary(DoFn):
    def __init__(self, window_offset_s: int, timestamp_key: str = "timestamp"):
        self._window_offset_s = window_offset_s
        self._timestamp_key = timestamp_key

    def process(
        self, group: tuple[Any, Iterable[dict]], window: IntervalWindow = DoFn.WindowParam
    ) -> Iterable[Boundary]:
        start_time = window.start.seconds() + self._window_offset_s

        key, messages = group
        messages = list(messages)  # On dataflow, `messages` is a _ConcatSequence object.

        yield Boundary.from_group(
            (key.ssvid, messages),
            offset=self._window_offset_s,
            start_time=start_time,
            timestamp_key=self._timestamp_key
        )
