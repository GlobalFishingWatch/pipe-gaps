from functools import cached_property

from gfw.common.iterables import binary_search_first_ge

from pipe_gaps.core import GapDetector


class Messages:
    """Sorted collection of position messages with time-based retrieval methods.

    Args:
        messages:
            List of message dicts.

        timestamp_key:
            Key used to extract timestamps from messages. Defaults to "timestamp".
    """

    def __init__(self, messages: list[dict], timestamp_key: str = "timestamp"):
        self._timestamp_key = timestamp_key
        self._messages = messages

    @cached_property
    def sorted(self) -> list[dict]:
        """Returns messages sorted by GapDetector's canonical key.

        Sorting is done in-place to avoid creating a new list — messages can be large.
        Result is cached so subsequent calls do not re-sort.
        """
        self._messages.sort(key=GapDetector.message_sort_key)
        return self._messages

    def first_message_at_or_after(self, timestamp: float) -> dict:
        """Returns first message at or after the given timestamp.

        Args:
            timestamp:
                Unix timestamp to search from.
        """
        idx = binary_search_first_ge(self.sorted, timestamp, key=lambda m: m[self._timestamp_key])

        return self.sorted[max(idx, 0)]

    def last_messages(self, offset: int = 0) -> list[dict]:
        """Returns all messages within offset seconds before the last message.

        Args:
            offset:
                Time window in seconds before the last message.
        """
        last_timestamp = self.sorted[-1][self._timestamp_key]
        cutoff = last_timestamp - offset
        i = binary_search_first_ge(self.sorted, cutoff, key=lambda m: m[self._timestamp_key])

        return self.sorted[i:]

    def __len__(self) -> int:
        return len(self._messages)

    def __getitem__(self, idx) -> dict:
        return self.sorted[idx]
