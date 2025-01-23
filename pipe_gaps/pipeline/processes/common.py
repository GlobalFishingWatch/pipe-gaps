"""Module with re-usable subclass implementations."""
import logging

from dataclasses import dataclass

logger = logging.getLogger(__name__)


class GroupByKey:
    """Defines a single or composite key for grouping elements."""
    def __init__(self, keys):
        self.keys = keys
        self.func = self._define_func()

    def __repr__(self):
        return str(self.keys)

    def name(self):
        return "And".join(s.title() for s in self.keys)

    def format(self, values):
        if not isinstance(values, (tuple, list)):
            values = [values]

        return "({})".format(
            ', '.join([f'{k}={v}' for k, v in zip(self.keys, values)])
        )

    def _define_func(self):
        return {
            k: lambda x: x[k]
            for k
            in self.keys
        }


class Boundaries:
    """Container for Boundary objects."""
    def __init__(self, boundaries):
        self._boundaries = sorted(boundaries, key=lambda x: x.first_message()["timestamp"])

    def consecutive_boundaries(self):
        return list(zip(self._boundaries[:-1], self._boundaries[1:]))

    def first_boundary(self):
        return self._boundaries[0]

    def last_boundary(self):
        return self._boundaries[-1]

    def first_message(self):
        return self.first_boundary().first_message()

    def last_message(self):
        return self.last_boundary().last_message()


@dataclass(eq=True, frozen=True)
class Boundary:
    """Encapsulates first N and last M AIS position messages for an ssvid and time interval.

    Args:
        ssvid: id for the vessel.
        start: first message of the time interval.
        end: last message of the time interval.
    """
    ssvid: str
    start: list
    end: list

    def __getitem__(self, key):
        return self.__dict__[key]

    @classmethod
    def from_group(
        cls, group: tuple, offset: int, start_time: int = None, timestamp_key="timestamp"
    ):
        """Instantiates a Boundary object from a group.

        Args:
            group: tuple with (key, messages).
            timestamp_key: name for the key containing the message timestamp.
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
    def get_index_for_start_time(cls, messages, start_time, default=0):
        # TODO: move to utils. Already implemented in GapDetector.
        for i, m in enumerate(messages):
            if m["timestamp"] >= start_time:
                return i

        return default

    @classmethod
    def get_last_messages(cls, messages, offset=0):
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
