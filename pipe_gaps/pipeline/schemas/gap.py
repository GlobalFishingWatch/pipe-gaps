import typing

from pipe_gaps.queries import Message


class Gap(typing.NamedTuple):
    """Schema for gaps."""

    OFF: Message
    ON: Message
