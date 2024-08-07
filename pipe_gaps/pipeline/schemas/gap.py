import typing

from .message import Message


class Gap(typing.NamedTuple):
    """Schema for gaps."""

    OFF: Message
    ON: Message
