import typing


class Message(typing.NamedTuple):
    """Schema for input messages."""

    ssvid: str
    seg_id: str
    msgid: str
    timestamp: float
