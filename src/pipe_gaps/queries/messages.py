"""This module encapsulates the vessels position messages query."""
import logging
from typing import Sequence, NamedTuple
from datetime import date, datetime
from functools import cached_property

from gfw.common.query import Query

logger = logging.getLogger(__name__)


class Message(NamedTuple):
    """Output type for messages, matching the result of MessagesQuery."""
    ssvid: str
    msgid: str
    seg_id: str
    timestamp: datetime  # This will be converted to FLOAT64 (seconds) in SQL
    lat: float
    lon: float
    receiver_type: str
    distance_from_shore_m: float
    distance_from_port_m: float


class MessagesQuery(Query):
    """Encapsulates a positions messages query.

    Args:
        start_date:
            start date of query.

        end_date:
            end date of query.

        source_messages:
            Table with position messages.

        source_segments:
            Table with segments.

        ssvids:
            List of ssvdis to filter.

        filter_good_seg:
            If true, only fetch messages that belong to 'good_seg' segments.

        filter_not_overlapping_and_short: If true, only fetch messages that don't belong to
            'overlapping_and_short' segments.
    """

    NAME = "messages"
    JINJA_TEMPLATE_FILENAME = "messages.sql.j2"

    def __init__(
        self,
        start_date: date,
        end_date: date,
        source_messages: str,
        source_segments: str,
        ssvids: Sequence[str] = (),
        filter_good_seg: bool = False,
        filter_not_overlapping_and_short: bool = False
    ):
        self._start_date = start_date
        self._end_date = end_date
        self._source_messages = source_messages
        self._source_segments = source_segments
        self._ssvids = ssvids
        self._filter_good_seg = filter_good_seg
        self._filter_not_overlapping_and_short = filter_not_overlapping_and_short

    @cached_property
    def output_type(cls) -> type[NamedTuple]:
        return Message

    @cached_property
    def template_filename(cls) -> type[NamedTuple]:
        return cls.JINJA_TEMPLATE_FILENAME

    @cached_property
    def template_vars(self):
        """Prepares variables to pass to the Jinja2 template."""
        return {
            "fields": self.get_select_fields(convert_datetime_to_timestamp=True),
            "source_messages": self._source_messages,
            "source_segments": self._source_segments,
            "start_date": self._start_date.isoformat(),
            "end_date": self._end_date.isoformat(),
            "ssvids": self.sql_strings(self._ssvids),
            "filter_not_overlapping_and_short": self._filter_not_overlapping_and_short,
            "filter_good_seg": self._filter_good_seg,
        }
