"""This module encapsulates database queries."""
import logging
import typing
from datetime import date, datetime

from .base import Query

logger = logging.getLogger(__name__)

# DB_TABLE_MESSAGES = "pipe_production_v20201001.research_messages"
DB_TABLE_MESSAGES = "pipe_ais_v3_published.messages"
DB_TABLE_SEGMENTS = "pipe_ais_v3_published.segs_activity"


class Message(typing.NamedTuple):
    """Schema for AIS messages.

    TODO: create this class dynamically using a JSON schema.
    https://docs.pydantic.dev/latest/concepts/models/#dynamic-model-creation
    """
    ssvid: str
    msgid: str
    timestamp: datetime
    lat: float
    lon: float
    receiver_type: str
    distance_from_shore_m: float


class AISMessagesQuery(Query):
    """Encapsulates a AIS messages query.

    Args:
        start_date: start date of query.
        end_date: end date of query.
        source_messages: table with AIS messages.
        source_segments: table with AIS segments.
        ssvids: list of ssvdis to filter.
    """

    NAME = "messages"

    TEMPLATE = """
      SELECT
        {fields}
      FROM
        `{source_messages}`
      WHERE
        (DATE(timestamp) >= "{start_date}" AND DATE(timestamp) <= "{end_date}")
        AND seg_id IN (
          SELECT
            seg_id
          FROM
            `{source_segments}`
          WHERE
            good_seg
            and not overlapping_and_short)
    """

    def __init__(
        self,
        start_date: date,
        end_date: date,
        source_messages: str = DB_TABLE_MESSAGES,
        source_segments: str = DB_TABLE_SEGMENTS,
        ssvids: list = None,
    ):
        self._start_date = start_date
        self._end_date = end_date
        self._source_messages = source_messages
        self._source_segments = source_segments
        self._ssvids = ssvids

    def render(self):
        query = self.TEMPLATE.format(
            source_messages=self._source_messages,
            source_segments=self._source_segments,
            start_date=self._start_date,
            end_date=self._end_date,
            fields=self._select_clause()
        )

        if self._ssvids is not None:
            ssvid_filter = ",".join(f'"{s}"' for s in self._ssvids)
            query = f"{query} AND ssvid IN ({ssvid_filter})"

        logger.debug("Rendered Query for AIS messages: ")
        logger.debug(query)

        return query

    def schema(self):
        return Message
