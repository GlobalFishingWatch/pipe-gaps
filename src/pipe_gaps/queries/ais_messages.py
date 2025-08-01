"""This module encapsulates the AIS position messages query."""
import logging
import typing
from datetime import date, datetime

import sqlparse

from pipe_gaps.common.query import Query

logger = logging.getLogger(__name__)

DB_TABLE_MESSAGES = "pipe_ais_v3_internal.research_messages"
DB_TABLE_SEGMENTS = "pipe_ais_v3_published.segs_activity"


class Message(typing.NamedTuple):
    """Schema for AIS messages.

    TODO: create this class dynamically using a JSON schema.
    https://docs.pydantic.dev/latest/concepts/models/#dynamic-model-creation
    """
    ssvid: str
    msgid: str
    seg_id: str
    timestamp: datetime
    lat: float
    lon: float
    receiver_type: str
    distance_from_shore_m: float
    distance_from_port_m: float

    def __getitem__(self, key):
        """Implement dict access interface."""
        return getattr(self, key)

    def items(self):
        return self._asdict().items()

    def keys(self):
        return self._asdict().keys()


class AISMessagesQuery(Query):
    """Encapsulates a AIS messages query.

    Args:
        start_date: start date of query.
        end_date: end date of query.
        source_messages: table with AIS messages.
        source_segments: table with AIS segments.
        ssvids: list of ssvdis to filter.
        filter_good_seg: If true, only fetch messages that belong to 'good_seg' segments.
        filter_not_overlapping_and_short: If true, only fetch messages that don't belong to
            'overlapping_and_short' segments.
    """

    NAME = "messages"

    COLUMN_MESSAGES_SSVID = "ssvid"
    COLUMN_SEGMENTS_GOOD_SEG2 = "good_seg2"
    COLUMN_SEGMENTS_OVERLAPPING_AND_SHORT = "overlapping_and_short"

    TEMPLATE = """
      SELECT
        {fields}
      FROM
        `{source_messages}`
      WHERE
        (DATE(timestamp) >= "{start_date}" AND DATE(timestamp) < "{end_date}")
        AND seg_id IN (
          SELECT
            seg_id
          FROM
            `{source_segments}`
        {segment_filters}
        )
    """

    AIS_CLASS_COLUMN = """
      (
        CASE
          WHEN type IN ('AIS.1', 'AIS.2', 'AIS.3') THEN 'A'
          WHEN type IN ('AIS.18','AIS.19') THEN 'B'
          ELSE NULL
        END
      ) as ais_class
    """

    def __init__(
        self,
        start_date: date,
        end_date: date,
        source_messages: str = DB_TABLE_MESSAGES,
        source_segments: str = DB_TABLE_SEGMENTS,
        ssvids: list = None,
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

    @classmethod
    def schema(cls):
        return Message

    def render(self):
        query = self.TEMPLATE.format(
            source_messages=self._source_messages,
            source_segments=self._source_segments,
            start_date=self._start_date,
            end_date=self._end_date,
            fields=self.select_clause(),
            segment_filters=self.segment_filters(),
        )

        if self._ssvids is not None and len(self._ssvids) > 0:
            ssvid_filter = ",".join(f'"{s}"' for s in self._ssvids)
            query = f"{query} AND {self.COLUMN_MESSAGES_SSVID} IN ({ssvid_filter})"

        logger.debug("Rendered Query for AIS messages: ")
        logger.debug(sqlparse.format(query, reindent=True, keyword_case='upper'))

        return query

    def select_clause(self):
        return f"{super().select_clause()}, {self.AIS_CLASS_COLUMN}"

    def segment_filters(self):
        filters = []

        if self._filter_good_seg:
            filters.append(self.COLUMN_SEGMENTS_GOOD_SEG2)

        if self._filter_not_overlapping_and_short:
            filters.append(f"not {self.COLUMN_SEGMENTS_OVERLAPPING_AND_SHORT}")

        return self.where_clause(filters)
