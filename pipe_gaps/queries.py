"""This module encapsulates database queries."""
import logging
import typing
from datetime import date
from abc import ABC, abstractmethod


logger = logging.getLogger(__name__)

# DB_TABLE_MESSAGES = "pipe_production_v20201001.research_messages"
DB_TABLE_MESSAGES = "pipe_ais_v3_published.messages"
DB_TABLE_SEGMENTS = "pipe_ais_v3_published.segs_activity"


def get_query(query_name, query_params):
    q = Query.subclasses()

    if query_name not in q:
        raise NotImplementedError(f"Query with name '{query_name}' not implemented.")

    return q[query_name](**query_params)


class QueryError(Exception):
    pass


class Query(ABC):
    @abstractmethod
    def render():
        """Renders query."""

    @classmethod
    def subclasses(cls):
        return {x.NAME: x for x in cls.__subclasses__()}


class Message(typing.NamedTuple):
    """Schema for AIS messages."""

    ssvid: str
    seg_id: str
    msgid: str
    timestamp: float


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
      ssvid,
      seg_id,
      msgid,
      CAST(UNIX_MICROS(timestamp) AS FLOAT64) / 1000000  AS timestamp,
      # lat,
      # lon,
      # course,
      # speed_knots,
      # type,
      # receiver_type,
      # regions,
      distance_from_shore_m,
      # distance_from_port_m
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
        )

        if self._ssvids is not None:
            ssvid_filter = ",".join(f'"{s}"' for s in self._ssvids)
            query = f"{query} AND ssvid IN ({ssvid_filter})"

        logger.debug("Rendered Query for AIS messages: ")
        logger.debug(query)

        return query

    @staticmethod
    def schema():
        return Message
