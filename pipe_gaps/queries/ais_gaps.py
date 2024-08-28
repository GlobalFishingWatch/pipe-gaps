"""This module encapsulates database queries."""
import logging
import typing
from datetime import date

from .base import Query

logger = logging.getLogger(__name__)

DB_TABLE_GAPS = "pipe_ais_v3_published.product_events_ais_gaps"


class AISGap(typing.NamedTuple):
    """Schema for AIS gaps."""

    ssvid: str
    gap_id: str
    gap_start_msgid: str
    gap_start: float
    is_open: bool
    gap_start_distance_from_shore_m: float


class AISGapsQuery(Query):
    """Encapsulates a gaps query.

    If end_date is not provied, open gaps will be returned.

    Args:
        start_date: start date of query.
        end_date: end date of query.
        source_gaps: table with AIS gaps.
        ssvids: list of ssvdis to filter.
    """

    NAME = "gaps"

    TEMPLATE = """
      SELECT
        ssvid,
        gap_id,
        CAST(UNIX_MICROS(gap_start) AS FLOAT64) / 1000000  AS gap_start,
        gap_start_msgid,
        gap_start_distance_from_shore_m,
        is_closed
      FROM `{source_gaps}`
      WHERE
          DATE(gap_start) >= "{start_date}"
    """

    def __init__(
        self,
        start_date: date,
        end_date: date = None,
        source_gaps: str = DB_TABLE_GAPS,
        ssvids: list = None,
    ):
        self._start_date = start_date
        self._end_date = end_date
        self._source_gaps = source_gaps
        self._ssvids = ssvids

    def render(self):
        query = self.TEMPLATE.format(
            source_gaps=self._source_gaps,
            start_date=self._start_date,
        )

        if self._ssvids is not None:
            ssvid_filter = ",".join(f'"{s}"' for s in self._ssvids)
            query = f"{query} AND ssvid IN ({ssvid_filter})"

        if self._end_date is not None:
            query = f"{query} AND AND DATE(gap_end) <= '{self._end_date}'"
        else:
            query = f"{query} AND is_closed = False"

        logger.debug("Rendered Query for AIS gaps: ")
        logger.debug(query)

        return query

    @staticmethod
    def schema():
        return AISGap
