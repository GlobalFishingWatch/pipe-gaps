"""This module encapsulates AIS GAPS query."""
import logging
import typing

from datetime import date, datetime

from .base import Query

logger = logging.getLogger(__name__)

DB_TABLE_GAPS = "pipe_ais_v3_published.product_events_ais_gaps"


class AISGap(typing.NamedTuple):
    """Schema for AIS gaps.

    TODO: create this class dynamically using a JSON schema.
    https://docs.pydantic.dev/latest/concepts/models/#dynamic-model-creation
    """

    gap_id: str
    gap_ssvid: str
    gap_start_timestamp: datetime
    gap_start_msgid: str
    gap_start_lat: float
    gap_start_lon: float
    gap_start_receiver_type: str
    gap_start_distance_from_shore_m: float
    gap_start_distance_from_port_m: float
    gap_end_timestamp: datetime = None
    gap_end_msgid: str = None
    gap_end_lat: float = None
    gap_end_lon: float = None
    gap_end_receiver_type: str = None
    gap_end_distance_from_shore_m: float = None
    is_closed: bool = False

    def __getitem__(self, key):
        """Implement dict access interface."""
        return getattr(self, key)

    def items(self):
        return self._asdict().items()


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
        {fields}
      FROM `{source_gaps}`
      WHERE
          DATE(gap_start_timestamp) >= "{start_date}"
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
            fields=self._select_clause()
        )

        if self._ssvids is not None:
            ssvid_filter = ",".join(f'"{s}"' for s in self._ssvids)
            query = f"{query} AND ssvid IN ({ssvid_filter})"

        if self._end_date is not None:
            query = f"{query} AND AND DATE(gap_end) < '{self._end_date}'"
        else:
            query = f"{query} AND is_closed = False"

        logger.debug("Rendered Query for AIS gaps: ")
        logger.debug(query)

        return query

    def schema(self):
        return AISGap
