"""This module encapsulates AIS GAPS query."""
import logging
import typing
from datetime import date, datetime

import sqlparse

from pipe_gaps.common.query import Query

logger = logging.getLogger(__name__)

DB_TABLE_GAPS = "pipe_ais_v3_internal.raw_gaps"


class AISGap(typing.NamedTuple):
    """Schema for AIS gaps.

    TODO: create this class dynamically using a JSON schema.
    https://docs.pydantic.dev/latest/concepts/models/#dynamic-model-creation
    """

    gap_id: str
    ssvid: str
    ssvid: str
    version: str
    positions_hours_before: int
    positions_hours_before_ter: int
    positions_hours_before_sat: int
    positions_hours_before_dyn: int
    distance_m: float
    duration_h: float
    implied_speed_knots: float
    start_timestamp: datetime
    start_seg_id: str
    start_msgid: str
    start_lat: float
    start_lon: float
    start_ais_class: str
    start_receiver_type: str
    start_distance_from_shore_m: float
    start_distance_from_port_m: float
    end_timestamp: datetime = None
    end_msgid: str = None
    end_seg_id: str = None
    end_lat: float = None
    end_lon: float = None
    end_ais_class: str = None
    end_receiver_type: str = None
    end_distance_from_shore_m: float = None
    end_distance_from_port_m: float = None
    is_closed: bool = False

    def __getitem__(self, key):
        """Implement dict access interface."""
        return getattr(self, key)

    def items(self):
        return self._asdict().items()


class AISGapsQuery(Query):
    """Encapsulates a gaps query.

    This query will return only the last version of each gap.

    Args:
        start_date: Fetch gaps whose OFF message timestamp is >= start_date.
        end_date: Fetch gaps whose OFF message timetsamp is < end_date.
        source_gaps: Table with AIS gaps.
        ssvids: List of ssvdis to filter.
        is_closed: Whether to fetch
            - only closed gaps (True),
            - only open gaps (False)
            - both (None).
    """

    NAME = "gaps"

    COLUMN_GAP_ID = "gap_id"
    COLUMN_IS_CLOSED = "is_closed"
    COLUMN_SSVID = "ssvid"
    COLUMN_START_TIME = "start_timestamp"
    COLUMN_VERSION = "version"

    TEMPLATE = """
    SELECT
      {fields}
    FROM ({last_versions_query})
    """

    def __init__(
        self,
        start_date: date = None,
        end_date: date = None,
        source_gaps: str = DB_TABLE_GAPS,
        ssvids: list = None,
        is_closed: bool = None
    ):
        self._start_date = start_date
        self._end_date = end_date
        self._source_gaps = source_gaps
        self._ssvids = ssvids
        self._is_closed = is_closed

    @classmethod
    def schema(self):
        return AISGap

    @classmethod
    def last_versions_query(cls, source_id: str = DB_TABLE_GAPS) -> str:
        """Returns a query to get the latest versions of gaps."""
        query = f"""
            SELECT *
            FROM `{source_id}`
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY
                    {cls.COLUMN_GAP_ID},
                    {cls.COLUMN_START_TIME}
                ORDER BY {cls.COLUMN_VERSION} DESC
            ) = 1
        """

        return query

    def render(self):
        query = self.TEMPLATE.format(
            fields=self.select_clause(),
            last_versions_query=self.last_versions_query(source_id=self._source_gaps)
        )

        filters = []
        if self._ssvids is not None:
            ssvid_filter = ",".join(f'"{s}"' for s in self._ssvids)
            filters.append(f"{self.COLUMN_SSVID} IN ({ssvid_filter})")

        if self._start_date is not None:
            filters.append(f"DATE({self.COLUMN_START_TIME}) >= '{self._start_date}'")

        if self._end_date is not None:
            filters.append(f"DATE({self.COLUMN_START_TIME}) < '{self._end_date}'")

        if self._is_closed is not None:
            if self._is_closed:
                filters.append(f"{self.COLUMN_IS_CLOSED}")
            else:
                filters.append(f"not {self.COLUMN_IS_CLOSED}")

        where_clause = self.where_clause(filters)

        query = f"{query} \n {where_clause}"

        logger.debug("Rendered Query for AIS gaps: ")
        logger.debug(sqlparse.format(query, reindent=True, keyword_case='upper'))

        return query
