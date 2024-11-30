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

    If end_date is not provied, open gaps will be returned.

    Args:
        start_date: start date of query.
        end_date: end date of query.
        source_gaps: table with AIS gaps.
        ssvids: list of ssvdis to filter.
    """

    NAME = "gaps"

    # TODO change gap_start to gap_start_timestamp
    # when we stop using research gaps table.

    TEMPLATE = """
      SELECT
        {fields}
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
            fields=self.select_clause()
        )

        if self._ssvids is not None:
            ssvid_filter = ",".join(f'"{s}"' for s in self._ssvids)
            query = f"{query} AND ssvid IN ({ssvid_filter})"

        # TODO change gap_end to gap_end_timestamp
        # when we stop using research gaps table.

        if self._end_date is not None:
            query = f"{query} AND DATE(gap_end) < '{self._end_date}'"
        else:
            query = f"{query} AND is_closed = False"

        logger.debug("Rendered Query for AIS gaps: ")
        logger.debug(query)

        return query

    def schema(self):
        return AISGap
