"""This module encapsulates a SELECT query for AIS gaps."""
import logging
from typing import Optional, NamedTuple, Sequence
from functools import cached_property
from datetime import date, datetime

from gfw.common.query import Query

logger = logging.getLogger(__name__)

DB_TABLE_GAPS = "world-fishing-827.pipe_ais_v3_internal.raw_gaps"


class Gap(NamedTuple):
    """Schema for gaps."""

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


class GapsQuery(Query):
    """Encapsulates a gaps query.

    This query will return only the last version of each gap.

    Args:
        start_date:
            Fetch gaps whose OFF message timestamp is >= start_date.

        end_date:
            Fetch gaps whose OFF message timetsamp is < end_date.

        source_gaps:
            Table with AIS gaps.

        ssvids:
            List of ssvdis to filter.

        is_closed:
            Whether to fetch
                - only closed gaps (True),
                - only open gaps (False)
                - both (None).
    """

    NAME = "gaps"

    def __init__(
        self,
        source_gaps: str = DB_TABLE_GAPS,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        ssvids: Sequence[str] = (),
        is_closed: Optional[bool] = None
    ) -> None:
        self._start_date = start_date
        self._end_date = end_date
        self._source_gaps = source_gaps
        self._ssvids = ssvids
        self._is_closed = is_closed

    @cached_property
    def output_type(self):
        return Gap

    @cached_property
    def template_filename(self) -> str:
        return "gaps.sql.j2"

    @cached_property
    def template_vars(self) -> dict:
        start_date = self._start_date
        if start_date is not None:
            start_date = start_date.isoformat()

        end_date = self._end_date
        if end_date is not None:
            end_date = end_date.isoformat()

        return {
            "fields": self.get_select_fields(),
            "source_gaps": self._source_gaps,
            "ssvids": self.sql_strings(self._ssvids),
            "start_date": start_date,
            "end_date": end_date,
            "is_closed": self._is_closed,
        }
