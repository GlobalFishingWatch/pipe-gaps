"""This module encapsulates a SELECT query for gaps."""
import dataclasses
import logging

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import NamedTuple, Optional, Sequence

from gfw.common.query import Query

logger = logging.getLogger(__name__)


class Gap(NamedTuple):
    """Schema for gaps."""

    gap_id: str
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


@dataclass
class GapsQuery(Query):
    """Last version of each gap, with TIMESTAMP columns (e.g. for the raw_gaps view).

    Its fields are the template's variables. Subclasses change what is read through is_closed,
    which isn't a constructor argument, and the timestamps_as_seconds property.

    Args:
        source_gaps:
            Table with gaps.

        start_date:
            Fetch gaps whose OFF message timestamp is >= start_date.

        end_date:
            Fetch gaps whose ON message timestamp is < end_date.

        ssvids:
            List of ssvids to filter.

        min_gap_length:
            If provided, filter out closed gaps whose duration_h is <= this threshold (in hours).

        is_closed:
            Not a constructor argument: set by each class. Read only open gaps (False) or all of
            them, each in its last version (None).
    """

    source_gaps: str
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    ssvids: Sequence[str] = ()
    min_gap_length: Optional[float] = None
    is_closed: Optional[bool] = field(default=None, init=False)

    @property
    def template_filename(self) -> str:
        return "gaps.sql.j2"

    @property
    def output_type(self) -> type[Gap]:
        return Gap

    @property
    def timestamps_as_seconds(self) -> bool:
        """Whether to return timestamps as Unix seconds instead of TIMESTAMPs."""
        return False

    @property
    def template_vars(self) -> dict:
        return {
            **dataclasses.asdict(self),
            "ssvids": self.sql_strings(self.ssvids),
            "fields": self.get_select_fields(self.timestamps_as_seconds),
        }


@dataclass
class OpenGapsQuery(GapsQuery):
    """Gaps still open, for the raw-gaps side input.

    Reads the open rows with all columns and only the ids of the closed ones, so the table's
    clustering on is_closed prunes the rest (see PIPELINE-4670). Timestamps are Unix seconds, as
    the gap detector expects.
    """

    is_closed: Optional[bool] = field(default=False, init=False)

    @property
    def timestamps_as_seconds(self) -> bool:
        return True
