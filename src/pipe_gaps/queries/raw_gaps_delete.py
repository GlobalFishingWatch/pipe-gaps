"""This module encapsulates a DELETE query for raw gaps."""
import logging
from functools import cached_property
from datetime import date

from gfw.common.query import Query

logger = logging.getLogger(__name__)


class RawGapsDeleteQuery(Query):
    """Encapsulates a DELETE query for raw gaps.

    Deletes gaps that will be re-created during daily processing.
    This enables proper backfills from a given start_date to the present.

    Args:
        source_gaps:
            Table with raw gaps to delete.

        start_date:
            Delete gaps if they satisfy one of the following conditions:
                - The gap is closed and end_timestamp >= start_date.
                - The gap is open and start_timestamp >= start_date.
    """

    NAME = "raw_gaps_delete"

    def __init__(self, source_gaps: str, start_date: date) -> None:
        self._start_date = start_date
        self._source_gaps = source_gaps

    @cached_property
    def output_type(self):
        return None

    @cached_property
    def template_filename(self) -> str:
        return "raw_gaps_delete.sql.j2"

    @cached_property
    def template_vars(self) -> dict:
        return {
            "source_gaps": self._source_gaps,
            "start_date": self._start_date.isoformat(),
        }
