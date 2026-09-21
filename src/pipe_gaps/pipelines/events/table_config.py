from dataclasses import dataclass
from typing import Any, Sequence

from gfw.common.bigquery.table_config import TableConfig
from gfw.common.bigquery.table_description import TableDescription
from gfw.common.strings import collapse_paragraphs
from pipe_gaps.assets import schemas
from pipe_gaps.pipelines.raw_gaps.table_config import CAVEATS


SUMMARY = """\
We create a gap event when the period of time between
consecutive positions from a single vessel exceeds a configured threshold in hours.
The `start/end` position messages of the gap are called `OFF/ON` messages,
respectively.

When the period of time between last known position
and the last time of the current day exceeds the threshold,
we create an open gap event.
In that case, the gap will not have an `ON` message (event_end and end_* fields),
until it is closed in the future when new data arrives.
"""


@dataclass
class GapEventsTableDescription(TableDescription):
    repo_name: str = "pipe-gaps"
    title: str = "GAP EVENTS"
    subtitle: str = "𝗧𝗶𝗺𝗲 𝗴𝗮𝗽𝘀 𝗯𝗲𝘁𝘄𝗲𝗲𝗻 𝘃𝗲𝘀𝘀𝗲𝗹𝘀 𝗽𝗼𝘀𝗶𝘁𝗶𝗼𝗻𝘀"
    summary: str = collapse_paragraphs(SUMMARY)
    caveats: str = CAVEATS


@dataclass
class GapEventsTableConfig(TableConfig):
    schema_file: str = "events.json"
    partition_type: str = "MONTH"
    partition_field: str = "event_start"
    clustering_fields: tuple = ("seg_id",)
    regions: Sequence[dict[str, Any]] = ()
    """Region name/description rows from pipe-regions' registry (see `main.run`), used to build
    ``regions_mean_position``'s fields dynamically instead of hardcoding the region list.
    """

    @property
    def schema(self) -> list[dict[str, Any]]:
        schema = schemas.get_schema(self.schema_file)
        for field in schema:
            if field["name"] == "regions_mean_position":
                field["fields"] = [
                    {
                        "name": region["name"],
                        "type": "STRING",
                        "mode": "REPEATED",
                        "description": region["description"],
                    }
                    for region in self.regions
                ]
        return schema

    def view_query(self) -> str | None:
        """Returns a rendered query to create a view of this table."""

    def delete_query(self, **kwargs: Any) -> str | None:
        """Returns a rendered query to truncate gaps from start_date."""
