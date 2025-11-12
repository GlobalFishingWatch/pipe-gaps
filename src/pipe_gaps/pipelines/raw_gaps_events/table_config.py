from dataclasses import dataclass
from typing import Any

from pipe_gaps.assets import schemas

from gfw.common.bigquery.table_config import TableConfig
from gfw.common.bigquery.table_description import TableDescription


SUMMARY = """\
This is the raw gaps events table.

For more information, see https://github.com/GlobalFishingWatch/pipe-gaps.
"""  # noqa

CAVEATS = """\
⬖ TBC.
"""  # noqa


@dataclass
class RawGapsEventsTableDescription(TableDescription):
    repo_name: str = "pipe-gaps"
    title: str = "RAW GAPS EVENTS"
    subtitle: str = "𝗧𝗶𝗺𝗲 𝗴𝗮𝗽𝘀 𝗯𝗲𝘁𝘄𝗲𝗲𝗻 𝗔𝗜𝗦 𝗽𝗼𝘀𝗶𝘁𝗶𝗼𝗻𝘀"
    summary: str = SUMMARY
    caveats: str = CAVEATS


@dataclass
class RawGapsEventsTableConfig(TableConfig):
    schema_file: str = "raw_gaps_events.json"
    partition_type: str = "MONTH"
    partition_field: str = "event_start"
    clustering_fields: tuple = ("seg_id", "event_start")

    @property
    def schema(self) -> list[dict]:
        return schemas.get_schema(self.schema_file)

    def view_query(self) -> str | None:
        """Returns a rendered query to create a view of this table."""
        pass

    def delete_query(self, **kwargs: Any) -> str | None:
        """Returns a rendered query to truncate gaps from start_date."""
        pass
