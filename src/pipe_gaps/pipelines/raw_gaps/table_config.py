from dataclasses import dataclass, field
from datetime import date
from typing import Optional

from gfw.common.bigquery.table_config import TableConfig
from gfw.common.bigquery.table_description import TableDescription
from gfw.common.bigquery.view_config import SingleSourceViewConfig
from pipe_gaps.assets import schemas
from pipe_gaps.queries import GapsDeleteQuery, GapsQuery


COMMON_SUMMARY = """\
A gap is created when the time between consecutive position reports from a vessel (AIS or VMS)
exceeds a configured threshold. These are raw gaps: no judgment is made here about their cause --
whether reporting was intentionally disabled is evaluated separately, downstream, using gaps
from this table as input. Its start/end position messages are called 𝗢𝗙𝗙/𝗢𝗡 messages,
respectively; one with no 𝗢𝗡 message yet is an 𝗼𝗽𝗲𝗻 gap, until it's closed once new data arrives.

Open gaps are closed by inserting a new row with a different timestamp (𝘃𝗲𝗿𝘀𝗶𝗼𝗻 field). The
𝗴𝗮𝗽_𝗶𝗱 is MD5 hash of [𝘀𝘀𝘃𝗶𝗱, 𝘀𝘁𝗮𝗿𝘁_𝘁𝗶𝗺𝗲𝘀𝘁𝗮𝗺𝗽, 𝘀𝘁𝗮𝗿𝘁_𝗹𝗮𝘁, 𝘀𝘁𝗮𝗿𝘁_𝗹𝗼𝗻].
"""  # noqa

SUMMARY_VERSIONED = (
    COMMON_SUMMARY
    + """\
Two rows with the same 𝗴𝗮𝗽_𝗶𝗱 can coexist in this table: one for the previous open gap and one
for the current closed gap.
"""  # noqa
)

CAVEATS_VERSIONED = """\
⬖ Gaps are generated based on 𝘀𝘀𝘃𝗶𝗱 so a single gap can refer to two different 𝘃𝗲𝘀𝘀𝗲𝗹_𝗶𝗱.
⬖ Gaps are generated based on position messages that are filtered by 𝗴𝗼𝗼𝗱_𝘀𝗲𝗴𝟮 field of the segments table in order to remove noise.
⬖ Gaps are generated based on position messages that are not filtered by not 𝗼𝘃𝗲𝗿𝗹𝗮𝗽𝗽𝗶𝗻𝗴_𝗮𝗻𝗱_𝘀𝗵𝗼𝗿𝘁 field of the segments table.
"""  # noqa


@dataclass
class GapsVersionedTableDescription(TableDescription):
    repo_name: str = "pipe-gaps"
    title: str = "GAPS"
    subtitle: str = "𝗧𝗶𝗺𝗲 𝗴𝗮𝗽𝘀 𝗯𝗲𝘁𝘄𝗲𝗲𝗻 𝘃𝗲𝘀𝘀𝗲𝗹𝘀 𝗽𝗼𝘀𝗶𝘁𝗶𝗼𝗻𝘀"
    summary: str = SUMMARY_VERSIONED
    caveats: str = CAVEATS_VERSIONED


SUMMARY = (
    COMMON_SUMMARY
    + """\
This view keeps only the gap's most recent state: its closed row once it's closed, or its
most recent open row while still open. For older versions of a gap (e.g. what it looked like
before it closed), see {source_table}.
"""  # noqa
)

CAVEATS = (
    CAVEATS_VERSIONED
    + """\
⬖ Gaps whose duration is below the configured 𝗺𝗶𝗻_𝗴𝗮𝗽_𝗹𝗲𝗻𝗴𝘁𝗵 threshold are filtered out of this
  view entirely, to exclude invalid gaps produced during reprocessing with new data.
"""  # noqa
)


@dataclass(kw_only=True)
class GapsTableDescription(GapsVersionedTableDescription):
    source_table: str
    """Name of the table this view is built on top of, substituted into the summary/caveats
    below -- never hardcoded, since it's expected to change (e.g. after a table rename)."""

    summary: str = field(init=False)
    caveats: str = field(init=False)

    def __post_init__(self) -> None:
        self.summary = SUMMARY.format(source_table=self.source_table)
        self.caveats = CAVEATS.format(source_table=self.source_table)


@dataclass
class GapsTableConfig(TableConfig):
    schema_file: str = "gaps.json"
    partition_type: str = "MONTH"
    partition_field: str = "start_timestamp"
    clustering_fields: tuple = ("is_closed", "version", "ssvid")

    @property
    def schema(self):
        return schemas.get_schema(self.schema_file)

    def delete_query(self, start_date: date, end_date: Optional[date] = None) -> str:
        """Returns a rendered query to truncate gaps from start_date."""
        query = GapsDeleteQuery(source_gaps=self.table_id, start_date=start_date)
        return query.render()


@dataclass
class GapsLatestViewConfig(SingleSourceViewConfig):
    suffix: str = "latest"

    # Gaps below this threshold are filtered from the view to exclude invalid gaps
    # produced during reprocessing with new data.
    min_gap_length: Optional[float] = None

    def view_query(self):
        """Returns a rendered query to create this view."""
        return GapsQuery(
            source_gaps=self.source.table_id, min_gap_length=self.min_gap_length
        ).render()
