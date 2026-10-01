from dataclasses import dataclass
from datetime import date
from typing import Optional

from gfw.common.bigquery.table_config import TableConfig
from gfw.common.bigquery.table_description import TableDescription
from gfw.common.bigquery.view_config import SingleSourceViewConfig
from pipe_gaps.assets import schemas
from pipe_gaps.queries import GapsDeleteQuery, GapsQuery


SUMMARY = """\
The gaps in this table are versioned. This means that open gaps are closed by inserting a new row with different timestamp (𝘃𝗲𝗿𝘀𝗶𝗼𝗻 field).
Thus, two rows with the same 𝗴𝗮𝗽_𝗶𝗱 can coexist: one for the previous open gap and one for the current closed gap.
The 𝗴𝗮𝗽_𝗶𝗱 is MD5 hash of [𝘀𝘀𝘃𝗶𝗱, 𝘀𝘁𝗮𝗿𝘁_𝘁𝗶𝗺𝗲𝘀𝘁𝗮𝗺𝗽, 𝘀𝘁𝗮𝗿𝘁_𝗹𝗮𝘁, 𝘀𝘁𝗮𝗿𝘁_𝗹𝗼𝗻].
"""  # noqa

CAVEATS = """\
⬖ Gaps are generated based on 𝘀𝘀𝘃𝗶𝗱 so a single gap can refer to two different 𝘃𝗲𝘀𝘀𝗲𝗹_𝗶𝗱.
⬖ Gaps are generated based on position messages that are filtered by 𝗴𝗼𝗼𝗱_𝘀𝗲𝗴𝟮 field of the segments table in order to remove noise.
⬖ Gaps are generated based on position messages that are not filtered by not 𝗼𝘃𝗲𝗿𝗹𝗮𝗽𝗽𝗶𝗻𝗴_𝗮𝗻𝗱_𝘀𝗵𝗼𝗿𝘁 field of the segments table.
"""  # noqa


@dataclass
class GapsTableDescription(TableDescription):
    repo_name: str = "pipe-gaps"
    title: str = "GAPS"
    subtitle: str = "𝗧𝗶𝗺𝗲 𝗴𝗮𝗽𝘀 𝗯𝗲𝘁𝘄𝗲𝗲𝗻 𝘃𝗲𝘀𝘀𝗲𝗹𝘀 𝗽𝗼𝘀𝗶𝘁𝗶𝗼𝗻𝘀"
    summary: str = SUMMARY
    caveats: str = CAVEATS


LAST_VERSIONS_SUMMARY = """\
This view returns only the 𝗹𝗮𝘀𝘁 𝘃𝗲𝗿𝘀𝗶𝗼𝗻 of each gap from the 𝗿𝗮𝘄_𝗴𝗮𝗽𝘀 table: for a closed gap,
its closed row; for a still-open gap, its most recent open row. Unlike 𝗿𝗮𝘄_𝗴𝗮𝗽𝘀, a 𝗴𝗮𝗽_𝗶𝗱 never
repeats here -- query this view instead of 𝗿𝗮𝘄_𝗴𝗮𝗽𝘀 unless you specifically need every historical
version of a gap (e.g. to see what it looked like before it closed).
"""  # noqa

LAST_VERSIONS_CAVEATS = """\
⬖ Gaps whose duration is below the configured 𝗺𝗶𝗻_𝗴𝗮𝗽_𝗹𝗲𝗻𝗴𝘁𝗵 threshold are filtered out of this
  view entirely, to exclude invalid gaps produced during reprocessing with new data. 𝗿𝗮𝘄_𝗴𝗮𝗽𝘀
  itself has no such filter.
"""  # noqa


@dataclass
class GapsLastVersionsTableDescription(TableDescription):
    repo_name: str = "pipe-gaps"
    title: str = "GAPS -- LAST VERSIONS"
    subtitle: str = "𝗟𝗮𝘁𝗲𝘀𝘁 𝘃𝗲𝗿𝘀𝗶𝗼𝗻 𝗼𝗳 𝗲𝗮𝗰𝗵 𝗴𝗮𝗽, 𝗱𝗲𝗱𝘂𝗽𝗹𝗶𝗰𝗮𝘁𝗲𝗱 𝗳𝗿𝗼𝗺 𝗿𝗮𝘄_𝗴𝗮𝗽𝘀"
    summary: str = LAST_VERSIONS_SUMMARY
    caveats: str = LAST_VERSIONS_CAVEATS


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
class GapsLastVersionsViewConfig(SingleSourceViewConfig):
    suffix: str = "last_versions"

    # Gaps below this threshold are filtered from the view to exclude invalid gaps
    # produced during reprocessing with new data.
    min_gap_length: Optional[float] = None

    def view_query(self):
        """Returns a rendered query to create this view."""
        return GapsQuery(
            source_gaps=self.source.table_id, min_gap_length=self.min_gap_length
        ).render()
