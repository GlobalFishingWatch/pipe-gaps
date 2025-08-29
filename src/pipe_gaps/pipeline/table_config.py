from dataclasses import dataclass

from pipe_gaps.assets import schemas
from pipe_gaps.queries import AISGapsQuery
from gfw.common.bigquery.table_config import TableConfig
from gfw.common.bigquery.table_description import TableDescription



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
class RawGapsTableDescription(TableDescription):
    repo_name: str = "pipe-gaps"
    title: str = "RAW GAPS"
    subtitle: str = "𝗧𝗶𝗺𝗲 𝗴𝗮𝗽𝘀 𝗯𝗲𝘁𝘄𝗲𝗲𝗻 𝗔𝗜𝗦 𝗽𝗼𝘀𝗶𝘁𝗶𝗼𝗻𝘀"
    summary: str = SUMMARY
    caveats: str = CAVEATS


@dataclass
class RawGapsTableConfig(TableConfig):
    schema_file: str = "ais-gaps.json"
    view_suffix: str = "last_versions"
    partition_type: str = "MONTH"
    partition_field: str = "start_timestamp"
    clustering_fields: tuple = ("is_closed", "version", "ssvid")

    @property
    def schema(self):
        return schemas.get_schema(self.schema_file)

    def view_query(self):
        """Returns a rendered query to create a view of this table."""
        return AISGapsQuery(source_gaps=self.table_id).render()
