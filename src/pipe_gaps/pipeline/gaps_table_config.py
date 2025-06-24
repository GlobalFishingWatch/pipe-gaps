from dataclasses import dataclass

from pipe_gaps.assets import schemas
from pipe_gaps.queries import AISGapsQuery
from pipe_gaps.common.config.bigquery_table_config import BigQueryTableConfig


PARTITION_FIELD = "start_timestamp"
PARTITION_TYPE = "MONTH"
CLUSTERING_FIELDS = ("is_closed", "version", "ssvid")
VIEW_SUFFIX = "last_versions"
SCHEMA_FILE = "ais-gaps.json"

DESCRIPTION_TEMPLATE = """\
「 ✦ 𝚁𝙰𝚆 𝙶𝙰𝙿𝚂 ✦ 」 
𝗧𝗶𝗺𝗲 𝗴𝗮𝗽𝘀 𝗯𝗲𝘁𝘄𝗲𝗲𝗻 𝗔𝗜𝗦 𝗽𝗼𝘀𝗶𝘁𝗶𝗼𝗻𝘀.
⬖ Created by pipe-gaps: v{version}.
⬖ https://github.com/GlobalFishingWatch/pipe-gaps

𝗦𝘂𝗺𝗺𝗮𝗿𝘆
Each row in this table is created when the period of time between two consecutive AIS positions from a single vessel exceeds {min_gap_length} hours.
When the period of time between last known position and the last time of the current day exceeds {min_gap_length} hours, we create an open gap.
In that case, the gap will not have a 𝗲𝗻𝗱_𝘁𝗶𝗺𝗲𝘀𝘁𝗮𝗺𝗽, until it is closed in the future when new data arrives.

The gaps in this table are versioned. This means that open gaps are closed by inserting a new row with different timestamp (𝘃𝗲𝗿𝘀𝗶𝗼𝗻 field).
Thus, two rows with the same 𝗴𝗮𝗽_𝗶𝗱 can coexist: one for the previous open gap and one for the current closed gap.
The 𝗴𝗮𝗽_𝗶𝗱 is MD5 hash of [𝘀𝘀𝘃𝗶𝗱, 𝘀𝘁𝗮𝗿𝘁_𝘁𝗶𝗺𝗲𝘀𝘁𝗮𝗺𝗽, 𝘀𝘁𝗮𝗿𝘁_𝗹𝗮𝘁, 𝘀𝘁𝗮𝗿𝘁_𝗹𝗼𝗻].

𝗖𝗮𝘃𝗲𝗮𝘁𝘀
⬖ Gaps are generated based on 𝘀𝘀𝘃𝗶𝗱 so a single gap can refer to two different 𝘃𝗲𝘀𝘀𝗲𝗹_𝗶𝗱.
⬖ Gaps are generated based on position messages that are filtered by 𝗴𝗼𝗼𝗱_𝘀𝗲𝗴𝟮 field of the segments table in order to remove noise.
⬖ Gaps are generated based on position messages that are not filtered by not 𝗼𝘃𝗲𝗿𝗹𝗮𝗽𝗽𝗶𝗻𝗴_𝗮𝗻𝗱_𝘀𝗵𝗼𝗿𝘁 field of the segments table.

For more information, see https://github.com/GlobalFishingWatch/pipe-gaps/blob/develop/README.md.

𝗥𝗲𝗹𝗲𝘃𝗮𝗻𝘁 𝗽𝗮𝗿𝗮𝗺𝗲𝘁𝗲𝗿𝘀
{params_json}
""" # noqa


@dataclass
class GapsTableConfig(BigQueryTableConfig):
    schema_file: str = SCHEMA_FILE
    view_suffix: str = VIEW_SUFFIX
    partition_type: str = PARTITION_TYPE
    partition_field: str = PARTITION_FIELD
    clustering_fields: tuple = CLUSTERING_FIELDS
    description_template: str = DESCRIPTION_TEMPLATE

    @property
    def schema(self):
        return schemas.get_schema(self.schema_file)

    @property
    def view_query(self):
        return AISGapsQuery.last_versions_query(source_id=self.table_id)
