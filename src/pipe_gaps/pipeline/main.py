import json
import math
import logging

from datetime import date, timedelta
from typing import Any
from types import SimpleNamespace

from gfw.common.bigquery_helper import BigQueryHelper
from gfw.common.beam.pipeline import BeamPipeline
from gfw.common.beam.transforms import WriteToPartitionedBigQuery

from pipe_gaps.common.beam.transforms.read_from_json import ReadFromJson
from pipe_gaps.common.beam.transforms.write_json import WriteJson
from pipe_gaps.common.beam.transforms.read_from_bigquery import ReadFromBigQuery

from pipe_gaps.core import GapDetector
from pipe_gaps.assets import schemas
from pipe_gaps.version import __version__
from pipe_gaps.queries import AISGapsQuery, AISMessagesQuery
from pipe_gaps.pipeline.transforms.detect_gaps import DetectGaps


logger = logging.getLogger(__name__)


BQ_TABLE_PARTITION_FIELD = "start_timestamp"
BQ_TABLE_PARTITION_TYPE = "MONTH"
BQ_TABLE_PARTITION_REQUIRE = False
BQ_TABLE_CLUSTERING_FIELDS = ["is_closed", "version", "ssvid"]
BQ_TABLE_VIEW_PREFIX = "last_versions"

BQ_TABLE_DESCRIPTION = """\
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
{params}
""" # noqa


def _create_view(source_id: str, config: dict):
    view_id = f"{source_id}_{BQ_TABLE_VIEW_PREFIX}"
    logger.info(f"Creating view: {view_id}")

    mock_client = config.get("mock_db_client") is True,
    project = config.get("project", None)

    query = AISGapsQuery.last_versions_query(source_id=source_id)
    client_factory = BigQueryHelper.get_client_factory(mocked=mock_client)
    bq_client = BigQueryHelper(client_factory=client_factory, project=project)
    return bq_client.create_view(view_id=view_id, view_query=query, exists_ok=True)


def run(config: SimpleNamespace) -> None:
    config = vars(config)
    """Builds and runs pipeline."""
    pipe, output_id = RawGapsPipeline.build(**config)

    pipe.run()
    if output_id is not None:
        _create_view(
            output_id,
            config,
        )


def build_table_description(**params):
    """Builds table description with relevant parameters."""
    return BQ_TABLE_DESCRIPTION.format(
        version=__version__,
        min_gap_length=params['min_gap_length'],
        params=json.dumps(params, indent=4)
    )


class RawGapsPipeline(BeamPipeline):
    @classmethod
    def build(
        cls,
        date_range: tuple,
        pipe_type: str = "beam",
        filter_not_overlapping_and_short: bool = False,
        filter_good_seg: bool = False,
        open_gaps_start_date: str = "2019-01-01",
        skip_open_gaps: bool = False,
        ssvids: list = (),
        min_gap_length: float = 6,
        n_hours_before: int = 12,
        window_period_d: int = None,
        eval_last: bool = True,
        normalize_output: bool = True,
        json_input_messages: str = None,
        json_input_open_gaps: str = None,
        bq_read_method: str = "EXPORT",
        bq_input_messages: str = None,
        bq_input_segments: str = "pipe_ais_v3_published.segs_activity",
        bq_input_open_gaps: str = None,
        bq_output_gaps: str = None,
        bq_output_gaps_description: bool = False,
        bq_write_disposition: str = "WRITE_APPEND",
        mock_db_client: bool = False,
        save_json: bool = False,
        work_dir: str = "workdir",
        unparsed_args: list = (),
        **kwargs: Any,
    ):
        start_date, end_date = [date.fromisoformat(x) for x in date_range]
        open_gaps_start_date = date.fromisoformat(open_gaps_start_date)

        if (
            json_input_messages is None
            and (bq_input_messages is None or bq_input_segments is None)
        ):
            raise ValueError("You need to provide either a JSON inputs or BQ input.")

        if bq_input_open_gaps is None:
            bq_input_open_gaps = bq_output_gaps

        read_from_bigquery_factory = ReadFromBigQuery.get_client_factory(mocked=mock_db_client)

        side_inputs = None
        if not skip_open_gaps and start_date > open_gaps_start_date:
            side_inputs = ReadFromBigQuery(
                query=AISGapsQuery(
                    source_gaps=bq_input_open_gaps,
                    start_date=open_gaps_start_date,
                    is_closed=False,
                ),
                method=bq_read_method,
                read_from_bigquery_factory=read_from_bigquery_factory,
                label="ReadOpenGaps",
            )

        gap_detector = GapDetector(
            threshold=min_gap_length,
            normalize_output=normalize_output
        )

        core_ptransform = DetectGaps(
            gap_detector=gap_detector,
            eval_last=eval_last,
            date_range=date_range,
            window_period_d=window_period_d,
            window_offset_h=n_hours_before,
        )

        sources = []
        sinks = []

        if json_input_messages is not None:
            sources.append(
                ReadFromJson.build(
                    input_file=json_input_messages,
                    lines=True,
                )
            )

        if bq_input_messages is not None:
            buffer_days = math.ceil(n_hours_before / 24)
            query_start_date = start_date - timedelta(days=buffer_days)

            sources.append(
                ReadFromBigQuery(
                    query=AISMessagesQuery(
                        source_messages=bq_input_messages,
                        source_segments=bq_input_segments,
                        start_date=query_start_date,
                        end_date=end_date,
                        ssvids=ssvids,
                        filter_not_overlapping_and_short=filter_not_overlapping_and_short,
                        filter_good_seg=filter_good_seg,
                    ),
                    method=bq_read_method,
                    read_from_bigquery_factory=read_from_bigquery_factory,
                    label="ReadAISMessages",
                ),
            )

        if bq_output_gaps is not None:
            description = None
            if bq_output_gaps_description:
                description = build_table_description(
                    bq_input_messages=bq_input_messages,
                    bq_input_segments=bq_input_segments,
                    filter_good_seg=filter_good_seg,
                    filter_not_overlapping_and_short=filter_not_overlapping_and_short,
                    min_gap_length=min_gap_length,
                    n_hours_before=n_hours_before,
                )

            sinks.append(
                WriteToPartitionedBigQuery(
                    table=bq_output_gaps,
                    description=description,
                    schema=schemas.get_schema("ais-gaps.json"),
                    partition_field=BQ_TABLE_PARTITION_FIELD,
                    partition_type=BQ_TABLE_PARTITION_TYPE,
                    clustering_fields=BQ_TABLE_CLUSTERING_FIELDS,
                    write_disposition=bq_write_disposition,
                    label="WriteGaps"
                )
            )

        if save_json:
            sinks.append(
                WriteJson.build(
                    output_prefix="gaps",
                    output_dir=work_dir,
                )
            )

        pipeline = cls(
            sources=sources,
            core=core_ptransform,
            sinks=sinks,
            side_inputs=side_inputs,
            name="pipe-gaps",
            version=__version__,
            unparsed_args=unparsed_args,
            **kwargs
        )

        return pipeline, bq_output_gaps
