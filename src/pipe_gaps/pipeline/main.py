import math
import logging

from datetime import timedelta

from gfw.common.bigquery_helper import BigQueryHelper
from gfw.common.beam.pipeline import Pipeline
from gfw.common.beam.pipeline.dag import LinearDag
from gfw.common.beam.transforms import WriteToPartitionedBigQuery

from pipe_gaps.common.beam.transforms.read_from_json import ReadFromJson
from pipe_gaps.common.beam.transforms.write_to_json import WriteToJson
from pipe_gaps.common.beam.transforms.read_from_bigquery import ReadFromBigQuery

from pipe_gaps.core import GapDetector
from pipe_gaps.version import __version__
from pipe_gaps.queries import AISGapsQuery, AISMessagesQuery
from pipe_gaps.pipeline.transforms.detect_gaps import DetectGaps
from pipe_gaps.pipeline.gaps_table import GapsTableConfig
from pipe_gaps.pipeline.config import GapsPipelineConfig


logger = logging.getLogger(__name__)


def run(
    unknown_unparsed_args: list = None,
    unknown_parsed_args: dict = None,
    **config,
) -> None:
    config = GapsPipelineConfig(
        beam_args=unknown_unparsed_args,
        beam_options=unknown_parsed_args,
        **config
    )

    if (
        config.json_input_messages is None
        and (config.bq_input_messages is None or config.bq_input_segments is None)
    ):
        raise ValueError("You need to provide either a JSON inputs or BQ input.")

    mock_bq = config.mock_db_client

    read_from_bq_factory = ReadFromBigQuery.get_client_factory(mocked=mock_bq)
    write_to_bq_factory = WriteToPartitionedBigQuery.get_client_factory(mocked=mock_bq)

    gaps_table_config = GapsTableConfig(table_id=config.bq_output_gaps)

    start_date, end_date = config.parsed_date_range
    open_gaps_start_date = config.parsed_open_gaps_start_date

    sources = []
    side_inputs = None
    sinks = []

    if not config.skip_open_gaps and start_date > open_gaps_start_date:
        side_inputs = ReadFromBigQuery(
            query=AISGapsQuery(
                source_gaps=config.bq_input_open_gaps or config.bq_output_gaps,
                start_date=open_gaps_start_date,
                is_closed=False,
            ),
            method=config.bq_read_method,
            read_from_bigquery_factory=read_from_bq_factory,
            label="ReadOpenGaps",
        )

    gap_detector = GapDetector(
        threshold=config.min_gap_length,
        normalize_output=config.normalize_output
    )

    core_ptransform = DetectGaps(
        gap_detector=gap_detector,
        eval_last=config.eval_last,
        date_range=config.date_range,
        window_period_d=config.window_period_d,
        window_offset_h=config.n_hours_before,
    )

    if config.json_input_messages is not None:
        sources.append(
            ReadFromJson(
                input_file=config.json_input_messages,
                lines=True,
            )
        )

    if config.bq_input_messages is not None:
        buffer_days = math.ceil(config.n_hours_before / 24)
        query_start_date = start_date - timedelta(days=buffer_days)

        sources.append(
            ReadFromBigQuery(
                query=AISMessagesQuery(
                    source_messages=config.bq_input_messages,
                    source_segments=config.bq_input_segments,
                    start_date=query_start_date,
                    end_date=end_date,
                    ssvids=config.ssvids,
                    filter_not_overlapping_and_short=config.filter_not_overlapping_and_short,
                    filter_good_seg=config.filter_good_seg,
                ),
                method=config.bq_read_method,
                read_from_bigquery_factory=read_from_bq_factory,
                label="ReadAISMessages",
            ),
        )

    if config.bq_output_gaps is not None:
        if config.bq_output_gaps_description:
            description = gaps_table_config.description(
                version=__version__,
                bq_input_messages=config.bq_input_messages,
                bq_input_segments=config.bq_input_segments,
                filter_good_seg=config.filter_good_seg,
                filter_not_overlapping_and_short=config.filter_not_overlapping_and_short,
                min_gap_length=config.min_gap_length,
                n_hours_before=config.n_hours_before,
            )

        sinks.append(
            WriteToPartitionedBigQuery(
                description=description,
                **gaps_table_config.bigquery_params(),
                write_disposition=config.bq_write_disposition,
                write_to_bigquery_factory=write_to_bq_factory,
                label="WriteGaps"
            )
        )

    if config.save_json:
        sinks.append(
            WriteToJson(
                output_prefix="gaps",
                output_dir=config.work_dir,
            )
        )

    dag = LinearDag(
        sources=sources,
        core=core_ptransform,
        sinks=sinks,
        side_inputs=side_inputs,
    )

    pipeline = Pipeline(
        name="pipe-gaps",
        version=__version__,
        dag=dag,
        unparsed_args=config.beam_args,
        **config.beam_options
    )

    result, _ = pipeline.run()

    if config.bq_output_gaps is not None:
        view_id = gaps_table_config.view_id
        logger.info(f"Creating view: {view_id}")

        query = AISGapsQuery.last_versions_query(source_id=gaps_table_config.table_id)
        client_factory = BigQueryHelper.get_client_factory(mocked=config.mock_db_client)
        bq_client = BigQueryHelper(client_factory=client_factory)
        return bq_client.create_view(view_id=view_id, view_query=query, exists_ok=True)
