from dataclasses import dataclass
from datetime import date

import math

from datetime import timedelta

from gfw.common.beam.transforms import WriteToPartitionedBigQuery

from pipe_gaps.common.beam.transforms.read_from_json import ReadFromJson
from pipe_gaps.common.beam.transforms.write_to_json import WriteToJson
from pipe_gaps.common.beam.transforms.read_from_bigquery import ReadFromBigQuery

from pipe_gaps.core import GapDetector
from pipe_gaps.version import __version__
from pipe_gaps.queries import AISGapsQuery, AISMessagesQuery
from pipe_gaps.pipeline.transforms.detect_gaps import DetectGaps
from pipe_gaps.pipeline.gaps_table import GapsTableConfig
from pipe_gaps.common.config.pipeline import PipelineConfig


@dataclass
class GapsPipelineConfig(PipelineConfig):
    filter_not_overlapping_and_short: bool = False
    filter_good_seg: bool = False
    open_gaps_start_date: str = "2019-01-01"
    skip_open_gaps: bool = False
    ssvids: tuple = ()
    min_gap_length: float = 6
    n_hours_before: int = 12
    window_period_d: int = None
    eval_last: bool = True
    normalize_output: bool = True
    json_input_messages: str = None
    json_input_open_gaps: str = None
    bq_read_method: str = "EXPORT"
    bq_input_messages: str = None
    bq_input_segments: str = "pipe_ais_v3_published.segs_activity"
    bq_input_open_gaps: str = None
    bq_output_gaps: str = None
    bq_output_gaps_description: bool = False
    bq_write_disposition: str = "WRITE_APPEND"
    mock_db_client: bool = False
    save_json: bool = False
    work_dir: str = "workdir"

    def __post_init__(self):
        if (
            self.json_input_messages is None
            and (self.bq_input_messages is None or self.bq_input_segments is None)
        ):
            raise ValueError("You need to provide either a JSON inputs or BQ input.")

    @property
    def start_date(self) -> date:
        return self.parsed_date_range[0]

    @property
    def end_date(self) -> date:
        return self.parsed_date_range[1]

    @property
    def open_gaps_start(self) -> date:
        return date.fromisoformat(self.open_gaps_start_date)

    @property
    def messages_query_start_date(self):
        buffer_days = math.ceil(self.n_hours_before / 24)
        return self.start_date - timedelta(days=buffer_days)

    @property
    def read_from_bigquery_factory(self):
        return ReadFromBigQuery.get_client_factory(mocked=self.mock_db_client)

    @property
    def write_to_bigquery_factory(self):
        return WriteToPartitionedBigQuery.get_client_factory(mocked=self.mock_db_client)

    @property
    def gaps_table_config(self):
        return GapsTableConfig(
            table_id=self.bq_output_gaps,
            write_disposition=self.bq_write_disposition
        )

    @property
    def bq_output_gaps_description_params(self):
        return dict(
            # Could be as well just return ALL parameters (and remove irrelevant ones).
            bq_input_messages=self.bq_input_messages,
            bq_input_segments=self.bq_input_segments,
            filter_good_seg=self.filter_good_seg,
            filter_not_overlapping_and_short=self.filter_not_overlapping_and_short,
            min_gap_length=self.min_gap_length,
            n_hours_before=self.n_hours_before,
        )

    @property
    def sources(self):
        sources = []
        if self.json_input_messages is not None:
            sources.append(
                ReadFromJson(
                    input_file=self.json_input_messages,
                    lines=True,
                )
            )

        if self.bq_input_messages is not None:
            sources.append(
                ReadFromBigQuery(
                    query=AISMessagesQuery(
                        source_messages=self.bq_input_messages,
                        source_segments=self.bq_input_segments,
                        start_date=self.messages_query_start_date,
                        end_date=self.end_date,
                        ssvids=self.ssvids,
                        filter_not_overlapping_and_short=self.filter_not_overlapping_and_short,
                        filter_good_seg=self.filter_good_seg,
                    ),
                    method=self.bq_read_method,
                    read_from_bigquery_factory=self.read_from_bigquery_factory,
                    label="ReadAISMessages",
                ),
            )

        return sources

    @property
    def core(self):
        core_ptransform = DetectGaps(
            gap_detector=GapDetector(
                threshold=self.min_gap_length,
                normalize_output=self.normalize_output
            ),
            eval_last=self.eval_last,
            date_range=self.date_range,
            window_period_d=self.window_period_d,
            window_offset_h=self.n_hours_before,
        )

        return core_ptransform

    @property
    def side_inputs(self):
        side_inputs = None
        if not self.skip_open_gaps and self.start_date > self.open_gaps_start:
            side_inputs = ReadFromBigQuery(
                query=AISGapsQuery(
                    source_gaps=self.bq_input_open_gaps or self.bq_output_gaps,
                    start_date=self.open_gaps_start,
                    is_closed=False,
                ),
                method=self.bq_read_method,
                read_from_bigquery_factory=self.read_from_bigquery_factory,
                label="ReadOpenGaps",
            )

        return side_inputs

    @property
    def sinks(self):
        sinks = []
        if self.bq_output_gaps is not None:
            sinks.append(
                WriteToPartitionedBigQuery(
                    **self.gaps_table_config.to_dict(
                        version=__version__,
                        description_enabled=self.bq_output_gaps_description,
                        description_params=self.bq_output_gaps_description_params
                    ),
                    write_to_bigquery_factory=self.write_to_bigquery_factory,
                    label="WriteGaps"
                )
            )

        if self.save_json:
            sinks.append(
                WriteToJson(
                    output_prefix="gaps",
                    output_dir=self.work_dir,
                )
            )

        return sinks
