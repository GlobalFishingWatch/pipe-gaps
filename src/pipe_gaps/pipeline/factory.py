from gfw.common.beam.transforms import WriteToPartitionedBigQuery

from gfw.common.beam.transforms.read_from_json import ReadFromJson
from gfw.common.beam.transforms.write_to_json import WriteToJson
from gfw.common.beam.pipeline.dag.factory import LinearDagFactory
from gfw.common.beam.transforms.read_from_bigquery import ReadFromBigQuery
from gfw.common.beam.pipeline.hooks import create_view_hook, delete_events_hook

from pipe_gaps.core import GapDetector
from pipe_gaps.queries import AISGapsQuery, AISMessagesQuery
from pipe_gaps.pipeline.table_config import RawGapsTableConfig, RawGapsTableDescription
from pipe_gaps.pipeline.transforms.detect_gaps import DetectGaps
from pipe_gaps.pipeline.config import RawGapsConfig


class RawGapsLinearDagFactory(LinearDagFactory):
    def __init__(self, config: RawGapsConfig):
        self.config = config

    @property
    def raw_gaps_table_config(self):
        """Returns configuration for the output gaps BigQuery table."""
        return RawGapsTableConfig(
            table_id=self.config.bq_output_gaps,
            description=RawGapsTableDescription(
                version=self.config.version,
                relevant_params=self.bq_output_gaps_description_params
            ),
        )

    @property
    def bq_output_gaps_description_params(self):
        """Returns Parameters to be included in the description of the BigQuery output table."""
        # Could be as well just return ALL parameters (and remove irrelevant ones).
        return dict(
            bq_input_messages=self.config.bq_input_messages,
            bq_input_segments=self.config.bq_input_segments,
            filter_good_seg=self.config.filter_good_seg,
            filter_not_overlapping_and_short=self.config.filter_not_overlapping_and_short,
            min_gap_length=self.config.min_gap_length,
            n_hours_before=self.config.n_hours_before,
        )

    @property
    def sources(self):
        # Constructs the list of source PTransforms based on config.
        sources = []
        if self.config.json_input_messages is not None:
            sources.append(
                ReadFromJson(
                    input_file=self.config.json_input_messages,
                    lines=True,
                )
            )

        if self.config.bq_input_messages is not None:
            query = AISMessagesQuery(
                source_messages=self.config.bq_input_messages,
                source_segments=self.config.bq_input_segments,
                start_date=self.config.messages_query_start_date,
                end_date=self.config.end_date,
                ssvids=self.config.ssvids,
                filter_not_overlapping_and_short=self.config.filter_not_overlapping_and_short,
                filter_good_seg=self.config.filter_good_seg,
            ).with_env(self.config.jinja_env)

            sources.append(
                ReadFromBigQuery.from_query(
                    query,
                    method=self.config.bq_read_method,
                    read_from_bigquery_factory=self.read_from_bigquery_factory,
                    label="ReadAISMessages",
                ),
            )

        return sources

    @property
    def core(self):
        # Core PTransform: Detect gaps in the AIS data.
        core_ptransform = DetectGaps(
            gap_detector=GapDetector(
                threshold=self.config.min_gap_length,
                normalize_output=self.config.normalize_output,
            ),
            eval_last=self.config.eval_last,
            date_range=self.config.date_range,
            window_period_d=self.config.window_period_d,
            window_offset_h=self.config.n_hours_before,
        )
        return core_ptransform

    @property
    def side_inputs(self):
        """Returns side inputs for the core transform (open gaps data)."""
        side_inputs = None
        if (
            not self.config.skip_open_gaps
            and self.config.start_date > self.config.open_gaps_start
        ):
            side_inputs = ReadFromBigQuery.from_query(
                query=AISGapsQuery(
                    source_gaps=self.config.bq_input_open_gaps or self.config.bq_output_gaps,
                    start_date=self.config.open_gaps_start,
                    is_closed=False,
                ).with_env(self.config.jinja_env),
                method=self.config.bq_read_method,
                read_from_bigquery_factory=self.read_from_bigquery_factory,
                label="ReadOpenGaps",
            )
        return side_inputs

    @property
    def sinks(self):
        # Constructs the list of sink PTransforms based on config.
        sinks = []
        if self.config.bq_output_gaps is not None:
            sinks.append(
                WriteToPartitionedBigQuery(
                    **self.raw_gaps_table_config.to_bigquery_params(
                        include_description=self.config.bq_output_gaps_description,
                    ),
                    write_to_bigquery_factory=self.write_to_bigquery_factory,
                    bigquery_helper_factory=self.bigquery_helper_factory,
                    label="WriteGaps",
                )
            )

        if self.config.save_json:
            sinks.append(
                WriteToJson(
                    output_prefix="gaps",
                    output_dir=self.config.work_dir,
                )
            )

        return sinks

    @property
    def pre_hooks(self):
        return [
            delete_events_hook(
                table_config=self.raw_gaps_table_config,
                start_date=self.config.start_date,
                mock=self.config.mock_bq_clients
            )
        ]

    @property
    def post_hooks(self):
        return [
            create_view_hook(
                table_config=self.raw_gaps_table_config,
                mock=self.config.mock_bq_clients
            )
        ]
