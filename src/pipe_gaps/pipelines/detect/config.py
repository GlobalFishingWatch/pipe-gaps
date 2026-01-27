from __future__ import annotations
import math
from dataclasses import dataclass, field
from datetime import date, timedelta

from gfw.common.beam.pipeline.config import PipelineConfig
from gfw.common.beam.pipeline.hooks import create_view_hook, delete_events_hook, create_table_hook

from pipe_gaps.pipelines.detect.table_config import GapsTableConfig, GapsTableDescription


@dataclass(frozen=True)
class DetectGapsConfig(PipelineConfig):
    filter_not_overlapping_and_short: bool = False
    filter_good_seg: bool = False
    open_gaps_start_date: str = "2019-01-01"
    skip_open_gaps: bool = False
    ssvids: tuple = field(default_factory=tuple)
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
    bq_write_disposition: str = "WRITE_APPEND"
    mock_bq_clients: bool = False
    save_json: bool = False
    work_dir: str = "workdir"

    def __post_init__(self) -> None:
        self.validate()

    @property
    def open_gaps_start(self) -> date:
        return date.fromisoformat(self.open_gaps_start_date)

    @property
    def messages_query_start_date(self) -> date:
        buffer_days = math.ceil(self.n_hours_before / 24)
        return self.start_date - timedelta(days=buffer_days)

    @property
    def table_config(self):
        """Returns configuration for the output gaps BigQuery table."""
        return GapsTableConfig(
            table_id=self.bq_output_gaps,
            description=GapsTableDescription(
                version=self.version,
                relevant_params=self.bq_output_gaps_description_params
            ),
        )

    @property
    def bq_output_gaps_description_params(self):
        """Returns Parameters to be included in the description of the BigQuery output table."""
        # Could be as well just return ALL parameters (and remove irrelevant ones).
        return dict(
            bq_input_messages=self.bq_input_messages,
            bq_input_segments=self.bq_input_segments,
            filter_good_seg=self.filter_good_seg,
            filter_not_overlapping_and_short=self.filter_not_overlapping_and_short,
            min_gap_length=self.min_gap_length,
            n_hours_before=self.n_hours_before,
        )

    @property
    def pre_hooks(self):
        pre_hooks = []
        if self.bq_output_gaps is not None:
            pre_hooks.append(
                create_table_hook(
                    table_config=self.table_config,
                    mock=self.mock_bq_clients
                )
            )

            pre_hooks.append(
                delete_events_hook(
                    table_config=self.table_config,
                    start_date=self.start_date,
                    mock=self.mock_bq_clients
                )
            )
        return pre_hooks

    @property
    def post_hooks(self):
        post_hooks = []
        if self.bq_output_gaps is not None:
            post_hooks.append(
                create_view_hook(
                    table_config=self.table_config,
                    mock=self.mock_bq_clients
                )
            )
        return post_hooks

    def validate(self):
        if (
            self.json_input_messages is None
            and (self.bq_input_messages is None or self.bq_input_segments is None)
        ):
            raise ValueError("You need to provide either a JSON inputs or BQ input.")

        if not self.end_date > self.start_date:
            raise ValueError(
                f"end_date ({self.end_date}) must be greater than start_date ({self.start_date})")
