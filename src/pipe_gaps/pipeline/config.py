from __future__ import annotations
import math
from dataclasses import dataclass, field
from datetime import date, timedelta

from gfw.common.pipeline.config import PipelineConfig


@dataclass
class RawGapsConfig(PipelineConfig):
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
    bq_output_gaps_description: bool = False
    bq_write_disposition: str = "WRITE_APPEND"
    mock_bq_clients: bool = False
    save_json: bool = False
    work_dir: str = "workdir"
    gcp_project: str = None  # TODO: Move this to the base class PipelineConfig.

    name = "pipe-gaps"

    def __post_init__(self) -> None:
        if (
            self.json_input_messages is None
            and (self.bq_input_messages is None or self.bq_input_segments is None)
        ):
            raise ValueError("You need to provide either a JSON inputs or BQ input.")

        # TODO: Move this to the base class PipelineConfig.__post_init__.
        if self.gcp_project is None:
            raise ValueError("You need to specify a GCP project to execute the pipeline.")
        # super().__post_init__()

    @property
    def open_gaps_start(self) -> date:
        return date.fromisoformat(self.open_gaps_start_date)

    @property
    def messages_query_start_date(self) -> date:
        buffer_days = math.ceil(self.n_hours_before / 24)
        return self.start_date - timedelta(days=buffer_days)
