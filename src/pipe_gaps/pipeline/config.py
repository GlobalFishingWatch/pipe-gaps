from dataclasses import dataclass, field
from typing import Any
from datetime import date


@dataclass
class PipelineConfig:
    """Base config for Beam-based pipelines."""
    date_range: tuple
    beam_options: dict[str, Any] = field(default_factory=dict)
    beam_args: list[str] = field(default_factory=list)

    @property
    def parsed_date_range(self) -> tuple[date, date]:
        return tuple(map(date.fromisoformat, self.date_range))


@dataclass
class GapsPipelineConfig(PipelineConfig):
    filter_not_overlapping_and_short: bool = False
    filter_good_seg: bool = False
    open_gaps_start_date: str = "2019-01-01"
    skip_open_gaps: bool = False
    ssvids: list = ()
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

    @property
    def parsed_open_gaps_start_date(self) -> date:
        return date.fromisoformat(self.open_gaps_start_date)
