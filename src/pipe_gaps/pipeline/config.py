from __future__ import annotations
import math
from typing import Any
from types import SimpleNamespace
from dataclasses import dataclass, field
from datetime import date, timedelta
from functools import cached_property

from jinja2 import Environment

from gfw.common.pipeline.config import PipelineConfig
from gfw.common.jinja2 import EnvironmentLoader


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

    name = "pipe-gaps"

    jinja_folder: str = "assets/queries"  # TODO: move to PipelineConfig base class.

    def __post_init__(self) -> None:
        if (
            self.json_input_messages is None
            and (self.bq_input_messages is None or self.bq_input_segments is None)
        ):
            raise ValueError("You need to provide either a JSON inputs or BQ input.")

    @property
    def open_gaps_start(self) -> date:
        return date.fromisoformat(self.open_gaps_start_date)

    @property
    def messages_query_start_date(self) -> date:
        buffer_days = math.ceil(self.n_hours_before / 24)
        return self.start_date - timedelta(days=buffer_days)

    @cached_property
    # --- TODO: move to PipelineConfig base class in gfw.common ---
    def top_level_package(self):
        module = self.__class__.__module__
        package = module.split(".")[0]

        return package

    @cached_property
    # --- TODO: move to PipelineConfig base class in gfw.common ---
    def jinja_env(self) -> Environment:
        return EnvironmentLoader().from_package(
            package=self.top_level_package,
            path=self.jinja_folder
        )

    @classmethod
    # --- TODO: move to PipelineConfig base class in gfw.common ---
    def from_namespace(cls, ns: SimpleNamespace, **kwargs: Any) -> PipelineConfig:
        """Creates a PipelineConfig instance from a SimpleNamespace.

        Args:
            ns: Namespace containing attributes matching PipelineConfig fields.

        Returns:
            A new PipelineConfig instance.
        """
        ns_dict = vars(ns)
        ns_dict.update(kwargs)

        return cls(**ns_dict)
