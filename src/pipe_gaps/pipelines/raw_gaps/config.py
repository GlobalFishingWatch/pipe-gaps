from __future__ import annotations

import logging
import math

from dataclasses import dataclass, field
from datetime import date, timedelta
from functools import cached_property

from gfw.common.beam.pipeline.hooks import create_table_hook, create_view_hook, delete_events_hook
from gfw.common.config import PipelineConfig
from pipe_gaps.pipelines.raw_gaps.hooks import create_segments_n_days_ahead_hook
from pipe_gaps.pipelines.raw_gaps.table_config import (
    GapsLatestViewConfig,
    GapsTableConfig,
    GapsTableDescription,
    GapsVersionedTableDescription,
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
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
    json_in_messages: str = None
    json_in_open_gaps: str = None
    bq_read_method: str = "EXPORT"
    bq_in_messages: str = None
    bq_in_segments: str = None
    bq_in_open_gaps: str = None
    bq_out_gaps: str = None
    versioned_suffix: str = "versioned"
    bq_write_disposition: str = "WRITE_APPEND"
    mock_bq_clients: bool = False
    save_json: bool = False
    work_dir: str = "workdir"
    good_seg_stabilization_days: int = 0

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
    def bq_out_gaps_versioned(self):
        """Returns the fully qualified ID of the versioned gaps table.

        Derived from :attr:`bq_out_gaps` by appending :attr:`versioned_suffix` -- the table
        itself is never configured directly, since it's an internal implementation detail of
        the public-facing view.

        Raises:
            ValueError: If :attr:`bq_out_gaps` is not set. Callers are expected to only
                access this when BigQuery output is actually configured.
        """
        if self.bq_out_gaps is None:
            raise ValueError("bq_out_gaps_versioned requires bq_out_gaps to be set.")
        return f"{self.bq_out_gaps}_{self.versioned_suffix}"

    @cached_property
    def table_config(self):
        """Returns configuration for the output gaps BigQuery table."""
        return GapsTableConfig(
            table_id=self.bq_out_gaps_versioned,
            description=GapsVersionedTableDescription(
                version=self.version,
                relevant_params=self.bq_out_gaps_description_params
            ),
        )

    @property
    def view_config(self):
        """Returns configuration for the gaps_latest BigQuery view."""
        return GapsLatestViewConfig(
            source=self.table_config,
            view_id=self.bq_out_gaps,
            min_gap_length=self.min_gap_length,
            description=GapsTableDescription(
                version=self.version,
                source_table=self.table_config.table_id.rsplit(".", 1)[-1],
                relevant_params=self.bq_out_gaps_description_params,
            ),
        )

    @property
    def bq_out_gaps_description_params(self):
        """Returns Parameters to be included in the description of the BigQuery output table."""
        # Could be as well just return ALL parameters (and remove irrelevant ones).
        return dict(
            bq_in_messages=self.bq_in_messages,
            bq_in_segments=self.bq_in_segments,
            filter_good_seg=self.filter_good_seg,
            filter_not_overlapping_and_short=self.filter_not_overlapping_and_short,
            min_gap_length=self.min_gap_length,
            n_hours_before=self.n_hours_before,
        )

    @property
    def pre_hooks(self):
        pre_hooks = []
        if self.filter_good_seg and self.good_seg_stabilization_days > 0:
            pre_hooks.append(
                create_segments_n_days_ahead_hook(
                    pipeline_config=self,
                    mock=self.mock_bq_clients
                )
            )

        if self.bq_out_gaps is not None:
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
        if self.bq_out_gaps is not None:
            post_hooks.append(
                create_view_hook(
                    view_config=self.view_config,
                    mock=self.mock_bq_clients
                )
            )
        return post_hooks

    def validate(self):
        if (
            self.json_in_messages is None
            and (self.bq_in_messages is None or self.bq_in_segments is None)
        ):
            raise ValueError("You need to provide either a JSON inputs or BQ input.")

        if not self.end_date > self.start_date:
            raise ValueError(
                f"end_date ({self.end_date}) must be greater than start_date ({self.start_date})")
