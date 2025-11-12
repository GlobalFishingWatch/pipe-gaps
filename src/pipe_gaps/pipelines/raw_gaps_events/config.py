from __future__ import annotations
from dataclasses import dataclass, field

# This is command does not use beam but is generic functionality.
# TODO: move to a more generic package inside gfw-common lib.
from gfw.common.beam.pipeline.config import PipelineConfig


@dataclass
class RawGapsEventsConfig(PipelineConfig):
    bq_input_raw_gaps: str = None
    bq_input_segment_info: str = None
    bq_input_voyages: str = None,
    bq_input_port_visits: str = None
    bq_input_regions: str = None
    bq_input_all_vessels_byyear: str = None
    bq_output: str = None
    bq_write_disposition: str = "WRITE_APPEND"
    labels: dict = field(default_factory=dict)
    project: str = None
    dry_run: bool = False
