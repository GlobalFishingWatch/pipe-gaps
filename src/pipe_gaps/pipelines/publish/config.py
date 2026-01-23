from __future__ import annotations
from dataclasses import dataclass, field

# This command does not use beam but PipelineConfig has generic functionality.
# TODO: move PipelineConfig to a more generic package inside gfw-common lib.
from gfw.common.beam.pipeline.config import PipelineConfig


@dataclass(frozen=True, kw_only=True)
class PublishGapsConfig(PipelineConfig):
    bq_input_gaps: str
    bq_input_segment_info: str
    bq_input_segs_activity: str
    bq_input_voyages: str
    bq_input_port_visits: str
    bq_input_regions: str
    bq_input_vessels_byyear: str
    bq_output: str
    project: str
    labels: dict = field(default_factory=dict)
    dry_run: bool = False
