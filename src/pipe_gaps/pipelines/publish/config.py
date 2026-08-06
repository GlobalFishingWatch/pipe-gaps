from __future__ import annotations
from dataclasses import dataclass, field

# This command does not use beam but PipelineConfig has generic functionality.
# TODO: move PipelineConfig to a more generic package inside gfw-common lib.
from gfw.common.config import PipelineConfig


@dataclass(frozen=True, kw_only=True)
class PublishGapsConfig(PipelineConfig):
    bq_in_gaps: str
    bq_in_segment_info: str
    bq_in_segs_activity: str
    bq_in_voyages: str
    bq_in_port_visits: str
    bq_in_regions: str
    bq_in_vessels_byyear: str
    bq_in_vessels_byyear_field_prefix: str
    bq_output: str
    project: str
    labels: dict = field(default_factory=dict)
    dry_run: bool = False
