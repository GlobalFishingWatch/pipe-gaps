from __future__ import annotations
from dataclasses import dataclass, field

# This command does not use beam but PipelineConfig has generic functionality.
# TODO: move PipelineConfig to a more generic package inside gfw-common lib.
from gfw.common.config import PipelineConfig


@dataclass(frozen=True, kw_only=True)
class GapEventsConfig(PipelineConfig):
    bq_in_raw_gaps: str
    bq_in_segment_info: str
    bq_in_segs_activity: str
    bq_in_voyages: str
    bq_in_port_visits: str
    bq_in_regions: str
    bq_in_vessels_byyear: str
    bq_in_vessels_byyear_field_prefix: str
    bq_out_gap_events: str
    project: str
    bq_in_vessels_byyear_flag_field: str | None = None
    labels: dict = field(default_factory=dict)
    dry_run: bool = False

    @property
    def vessels_byyear_flag_field(self) -> str:
        """Field to read the vessel flag from in ``bq_in_vessels_byyear``.

        Defaults to ``<prefix>mmsi_flag``, the field the query read before this
        became configurable, so a caller that omits it keeps its behaviour
        whichever prefix it passes. VMS passes ``gfw_best_flag``, which is
        COALESCE(reported flag, registry flag, source tenant) on its PVIS.
        See PIPELINE-4424.
        """
        return self.bq_in_vessels_byyear_flag_field or (
            f"{self.bq_in_vessels_byyear_field_prefix}mmsi_flag"
        )
