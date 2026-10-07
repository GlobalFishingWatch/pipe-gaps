from __future__ import annotations

from dataclasses import dataclass

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
    bq_in_regions_registry: str
    bq_in_vessels_byyear: str
    bq_in_vessels_byyear_field_prefix: str
    bq_out_gap_events: str
    project: str
    bq_in_vessels_byyear_flag_field: str | None = None
    dry_run: bool = False
    exclude_open_gaps: bool = False

    # Disabling-event classification (PIPELINE-4615): optional, additive-only -- it never
    # filters out a gap-event row, it only adds intentional_disabling and the fields that
    # drove it inside event_info.
    classify_disabling: bool = False

    # No default: a reception table is required whenever classification is on, so every
    # caller must consciously choose which one (e.g. the prototype's "TEMPORARY" static
    # 2017-2019 snapshot), rather than silently inheriting one.
    bq_in_sat_reception: str | None = None

    disabling_min_gap_duration_h: float = 12
    disabling_min_distance_from_shore_m: float = 92600  # 1852 * 50 nautical miles
    disabling_min_reception_positions_per_day: float = 10
    disabling_min_positions_before: float = 14

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        if self.classify_disabling and self.bq_in_sat_reception is None:
            raise ValueError("bq_in_sat_reception is required when classify_disabling is set.")

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
