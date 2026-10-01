from types import SimpleNamespace
from typing import Any

from gfw.common.cli import Command, Option
from gfw.common.cli.actions import NestedKeyValueAction
from pipe_gaps.cli.validations import date_range
from pipe_gaps.pipelines.events.main import run


DESCRIPTION = """\
Enriches gaps data and creates publication events.
"""

HELP_BQ_IN_RAW_GAPS = "BigQuery table with raw gaps."
HELP_BQ_IN_SEGMENT_INFO = "BigQuery table with segments information."
HELP_BQ_IN_SEGS_ACTIVITY = "BigQuery table with research aggregated segments data."
HELP_BQ_IN_VOYAGES = "BigQuery table with voyages."
HELP_BQ_IN_PORT_VISITS = "BigQuery table with port visits."
HELP_BQ_IN_REGIONS = "BigQuery table with regions."
HELP_BQ_IN_REGISTRY = (
    "BigQuery table with the region name -> description registry (published by pipe-regions' "
    "publish-registry command), used to build the query's region struct/schema dynamically "
    "instead of hardcoding the region list."
)
HELP_BQ_IN_VESSELS_BYYEAR = "BigQuery table with vessels by year."
HELP_BQ_IN_VESSELS_BYYEAR_FIELD_PREFIX = "Field prefix for fields in bq-in-vessels-by-year."
HELP_BQ_IN_VESSELS_BYYEAR_FLAG_FIELD = (
    "Field to read the vessel flag from in bq-in-vessels-by-year. "
    "Defaults to «<field-prefix>mmsi_flag»; VMS pipelines pass «gfw_best_flag»."
)
HELP_LABELS = "Labels to audit costs over the queries."

HELP_BQ_OUT_GAP_EVENTS = "BigQuery table in which to store the gap events."

HELP_CLASSIFY_DISABLING = (
    "If passed, adds intentional_disabling and the fields that drove it to event_info. "
    "Additive only -- never filters out a gap-event row."
)
HELP_BQ_IN_SAT_RECEPTION = (
    "BigQuery table with gridded satellite reception quality (lat_bin, lon_bin, class, "
    "positions_per_day). Required when --classify-disabling is passed."
)
HELP_DISABLING_MIN_GAP_DURATION_H = (
    "Minimum gap duration (hours) to classify a gap as intentional disabling."
)
HELP_DISABLING_MIN_DISTANCE_FROM_SHORE_M = (
    "Minimum distance from shore (meters) at gap start to classify a gap as intentional disabling."
)
HELP_DISABLING_MIN_RECEPTION_POSITIONS_PER_DAY = (
    "Minimum satellite reception quality (positions per day) at gap start to classify a gap "
    "as intentional disabling."
)
HELP_DISABLING_MIN_POSITIONS_BEFORE = (
    "Minimum position count in the hours before the gap (see --n-hours-before in raw-gaps) to "
    "classify a gap as intentional disabling."
)

HELP_MOCK_BQ_CLIENTS = "If passed, mocks the BQ clients [Useful for development]."
HELP_DATE_RANGE = "Create gap events for this date range, e.g., «2024-01-01,2024-01-02»."
HELP_BQ_PROJECT = "Project to use when executing the events query."
HELP_DRY_RUN = "If True, executes queries in dry run mode."
HELP_EXCLUDE_OPEN_GAPS = "If passed, excludes open gaps (no end_timestamp yet) from the output."


class GapEvents(Command):
    @property
    def name(cls):
        return "gap-events"

    @property
    def description(self):
        return DESCRIPTION

    @property
    def options(self):
        return [
            Option("--date-range", type=date_range, help=HELP_DATE_RANGE),
            Option("--project", type=str, help=HELP_BQ_PROJECT),
            Option("--dry-run", type=bool, help=HELP_DRY_RUN),
            Option("--bq-in-raw-gaps", type=str, help=HELP_BQ_IN_RAW_GAPS),
            Option("--bq-in-segment-info", type=str, help=HELP_BQ_IN_SEGMENT_INFO),
            Option("--bq-in-segs-activity", type=str, help=HELP_BQ_IN_SEGS_ACTIVITY),
            Option("--bq-in-voyages", type=str, help=HELP_BQ_IN_VOYAGES),
            Option("--bq-in-port-visits", type=str, help=HELP_BQ_IN_PORT_VISITS),
            Option("--bq-in-regions", type=str, help=HELP_BQ_IN_REGIONS),
            Option("--bq-in-regions-registry", type=str, required=True, help=HELP_BQ_IN_REGISTRY),
            Option("--bq-in-vessels-byyear", type=str, help=HELP_BQ_IN_VESSELS_BYYEAR),
            Option("--bq-in-vessels-byyear-field-prefix", type=str,
                   help=HELP_BQ_IN_VESSELS_BYYEAR_FIELD_PREFIX, default=""),
            Option("--bq-in-vessels-byyear-flag-field", type=str,
                   help=HELP_BQ_IN_VESSELS_BYYEAR_FLAG_FIELD, default=None),
            Option("--bq-out-gap-events", type=str, help=HELP_BQ_OUT_GAP_EVENTS),
            Option("--mock-bq-clients", type=bool, help=HELP_MOCK_BQ_CLIENTS),
            Option("--exclude-open-gaps", type=bool, help=HELP_EXCLUDE_OPEN_GAPS),
            Option("--labels", type=str, nargs="*", action=NestedKeyValueAction, help=HELP_LABELS),
            Option("--classify-disabling", type=bool, help=HELP_CLASSIFY_DISABLING),
            Option("--bq-in-sat-reception", type=str, help=HELP_BQ_IN_SAT_RECEPTION),
            Option(
                "--disabling-min-gap-duration-h",
                type=float,
                default=12,
                help=HELP_DISABLING_MIN_GAP_DURATION_H,
            ),
            Option(
                "--disabling-min-distance-from-shore-m",
                type=float,
                default=92600,
                help=HELP_DISABLING_MIN_DISTANCE_FROM_SHORE_M,
            ),
            Option(
                "--disabling-min-reception-positions-per-day",
                type=float,
                default=10,
                help=HELP_DISABLING_MIN_RECEPTION_POSITIONS_PER_DAY,
            ),
            Option(
                "--disabling-min-positions-before",
                type=float,
                default=14,
                help=HELP_DISABLING_MIN_POSITIONS_BEFORE,
            ),
        ]

    @classmethod
    def run(cls, config: SimpleNamespace, **kwargs: Any) -> Any:
        return run(config, **kwargs)
