from typing import Any
from types import SimpleNamespace

from gfw.common.cli import Command, Option
from gfw.common.cli.actions import NestedKeyValueAction

from pipe_gaps.cli.validations import date_range
from pipe_gaps.pipelines.events.main import run

DESCRIPTION = """\
Enriches gaps data and creates publication events.
"""

HELP_BQ_IN_GAPS = "BigQuery table with gaps."
HELP_BQ_IN_SEGMENT_INFO = "BigQuery table with segments information."
HELP_BQ_IN_SEGS_ACTIVITY = "BigQuery table with research aggregated segments data."
HELP_BQ_IN_VOYAGES = "BigQuery table with voyages."
HELP_BQ_IN_PORT_VISITS = "BigQuery table with port visits."
HELP_BQ_IN_REGIONS = "BigQuery table with regions."
HELP_BQ_IN_VESSELS_BYYEAR = "BigQuery table with vessels by year."
HELP_BQ_IN_VESSELS_BYYEAR_FIELD_PREFIX = "Field prefix for fields in bq-in-vessels-by-year."
HELP_LABELS = "Labels to audit costs over the queries."

HELP_BQ_OUTPUT = "BigQuery table in which to store the gap events."

HELP_MOCK_BQ_CLIENTS = "If passed, mocks the BQ clients [Useful for development]."
HELP_DATE_RANGE = "Create gap events for this date range, e.g., «2024-01-01,2024-01-02»."
HELP_BQ_PROJECT = "Project to use when executing the events query."
HELP_DRY_RUN = "If True, executes queries in dry run mode."


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
            Option("--bq-in-gaps", type=str, help=HELP_BQ_IN_GAPS),
            Option("--bq-in-segment-info", type=str, help=HELP_BQ_IN_SEGMENT_INFO),
            Option("--bq-in-segs-activity", type=str, help=HELP_BQ_IN_SEGS_ACTIVITY),
            Option("--bq-in-voyages", type=str, help=HELP_BQ_IN_VOYAGES),
            Option("--bq-in-port-visits", type=str, help=HELP_BQ_IN_PORT_VISITS),
            Option("--bq-in-regions", type=str, help=HELP_BQ_IN_REGIONS),
            Option("--bq-in-vessels-byyear", type=str, help=HELP_BQ_IN_VESSELS_BYYEAR),
            Option("--bq-in-vessels-byyear-field-prefix", type=str,
                   help=HELP_BQ_IN_VESSELS_BYYEAR_FIELD_PREFIX, default=""),
            Option("--bq-output", type=str, help=HELP_BQ_OUTPUT),
            Option("--mock-bq-clients", type=bool, help=HELP_MOCK_BQ_CLIENTS),
            Option("--labels", type=str, nargs="*", action=NestedKeyValueAction, help=HELP_LABELS),
        ]

    @classmethod
    def run(cls, config: SimpleNamespace, **kwargs: Any) -> Any:
        return run(config, **kwargs)
