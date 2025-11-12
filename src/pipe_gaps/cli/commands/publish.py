from typing import Any
from types import SimpleNamespace

from gfw.common.cli import Command, Option

from pipe_gaps.cli.validations import date_range
from pipe_gaps.pipelines.publish.main import run

DESCRIPTION = """\
Enrich gaps data and create publication events.
"""

HELP_BQ_INPUT_GAPS = "BigQuery table with gaps."
HELP_BQ_INPUT_SEGMENT_INFO = "BigQuery table with segments information."
HELP_BQ_INPUT_VOYAGES = "BigQuery table with voyages."
HELP_BQ_INPUT_PORT_VISITS = "BigQuery table with port visits."
HELP_BQ_INPUT_REGIONS = "BigQuery table with regions."
HELP_BQ_INPUT_VESSELS_BYYEAR = "BigQuery table with vessels by year."

HELP_BQ_OUTPUT = "BigQuery table in which to store the gap events."

HELP_MOCK_BQ_CLIENTS = "If passed, mocks the BQ clients [Useful for development]."
HELP_DATE_RANGE = "Create gap events for this date range, e.g., «2024-01-01,2024-01-02»."
HELP_BQ_PROJECT = "Project to use when executing the events query."
HELP_DRY_RUN = "If True, executes queries in dry run mode."


class PublishGaps(Command):
    @property
    def name(cls):
        return "publish"

    @property
    def description(self):
        return DESCRIPTION

    @property
    def options(self):
        return [
            Option("--date-range", type=date_range, help=HELP_DATE_RANGE),
            Option("--project", type=str, help=HELP_BQ_PROJECT),
            Option("--dry-run", type=bool, help=HELP_DRY_RUN),
            Option("--bq-input-gaps", type=str, help=HELP_BQ_INPUT_GAPS),
            Option("--bq-input-segment-info", type=str, help=HELP_BQ_INPUT_SEGMENT_INFO),
            Option("--bq-input-voyages", type=str, help=HELP_BQ_INPUT_VOYAGES),
            Option("--bq-input-port-visits", type=str, help=HELP_BQ_INPUT_PORT_VISITS),
            Option("--bq-input-regions", type=str, help=HELP_BQ_INPUT_REGIONS),
            Option("--bq-input-vessels-byyear", type=str, help=HELP_BQ_INPUT_VESSELS_BYYEAR),
            Option("--bq-output", type=str, help=HELP_BQ_OUTPUT),
            Option("--mock-bq-clients", type=bool, help=HELP_MOCK_BQ_CLIENTS),
        ]

    @classmethod
    def run(cls, config: SimpleNamespace, **kwargs: Any) -> Any:
        return run(config, **kwargs)
