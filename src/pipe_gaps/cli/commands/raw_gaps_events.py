from typing import Any
from types import SimpleNamespace

from gfw.common.cli import Command, Option

from pipe_gaps.cli.validations import date_range
from pipe_gaps.pipelines.raw_gaps_events.main import run

DESCRIPTION = """Creates raw gaps publication events."""

HELP_BQ_INPUT_RAW_GAPS = "BigQuery table with input raw gaps."
HELP_BQ_OUTPUT = "BigQuery table in which to store the raw gap events."

HELP_MOCK_BQ_CLIENTS = "If passed, mocks the BQ clients [Useful for development]."
HELP_DATE_RANGE = "Create raw gaps events for this date range, e.g., «2024-01-01,2024-01-02»."
HELP_BQ_PROJECT = "Project to use when executing the events query."


class RawGapsEvents(Command):
    @property
    def name(cls):
        return "raw_gaps_events"

    @property
    def description(self):
        return DESCRIPTION

    @property
    def options(self):
        return [
            Option("--bq-input-raw-gaps", type=str, help=HELP_BQ_INPUT_RAW_GAPS),
            Option("--bq-input-segment_info", type=str, help=HELP_BQ_INPUT_RAW_GAPS),
            Option("--bq-input-voyages", type=str, help=HELP_BQ_INPUT_RAW_GAPS),
            Option("--bq-input-port_visits", type=str, help=HELP_BQ_INPUT_RAW_GAPS),
            Option("--bq-input-regions_table", type=str, help=HELP_BQ_INPUT_RAW_GAPS),
            Option("--bq-input-all_vessels_byyear", type=str, help=HELP_BQ_INPUT_RAW_GAPS),
            Option("--bq-output", type=str, help=HELP_BQ_OUTPUT),
            Option("--project", type=str, help=HELP_BQ_PROJECT),
            Option("--date-range", type=date_range, help=HELP_DATE_RANGE),
            Option("--mock-bq-clients", type=bool, help=HELP_MOCK_BQ_CLIENTS),
        ]

    @classmethod
    def run(cls, config: SimpleNamespace, **kwargs: Any) -> Any:
        return run(config, **kwargs)
