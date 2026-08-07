from typing import Any
from types import SimpleNamespace

from gfw.common.cli import Command, Option

from pipe_gaps.pipelines.raw_gaps.main import run
from pipe_gaps.cli.validations import date_range, ssvids

DESCRIPTION = """\
Detects time gaps in position messages.

The definition of a gap is configurable by a time threshold 'min-gap-length'.
For more information, check the documentation at
    https://github.com/GlobalFishingWatch/pipe-gaps/.

You can provide a configuration file or command-line arguments.
The latter take precedence, so if you provide both, command-line arguments
will overwrite options in the config file provided.

Besides the arguments defined here, you can also pass any pipeline option
defined for Apache Beam PipelineOptions class. For more information, see
    https://cloud.google.com/dataflow/docs/reference/pipeline-options#python.\n
"""

HELP_BQ_READ_METHOD = "BigQuery read method. It may be 'DIRECT_READ' or 'EXPORT'."
HELP_BQ_IN_MESSAGES = "BigQuery table with with input messages."
HELP_BQ_IN_SEGMENTS = "BigQuery table with with input segments."
HELP_BQ_IN_OPEN_GAPS = "BigQuery table with open gaps."
HELP_BQ_OUT_GAPS = "BigQuery table in which to store the gap events."
HELP_JSON_IN_MESSAGES = "JSON file with input messages [Useful for development]."
HELP_JSON_IN_OPEN_GAPS = "JSON file with open gaps [Useful for development]."

HELP_OPEN_GAPS_START = "Fetch open gaps starting from this date range e.g., '2012-01-01'."
HELP_SKIP_OPEN_GAPS = "If passed, pipeline will not fetch open gaps [Useful for development]. "
HELP_OVERL = "Fetch messages that do not belong to 'overlapping_and_short' segments."
HELP_GOOD_SEG = "Fetch messages that belong to 'good_seg2' segments."
HELP_MOCK_BQ_CLIENTS = "If passed, mocks the BQ clients [Useful for development]."
HELP_SAVE_JSON = "If passed, saves the results in JSON file [Useful for development]."
HELP_WORK_DIR = "Directory to use as working directory."
HELP_SSVIDS = "Detect gaps for this list of ssvids, e.g., «412331104,477334300»."
HELP_DATE_RANGE = "Detect gaps within this date range, e.g., «2024-01-01,2024-01-02»."

HELP_MIN_GAP_LENGTH = "Minimum time difference (hours) to start considering gaps."
HELP_WINDOW_PERIOD_D = "Period (in days) of time windows used to parallelize the process."
HELP_EVAL_LAST = "If passed, evaluates last message of each SSVID to create an open gap."
HELP_N_HOURS_BEFORE = "Count messages this amount of hours before each gap."
HELP_STABILIZATION = (
    "Number of days the segments table needs to be ahead in order to filter messages with a "
    "stable good_seg metric. If good_seg filter is ON and this validation fails, "
    "the process will throw an error."
)


class RawGaps(Command):
    @property
    def name(cls):
        return "raw-gaps"

    @property
    def description(self):
        return DESCRIPTION

    @property
    def options(self):
        return [
            Option("-i", "--json-in-messages", type=str, help=HELP_JSON_IN_MESSAGES),
            Option("-s", "--json-in-open-gaps", type=str, help=HELP_JSON_IN_OPEN_GAPS),
            Option("--bq-read-method", type=str, default="EXPORT", help=HELP_BQ_READ_METHOD),
            Option("--bq-in-messages", type=str, help=HELP_BQ_IN_MESSAGES),
            Option("--bq-in-segments", type=str, help=HELP_BQ_IN_SEGMENTS),
            Option("--bq-in-open-gaps", type=str, help=HELP_BQ_IN_OPEN_GAPS),
            Option("--bq-out-gaps", type=str, help=HELP_BQ_OUT_GAPS),
            Option("--open-gaps-start-date", type=str, required=True, help=HELP_OPEN_GAPS_START),
            Option("--filter-not-overlapping-and-short", type=bool, help=HELP_OVERL),
            Option("--filter-good-seg", type=bool, help=HELP_GOOD_SEG),
            Option("--skip-open-gaps", type=bool, help=HELP_SKIP_OPEN_GAPS),
            Option("--mock-bq-clients", type=bool, help=HELP_MOCK_BQ_CLIENTS),
            Option("--save-json", type=bool, help=HELP_SAVE_JSON),
            Option("--work-dir", type=str, default="workdir", help=HELP_WORK_DIR),
            Option("--ssvids", type=ssvids, default=tuple(), help=HELP_SSVIDS),
            Option("--date-range", type=date_range, help=HELP_DATE_RANGE),
            Option("--min-gap-length", type=float, required=True, help=HELP_MIN_GAP_LENGTH),
            Option("--window-period-d", type=float, help=HELP_WINDOW_PERIOD_D),
            Option("--eval-last", type=bool, help=HELP_EVAL_LAST),
            Option("--n-hours-before", default=12, type=float, help=HELP_N_HOURS_BEFORE),
            Option("--good-seg-stabilization-days", default=0, type=int, help=HELP_STABILIZATION)
        ]

    @classmethod
    def run(cls, config: SimpleNamespace, **kwargs: Any) -> Any:
        return run(config, **kwargs)
