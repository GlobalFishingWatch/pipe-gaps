"""This module implements a CLI for the gaps pipeline."""
import sys
import logging


from gfw.common.cli import CLI
from gfw.common.cli.command import Option
from gfw.common.logging import LoggerConfig
from gfw.common.cli.formatting import default_formatter

from pipe_gaps.version import __version__
from pipe_gaps.pipeline.main import run as run_pipeline

from .validations import date_range, ssvids

logger = logging.getLogger(__name__)


NAME = "pipe-gaps"
DESCRIPTION = """
    Detects time gaps in AIS position messages.
    The definition of a gap is configurable by a time threshold 'min-gap-length'.
    For more information, check the documentation at
        https://github.com/GlobalFishingWatch/pipe-gaps/tree/develop.

    You can provide a configuration file or command-line arguments.
    The latter take precedence, so if you provide both, command-line arguments
    will overwrite options in the config file provided.

    Besides the arguments defined here, you can also pass any pipeline option
    defined for Apache Beam PipelineOptions class. For more information, see
        https://cloud.google.com/dataflow/docs/reference/pipeline-options#python.
"""
_DEFAULT = "(default: %(default)s)"

HELP_BQ_READ_METHOD = "BigQuery read method. It may be 'DIRECT_READ' or 'EXPORT'."
HELP_BQ_INPUT_MESSAGES = "BigQuery table with with input messages."
HELP_BQ_INPUT_SEGMENTS = "BigQuery table with with input segments."
HELP_BQ_INPUT_OPEN_GAPS = "BigQuery table with open gaps."
HELP_BQ_OUTPUT_GAPS = "BigQuery table in which to store the gap events."
HELP_BQ_OUTPUT_GAPS_DESCRIPTION = "If passed, creates a description for the output table."
HELP_JSON_INPUT_MESSAGES = "JSON file with input messages [Useful for development]."
HELP_JSON_INPUT_OPEN_GAPS = "JSON file with open gaps [Useful for development]."

HELP_OPEN_GAPS_START_DATE = "Fetch open gaps starting from this date range e.g., '2012-01-01'."
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


def run(args):
    gaps_cli = CLI(
        name=NAME,
        description=DESCRIPTION,
        formatter=default_formatter(max_pos=100),
        run=run_pipeline,
        options=[
            # Options declared here are going to be inherited by subcommands, if any.
            Option("-i", "--json-input-messages", type=str, help=HELP_JSON_INPUT_MESSAGES),
            Option("-s", "--json-input-open-gaps", type=str, help=HELP_JSON_INPUT_OPEN_GAPS),
            Option("--bq-read-method", type=str, help=HELP_BQ_READ_METHOD),
            Option("--bq-input-messages", type=str, help=HELP_BQ_INPUT_MESSAGES),
            Option("--bq-input-segments", type=str, help=HELP_BQ_INPUT_SEGMENTS),
            Option("--bq-input-open-gaps", type=str, help=HELP_BQ_INPUT_OPEN_GAPS),
            Option("--bq-output-gaps", type=str, help=HELP_BQ_OUTPUT_GAPS),
            Option("--bq-output-gaps-description", type=str, help=HELP_BQ_OUTPUT_GAPS_DESCRIPTION),
            Option("--open-gaps-start-date", type=str, help=HELP_OPEN_GAPS_START_DATE),
            Option("--filter-not-overlapping-and-short", type=bool, help=HELP_OVERL),
            Option("--filter-good-seg", type=bool, help=HELP_GOOD_SEG),
            Option("--skip-open-gaps", type=bool, help=HELP_SKIP_OPEN_GAPS),
            Option("--mock-bq-clients", type=bool, help=HELP_MOCK_BQ_CLIENTS),
            Option("--save-json", type=bool, help=HELP_SAVE_JSON),
            Option("--work-dir", type=str, help=HELP_WORK_DIR),
            Option("--ssvids", type=ssvids, help=HELP_SSVIDS),
            Option("--date-range", type=date_range, help=HELP_DATE_RANGE),
            Option("--min-gap-length", type=float, help=HELP_MIN_GAP_LENGTH),
            Option("--window-period-d", type=float, help=HELP_WINDOW_PERIOD_D),
            Option("--eval-last", type=bool, help=HELP_EVAL_LAST),
            Option("--n-hours-before", type=float, help=HELP_N_HOURS_BEFORE),
        ],
        version=__version__,
        examples=[
            "pipe-gaps -c config/sample-from-file.json --min-gap-length 1.3",
        ],
        logger_config=LoggerConfig(
            warning_level=[
                "apache_beam.runners.portability",
                "apache_beam.runners.worker",
                "apache_beam.transforms.core",
                "apache_beam.io.filesystem",
                "apache_beam.io.gcp.bigquery_tools",
                "urllib3"
            ]
        ),
        allow_unknown=True
    )

    return gaps_cli.execute(args)


def main():
    run(sys.argv[1:])


if __name__ == "__main__":
    main()
