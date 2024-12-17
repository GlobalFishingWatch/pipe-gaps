"""This module implements a CLI for the gaps pipeline."""
import sys
import json
import math
import logging
import argparse

from pathlib import Path
from datetime import date, timedelta

from pipe_gaps import utils
from pipe_gaps import pipeline

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
EPILOG = (
    "Example: \n"
    "    pipe-gaps -c config/sample-from-file.json --min-gap-length 1.3"
)

LOGGER_LEVEL_WARNING = [
    "apache_beam.runners.portability",
    "apache_beam.runners.worker",
    "apache_beam.transforms.core",
    "apache_beam.io.filesystem",
    "apache_beam.io.gcp.bigquery_tools",
    "urllib3"
]

_DEFAULT = "(default: %(default)s)"
# TODO: try to get this descriptions from docstrings, so we don´t duplicate them.
# TODO: put descriptions in docstring of build_pipeline function.
HELP_CONFIG_FILE = f"JSON file with pipeline configuration {_DEFAULT}."
HELP_VERBOSE = "Set logger level to DEBUG."
HELP_NO_RICH_LOGGING = "Disable rich logging (useful prof production environments)."
HELP_ONLY_RENDER = "Only render command-line call equivalent to provided config file."

HELP_PIPE_TYPE = "Pipeline type: ['naive', 'beam']."
HELP_BQ_READ_METHOD = "BigQuery read method. It may be 'DIRECT_RED' or 'EXPORT'."
HELP_BQ_INPUT_MESSAGES = "BigQuery table with with input messages."
HELP_BQ_INPUT_SEGMENTS = "BigQuery table with with input segments."
HELP_BQ_INPUT_OPEN_GAPS = "BigQuery table with open gaps."
HELP_BQ_OUTPUT_GAPS = "BigQuery table in which to store the gap events."
HELP_JSON_INPUT_MESSAGES = "JSON file with input messages [Useful for development]."
HELP_JSON_INPUT_OPEN_GAPS = "JSON file with open gaps [Useful for development]."

HELP_OPEN_GAPS_START_DATE = "Fetch open gaps starting from this date range e.g., '2012-01-01'."
HELP_SKIP_OPEN_GAPS = "If passed, pipeline will not fetch open gaps [Useful for development]. "
HELP_OVERL = "Fetch messages that do not belong to 'overlapping_and_short' segments."
HELP_GOOD_SEG = "Fetch messages that belong to 'good_seg2' segments."
HELP_MOCK_DB_CLIENT = "If passed, mocks the DB client [Useful for development]."
HELP_SAVE_JSON = "If passed, saves the results in JSON file [Useful for development]."
HELP_WORK_DIR = "Directory to use as working directory."
HELP_SSVIDS = "Detect gaps for this list of ssvids, e.g., «412331104 477334300»."
HELP_DATE_RANGE = "Detect gaps within this date range, e.g., «2024-01-01,2024-01-02»."

HELP_MIN_GAP_LENGTH = "Minimum time difference (hours) to start considering gaps."
HELP_WINDOW_PERIOD_D = "Period (in days) of time windows used to parallelize the process."
HELP_EVAL_LAST = "If passed, evaluates last message of each SSVID to create an open gap."
HELP_N_HOURS_BEFORE = "Count messages this amount of hours before each gap."


ERROR_DATE_RANGE = "Must be a string with two dates in ISO format separated by a space."


def formatter():
    """Returns a formatter for argparse help."""

    def formatter(prog):
        return argparse.RawTextHelpFormatter(prog, max_help_position=50)

    return formatter


def render_command_line_call(config: dict, unparsed: list) -> str:
    """Renders command-line call from config dictionary.

    Args:
        config: dictionary with config.
        unparsed: list of unparsed CLI args.

    Returns:
        Multiline string with CLI full command.
    """

    command = f"{NAME} \\\n"
    argument = "--{name}{sep}{value} {end}"

    for flag in unparsed:
        command += f"{flag} \\\n"

    items = {k: v for k, v in config.items() if k != "pipeline_options"}.items()
    for i, (k, v) in enumerate(items):
        name = k.replace("_", "-")
        value = v
        sep = "="
        end = "\\\n"

        if isinstance(v, (list, tuple)):
            if len(v) == 0:
                continue

            value = ",".join(v)

        if isinstance(v, bool):
            value = ""
            sep = ""

        if i == len(items) - 1:
            end = ""

        command += argument.format(name=name, value=value, sep=sep, end=end)

    return command


def run(config: dict) -> None:
    """Builds and runs pipeline."""
    logger.info("Using following GAPS pipeline configuration: ")
    logger.info(json.dumps(config, indent=4))

    pipe = build_pipeline(**config)
    try:
        pipe.run()
    except pipeline.PipelineError as e:
        logger.error(e)


def build_pipeline(
    date_range: tuple,
    pipe_type: str = "beam",
    filter_not_overlapping_and_short: bool = False,
    filter_good_seg: bool = False,
    open_gaps_start_date: str = "2019-01-01",
    skip_open_gaps: bool = False,
    ssvids: list = (),
    min_gap_length: float = 6,
    n_hours_before: int = 12,
    window_period_d: int = None,
    eval_last: bool = True,
    normalize_output: bool = True,
    json_input_messages: str = None,
    bq_read_method: str = "EXPORT",
    bq_input_messages: str = None,
    bq_input_segments: str = "pipe_ais_v3_published.segs_activity",
    bq_input_open_gaps: str = None,
    bq_output_gaps: str = None,
    bq_write_disposition: str = "WRITE_APPEND",
    mock_db_client: bool = False,
    save_json: bool = False,
    work_dir: str = "workdir",
    pipeline_options: dict = None,
):
    """This function creates a configuration that complies with pipeline factory interface."""

    start_date, end_date = [date.fromisoformat(x) for x in date_range]
    open_gaps_start_date = date.fromisoformat(open_gaps_start_date)

    if json_input_messages is None and (bq_input_messages is None or bq_input_segments is None):
        raise ValueError("You need to provide either a JSON inputs or BQ input.")

    if bq_input_open_gaps is None:
        bq_input_open_gaps = bq_output_gaps

    def create_bigquery_input_config():
        buffer_days = math.ceil(n_hours_before / 24)
        query_start_date = start_date - timedelta(days=buffer_days)

        return {
            "kind": "query",
            "query_name": "messages",
            "query_params": {
                "source_messages": bq_input_messages,
                "source_segments": bq_input_segments,
                "start_date": query_start_date,
                "end_date": end_date,
                "ssvids": ssvids,
                "filter_not_overlapping_and_short": filter_not_overlapping_and_short,
                "filter_good_seg": filter_good_seg,

            },
            "mock_db_client": mock_db_client,
            "method": bq_read_method
        }

    def create_bq_side_input():
        return {
            "kind": "query",
            "query_name": "gaps",
            "query_params": {
                "source_gaps": bq_input_open_gaps,
                "start_date": open_gaps_start_date,
                "end_date": start_date
            },
            "mock_db_client": mock_db_client,
            "method": bq_read_method
        }

    def create_json_input_config():
        return {
            "kind": "json",
            "input_file": json_input_messages,
            "lines": True
        }

    def create_bq_output_config():
        return {
            "kind": "bigquery",
            "table": bq_output_gaps,
            "schema": "gaps",
            "write_disposition": bq_write_disposition
        }

    def create_json_output_config():
        return {
            "kind": "json",
            "output_prefix": "gaps",
            "output_dir": work_dir
        }

    def create_core_config():
        return {
            "kind": "detect_gaps",
            "eval_last": eval_last,
            "threshold": min_gap_length,
            "normalize_output": normalize_output,
            "date_range": date_range,
            "window_period_d": window_period_d,
            "window_offset_h": n_hours_before,
        }

    inputs = []
    outputs = []
    side_inputs = []
    options = {}

    if json_input_messages is not None:
        inputs.append(create_json_input_config())

    if bq_input_messages is not None:
        inputs.append(create_bigquery_input_config())

    if bq_input_open_gaps is not None and not skip_open_gaps and start_date > open_gaps_start_date:
        side_inputs.append(create_bq_side_input())

    if bq_output_gaps is not None:
        outputs.append(create_bq_output_config())

    if save_json:
        outputs.append(create_json_output_config())

    if pipeline_options is not None:
        options.update(pipeline_options)

    config = {
        "pipe_type": pipe_type,
        "pipe_config": {
            "inputs": inputs,
            "side_inputs": side_inputs,
            "core": create_core_config(),
            "outputs": outputs,
            "options": options,
            "work_dir": work_dir
        }
    }

    return pipeline.factory.from_config(config)


def validate_date(date_str):
    try:
        date.fromisoformat(date_str)
    except Exception as e:
        raise argparse.ArgumentTypeError(f"{ERROR_DATE_RANGE} \n {e}")


def date_range(date_str):
    date_range = date_str.split(",")
    if len(date_range) != 2:
        raise argparse.ArgumentTypeError(ERROR_DATE_RANGE)

    for d in date_range:
        validate_date(d)

    return date_range


def ssvids(ssvids_str):
    return ssvids_str.split(",")


def cli(args):
    """CLI for gaps pipeline."""

    p = argparse.ArgumentParser(
        prog=NAME,
        description=DESCRIPTION,
        epilog=EPILOG,
        formatter_class=formatter(),
    )

    add = p.add_argument
    add("-c", "--config-file", type=Path, default=None, metavar=" ", help=HELP_CONFIG_FILE)
    add("-v", "--verbose", action="store_true", default=False, help=HELP_VERBOSE)
    add("--no-rich-logging", action="store_true", default=False, help=HELP_NO_RICH_LOGGING)
    add("--only-render", action="store_true", help=HELP_ONLY_RENDER)

    add = p.add_argument_group("general pipeline configuration").add_argument
    add("--pipe-type", type=str, metavar=" ", help=HELP_PIPE_TYPE)
    add("-i", "--json-input-messages", type=str, metavar=" ", help=HELP_JSON_INPUT_MESSAGES)
    add("-s", "--json-input-open-gaps", type=str, metavar=" ", help=HELP_JSON_INPUT_OPEN_GAPS)
    add("--bq-read-metohd", type=str, metavar=" ", help=HELP_BQ_READ_METHOD)
    add("--bq-input-messages", type=str, metavar=" ", help=HELP_BQ_INPUT_MESSAGES)
    add("--bq-input-segments", type=str, metavar=" ", help=HELP_BQ_INPUT_SEGMENTS)
    add("--bq-input-open-gaps", type=str, metavar=" ", help=HELP_BQ_INPUT_OPEN_GAPS)
    add("--bq-output-gaps", type=str, metavar=" ", help=HELP_BQ_OUTPUT_GAPS)
    add("--open-gaps-start-date", type=str, metavar=" ", help=HELP_OPEN_GAPS_START_DATE)
    add("--filter-not-overlapping-and-short", action="store_true", default=None, help=HELP_OVERL)
    add("--filter-good-seg", action="store_true", default=None, help=HELP_GOOD_SEG)
    add("--skip-open-gaps", action="store_true", default=None, help=HELP_SKIP_OPEN_GAPS)
    add("--mock-db-client", action="store_true",  default=None, help=HELP_MOCK_DB_CLIENT)
    add("--save-json", action="store_true", default=None, help=HELP_SAVE_JSON)
    add("--work-dir", type=str, metavar=" ", help=HELP_WORK_DIR)
    add("--ssvids", type=ssvids, metavar=" ", help=HELP_SSVIDS)
    add("--date-range", type=date_range, metavar=" ", help=HELP_DATE_RANGE)

    add = p.add_argument_group("gap detection process").add_argument
    add("--min-gap-length", type=float, metavar=" ", help=HELP_MIN_GAP_LENGTH)
    add("--window-period-d", type=float, metavar=" ", help=HELP_WINDOW_PERIOD_D)
    add("--eval-last", action="store_true", default=None, help=HELP_EVAL_LAST)
    add("--n-hours-before", type=float, metavar=" ", help=HELP_N_HOURS_BEFORE)

    ns, unknown = p.parse_known_args(args=args or ["--help"])

    config_file = ns.config_file
    verbose = ns.verbose
    no_rich_logging = ns.no_rich_logging
    only_render = ns.only_render

    # Delete CLI configuration from parsed namespace.
    del ns.verbose
    del ns.config_file
    del ns.only_render
    del ns.no_rich_logging

    utils.setup_logger(warning_level=LOGGER_LEVEL_WARNING, rich=not no_rich_logging)

    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Convert namespace of args to dict.
    cli_args = vars(ns)

    # Erase null arguments.
    for arg in list(cli_args):
        if cli_args[arg] is None:
            del cli_args[arg]

    config = {}
    # Load config file if exists.
    if config_file is not None:
        config = utils.json_load(config_file)

    # Override configuration file with CLI args.
    config.update(cli_args)

    if only_render:
        # Only render equivalent command-line args call and exit.
        logger.info("Equivalent command-line call: ")
        print(render_command_line_call(config, unknown))
        return config

    logger.info(
        "Following unknown args will be parsed internally by the pipeline: {}".format(unknown))

    config.setdefault("pipeline_options", {})["unparsed"] = unknown
    run(config)

    return config


def main():
    cli(sys.argv[1:])


if __name__ == "__main__":
    main()
