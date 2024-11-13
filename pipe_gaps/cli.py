"""This module implements a CLI for the gaps pipeline."""
import sys
import json
import math
import logging
import argparse
from argparse import BooleanOptionalAction

from pathlib import Path
from datetime import date, timedelta

from pipe_gaps import utils
from pipe_gaps import pipeline

logger = logging.getLogger(__name__)


NAME = "pipe-gaps"
DESCRIPTION = """
    Detects time gaps in AIS position messages.
    The definition of a gap is configurable by a time threshold 'max_gap_length'.
    For more information, check the documentation at
        https://github.com/GlobalFishingWatch/pipe-gaps/tree/develop.

    You can provide a configuration file or command-line arguments.
    The latter take precedence, so if you provide both, command-line arguments
    will overwrite options in the config file provided.

    Besides the arguments defined here, you can also pass any valid argument
    of Apache Beam PipelineOptions class. For more information, see
        https://cloud.google.com/dataflow/docs/reference/pipeline-options#python.
"""
EPILOG = (
    "Example: \n"
    "    pipe-gaps -c config/sample-from-file-1.json --max_gap_length 1.3"
)

_DEFAULT = "(default: %(default)s)"
# TODO: try to get this descriptions from docstrings, so we don´t duplicate them.
HELP_CONFIG_FILE = f"JSON file with pipeline configuration {_DEFAULT}."
HELP_VERBOSE = "Set logger level to DEBUG."

HELP_PIPE_TYPE = "Pipeline type: ['naive', 'beam']."
HELP_BQ_INPUT_MESSAGES = "BigQuery table with with input messages."
HELP_BQ_INPUT_OPEN_GAPS = "BigQuery table with open gaps."
HELP_BQ_OUTPUT_GAPS = "BigQuery table in which to store the gap events."
HELP_JSON_INPUT_MESSAGES = "JSON file with input messages [Useful for development]."
HELP_JSON_INPUT_OPEN_GAPS = "JSON file with open gaps [Useful for development]."
HELP_DATE_RANGE = "Detect gaps within this date range e.g., '2024-01-01 2024-01-02'."
HELP_OPEN_GAPS_START_DATE = "Fetch open gaps starting from this date range e.g., '2012-01-01'."
HELP_SKIP_OPEN_GAPS = "Whether or not to fetch open gaps [Useful for development]. "
HELP_SSVIDS = "Detect gaps for this list of ssvids."
HELP_FILTER_OVERLAP = "Filter messages from 'overlapping and short' (OLAS) segments."
HELP_MOCK_DB_CLIENT = "If passed, mocks the DB client [Useful for development]."
HELP_SAVE_JSON = "If passed, saves the results in JSON file [Useful for development]."
HELP_WORK_DIR = "Directory to use as working directory."

HELP_MIN_GAP_LENGTH = "Minimum time difference (hours) to start considering gaps."
HELP_WINDOW_PERIOD_D = "Period (in days) of time windows used to parallelize the process."
HELP_EVAL_LAST = "If passed, evaluates last message of each SSVID to create an open gap."


def formatter():
    """Returns a formatter for argparse help."""

    def formatter(prog):
        return argparse.RawTextHelpFormatter(prog, max_help_position=50)

    return formatter


def run(config: dict) -> None:

    logger.info("Using following GAPS pipeline configuration: ")
    logger.info(json.dumps(config, indent=4))

    pipe = build_pipeline(**config)
    try:
        pipe.run()
    except pipeline.PipelineError as e:
        logger.error(e)


def build_pipeline(
    pipe_type: str = "beam",
    date_range: tuple = ("2024-01-01", "2024-01-02"),
    filter_overlap: bool = False,
    open_gaps_start_date: str = "2019-01-01",
    skip_open_gaps: bool = False,
    ssvids: list = (),
    min_gap_length: float = 6,
    n_hours_before: int = 12,
    window_period_d: int = 30,
    eval_last: bool = True,
    normalize_output: bool = True,
    json_input_messages: str = None,
    bq_write_disposition: str = "WRITE_APPEND",
    bq_input_messages: str = None,
    bq_input_segments: str = "pipe_ais_v3_published.segs_activity",
    bq_input_open_gaps: str = None,
    bq_output_gaps: str = None,
    mock_db_client: bool = False,
    beam_options: dict = None,
    save_json: bool = False,
    work_dir: str = "workdir"
):
    """This function creates a configuration that complies with pipeline factory interface."""

    start_date, end_date = date_range

    start_date = date.fromisoformat(start_date)
    end_date = date.fromisoformat(end_date)

    if json_input_messages is None and (bq_input_messages is None or bq_input_segments is None):
        raise ValueError("You need to provide either a JSON inputs or BQ input.")

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
                "filter_overlapping_and_short": filter_overlap
            },
            "mock_db_client": mock_db_client,
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
            "mock_db_client": mock_db_client
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
            "groups_key": "ssvid",
            "boundaries_key": "ssvid",
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

    if bq_input_open_gaps is not None and not skip_open_gaps:
        side_inputs.append(create_bq_side_input())

    if bq_output_gaps is not None:
        outputs.append(create_bq_output_config())

    if save_json:
        outputs.append(create_json_output_config())

    if beam_options is not None:
        options.update(beam_options)

    config = {
        "pipe_type": pipe_type,
        "pipe_config": {
            "inputs": inputs,
            "side_inputs": side_inputs,
            "core": create_core_config(),
            "outputs": outputs,
            "options": options
        }
    }

    return pipeline.factory.from_config(config)


def cli(args):
    """CLI for gaps pipeline."""
    utils.setup_logger(
        warning_level=[
            "apache_beam.runners.portability",
            "apache_beam.runners.worker",
            "apache_beam.transforms.core",
            "apache_beam.io.filesystem",
            "apache_beam.io.gcp.bigquery_tools",
            "urllib3"
        ]
    )

    p = argparse.ArgumentParser(
        prog=NAME,
        description=DESCRIPTION,
        epilog=EPILOG,
        formatter_class=formatter(),
        # argument_default=argparse.SUPPRESS
    )

    add = p.add_argument
    add("-c", "--config-file", type=Path, default=None, metavar=" ", help=HELP_CONFIG_FILE)
    add("-v", "--verbose", action="store_true", default=False, help=HELP_VERBOSE)

    add = p.add_argument_group("general pipeline configuration").add_argument
    add("--pipe-type", type=str, metavar=" ", help=HELP_PIPE_TYPE)
    add("-bi", "--bq-input-messages", type=str, metavar=" ", help=HELP_BQ_INPUT_MESSAGES)
    add("-bs", "--bq-input-open-gaps", type=str, metavar=" ", help=HELP_BQ_INPUT_OPEN_GAPS)
    add("-bo", "--bq-output-gaps", type=str, metavar=" ", help=HELP_BQ_OUTPUT_GAPS)
    add("-i", "--json-input-messages", type=str, metavar=" ", help=HELP_JSON_INPUT_MESSAGES)
    add("-s", "--json-input-open-gaps", type=str, metavar=" ", help=HELP_JSON_INPUT_OPEN_GAPS)
    add("--date-range", type=str, nargs=2, metavar=" ", help=HELP_DATE_RANGE)
    add("--filter-overlap", type=bool, default=None, metavar=" ", help=HELP_FILTER_OVERLAP)
    add("--open-gaps-start-date", type=str, metavar=" ", help=HELP_OPEN_GAPS_START_DATE)
    add("--skip-open-gaps", action="store_true", default=None, help=HELP_SKIP_OPEN_GAPS)
    add("--ssvids", type=str, nargs="+", metavar=" ", help=HELP_SSVIDS)
    add("--mock-db-client", default=None, action=BooleanOptionalAction, help=HELP_MOCK_DB_CLIENT)
    add("--work-dir", type=str, metavar=" ", help=HELP_WORK_DIR)
    add("--save-json", default=None, action=BooleanOptionalAction, help=HELP_SAVE_JSON)

    boolean = BooleanOptionalAction
    add = p.add_argument_group("gap detection process").add_argument
    add("--min-gap-length", type=float, metavar=" ", help=HELP_MIN_GAP_LENGTH)
    add("--window-period_d", type=float, metavar=" ", help=HELP_WINDOW_PERIOD_D)
    add("--eval-last", default=None, action=boolean, help=HELP_EVAL_LAST)

    ns, unknown = p.parse_known_args(args=args or ["--help"])

    config_file = ns.config_file
    verbose = ns.verbose

    # Delete CLI configuration from parsed namespace.
    del ns.verbose
    del ns.config_file

    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Convert namespace of args to dict.
    cli_args = vars(ns)

    # Erase null arguments.
    for arg in list(cli_args):
        if cli_args[arg] is None:
            del cli_args[arg]

    # Parse unknown arguments to dict.
    extra_args = dict(zip(unknown[:-1:2], unknown[1::2]))
    extra_args = {k.replace("--", ""): v for k, v in extra_args.items()}

    config = {}
    # Load config file if exists.
    if config_file is not None:
        config = utils.json_load(config_file)

    config.update({**cli_args, **extra_args})

    run(config)


def main():
    cli(sys.argv[1:])


if __name__ == "__main__":
    main()
