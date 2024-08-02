"""This module implements a CLI for the gaps pipeline."""
import sys
import logging
import argparse

from datetime import timedelta, date
from pathlib import Path

from pipe_gaps import utils
from pipe_gaps import pipeline

logger = logging.getLogger(__name__)


NAME = "pipe-gaps"
DESCRIPTION = """
    Detects time gaps in AIS position messages.
    The definition of a gap is configurable by a time threshold.

    If input-file is provided, all Query parameters are ignored.
"""
EPILOG = (
    "Example: pipe-gaps --start-date 2024-01-01 --end-date 2024-01-02' --threshold 0.1"
    " --ssvids 243042594 235053248 413539620"
)

_DEFAULT = "(default: %(default)s)"
# TODO: try to get this descriptions from docstrings, so we don´t duplicate them.
HELP_CONFIG_FILE = "JSON config file to use (optional)."
HELP_INPUT_FILE = f"JSON file with input messages to use {_DEFAULT}."
HELP_THRESHOLD = f"Minimum time difference (hours) to start considering gaps {_DEFAULT}."
HELP_START_DATE = f"Query filter: messages after this dete, e.g., '2024-01-01' {_DEFAULT}."
HELP_END_DATE = f"Query filter: messages before this date, e.g., '2024-01-02' {_DEFAULT}."
HELP_SSVIDS = f"Query filter: list of ssvids {_DEFAULT}."
HELP_SHOW_PROGRESS = f"If True, renders a progress bar {_DEFAULT}."
HELP_MOCK_DB_CLIENT = "If True, mocks the DB client. Useful for development and testing."
HELP_SAVE_JSON = f"If True, saves the results in JSON file {_DEFAULT}."
HELP_PIPE_TYPE = f"Pipeline type: ['naive', 'beam'] {_DEFAULT}."
HELP_WORK_DIR = f"Directory to use as working directory {_DEFAULT}."
HELP_VERBOSE = f"Set logger level to DEBUG {_DEFAULT}."


def formatter():
    """Returns a formatter for argparse help."""

    def formatter(prog):
        return argparse.RawTextHelpFormatter(prog, max_help_position=50)

    return formatter


def _date(s) -> date:
    """Argparse type: string "YYYY-MM-DD" date object."""
    return utils.date_from_string(s)


def threshold(s) -> timedelta:
    """Argparse type: float into timedelta object."""
    return timedelta(hours=float(s))


def cli(args):
    """CLI for gaps pipeline."""
    utils.setup_logger(warning_level=["apache_beam"])

    p = argparse.ArgumentParser(
        prog=NAME, description=DESCRIPTION, epilog=EPILOG, formatter_class=formatter()
    )

    # _threshold = timedelta(hours=12)
    add = p.add_argument
    add("-c", "--config-file", type=Path, metavar=" ", help=HELP_CONFIG_FILE)
    add("-i", "--input-file", type=Path, metavar=" ", help=HELP_INPUT_FILE)
    add("--pipe-type", type=str, metavar=" ", help=HELP_PIPE_TYPE)
    add("--threshold", type=float, metavar=" ", help=HELP_THRESHOLD)
    add("--show-progress", action="store_true", help=HELP_SHOW_PROGRESS)
    add("--start-date", type=str, metavar=" ", help=HELP_START_DATE)
    add("--end-date", type=str, metavar=" ", help=HELP_END_DATE)
    add("--ssvids", type=str, nargs="+", metavar=" ", help=HELP_SSVIDS)
    add("--mock-db-client", action="store_true", help=HELP_MOCK_DB_CLIENT)
    add("--save-json", action="store_true", help=HELP_SAVE_JSON)
    add("--work-dir", type=Path, metavar=" ", help=HELP_WORK_DIR)
    add("-v", "--verbose", action="store_true", help=HELP_VERBOSE)

    ns = p.parse_args(args=args or ["--help"])

    config_file = ns.config_file
    verbose = ns.verbose

    # Delete CLI configuration already used.
    del ns.verbose
    del ns.config_file

    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Load config file if exists.
    config = {}
    if config_file is not None:
        config = utils.json_load(config_file)

    # Validate config_file.
    # for k in config:
    #    if k not in ns:
    #        raise ValueError("Invalid key: '{}' in config file: {}".format(k, config_file))

    # Convert namespace of args to dict.
    args_dict = vars(ns)

    # Override config file with CLI params
    for k, v in args_dict.items():
        if v is not None:
            config[k] = v

    # Group query parameters in single parameter.
    # TODO: should be done here?
    query_params_keys = ["start_date", "end_date", "ssvids"]
    query_params = {}
    for k in query_params_keys:
        param = config.pop(k, None)
        if param is not None:
            query_params[k] = param

    core_params_keys = ["threshold", "show_progress"]
    core_config = {}
    for k in core_params_keys:
        param = config.pop(k, None)
        if param is not None:
            core_config[k] = param

    config["query_params"] = query_params

    # Run pipeline with parsed config.
    print(config)
    try:
        pipeline.create(**config).run()
    except pipeline.PipelineError as e:
        logger.error(e)


def main():
    cli(sys.argv[1:])


if __name__ == "__main__":
    main()
