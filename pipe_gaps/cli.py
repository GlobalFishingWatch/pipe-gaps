"""This module implements a CLI for the gaps pipeline."""
import sys
import logging
import argparse

from datetime import timedelta, date
from pathlib import Path

from pipe_gaps import utils
from pipe_gaps import pipe
from pipe_gaps import constants as ct

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
HELP_INPUT_FILE = f"JSON file with input messages to use {_DEFAULT}."
HELP_THRESHOLD = f"Minimum time difference (hours) to start considering gaps {_DEFAULT}."
HELP_START_DATE = f"Query filter: messages after this dete, e.g., '2024-01-01' {_DEFAULT}."
HELP_END_DATE = f"Query filter: messages before this date, e.g., '2024-01-02' {_DEFAULT}."
HELP_SSVIDS = f"Query filter: list of ssvids {_DEFAULT}."
HELP_SHOW_PROGRESS = f"If True, renders a progress bar {_DEFAULT}."
HELP_MOCK_DB_CLIENT = "If True, mocks the DB client. Useful for development and testing."
HELP_SAVE = f"If True, saves the results in JSON file {_DEFAULT}."
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
    utils.setup_logger()

    p = argparse.ArgumentParser(
        prog=NAME, description=DESCRIPTION, epilog=EPILOG, formatter_class=formatter()
    )

    p.set_defaults(func=pipe.run)

    _threshold = timedelta(hours=12)
    add = p.add_argument
    add("-i", "--input-file", type=Path, metavar=" ", help=HELP_INPUT_FILE)
    add("--threshold", type=threshold, default=_threshold, metavar=" ", help=HELP_THRESHOLD)
    add("--start-date", type=_date, metavar=" ", help=HELP_START_DATE)
    add("--end-date", type=_date, metavar=" ", help=HELP_END_DATE)
    add("--ssvids", type=str, nargs="+", metavar=" ", help=HELP_SSVIDS)
    add("--show-progress", action="store_true", help=HELP_SHOW_PROGRESS)
    add("--mock-db-client", action="store_true", help=HELP_MOCK_DB_CLIENT)
    add("--save", action="store_true", help=HELP_SAVE)
    add("--work-dir", type=Path, default=Path(ct.WORK_DIR), metavar=" ", help=HELP_WORK_DIR)
    add("-v", "--verbose", action="store_true", help=HELP_VERBOSE)

    args = p.parse_args(args=args or ["--help"])

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    command = args.func
    del args.func
    del args.verbose

    args = vars(args)

    # TODO: abstract this as a function of particular command.
    query_params_keys = ["start_date", "end_date", "ssvids"]
    query_params = {}
    for k in query_params_keys:
        if args[k] is not None:
            query_params[k] = args[k]
        del args[k]

    try:
        command(query_params=query_params, **args)
    except ValueError as e:
        logger.error(e)


def main():
    cli(sys.argv[1:])


if __name__ == "__main__":
    main()
