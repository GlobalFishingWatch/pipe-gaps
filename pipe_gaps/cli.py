"""This module implements a CLI for the gaps pipeline."""
import sys
import logging
import argparse
import collections
from argparse import BooleanOptionalAction

from pathlib import Path

from pipe_gaps import utils
from pipe_gaps import pipeline

logger = logging.getLogger(__name__)


NAME = "pipe-gaps"
DESCRIPTION = """
    Detects time gaps in AIS position messages.
    The definition of a gap is configurable by a time threshold.
"""
EPILOG = (
    "Example: \n"
    "    pipe-gaps -c config/sample-from-file-1.json --threshold 1.3"
)

_DEFAULT = "(default: %(default)s)"
# TODO: try to get this descriptions from docstrings, so we don´t duplicate them.
HELP_CONFIG_FILE = f"JSON file with pipeline configuration {_DEFAULT}."
HELP_INPUT_FILE = "JSON file with main inputs for the pipeline."
HELP_SIDE_INPUT_FILE = "JSON file with side inputs for the pipeline."
HELP_THRESHOLD = "Minimum time difference (hours) to start considering gaps."
HELP_START_DATE = "Query filter: messages after this dete, e.g., '2024-01-01'."
HELP_END_DATE = "Query filter: messages before this date, e.g., '2024-01-02'."
HELP_SSVIDS = "Query filter: list of ssvids."
HELP_SORT_METHOD = "Sorting algorihtm."
HELP_EVAL_LAST = "If passed, evaluates last message of each SSVID to create an open gap."
HELP_SHOW_PROGRESS = "If passed, renders a progress bar."
HELP_MOCK_DB_CLIENT = "If passed, mocks the DB client. Useful for development and testing."
HELP_SAVE_JSON = "If passed, saves the results in JSON file."
HELP_SAVE_STATS = "If passed, saves some statistics."
HELP_PIPE_TYPE = "Pipeline type: ['naive', 'beam']."
HELP_WORK_DIR = "Directory to use as working directory."
HELP_VERBOSE = "Set logger level to DEBUG."


def formatter():
    """Returns a formatter for argparse help."""

    def formatter(prog):
        return argparse.RawTextHelpFormatter(prog, max_help_position=50)

    return formatter


def dict_update(d, u):
    """Updates a nested dictionary."""
    for k, v in u.items():
        if isinstance(v, collections.abc.Mapping):
            d[k] = dict_update(d.get(k) or {}, v)
        else:
            d[k] = v
    return d


def cli(args):
    """CLI for gaps pipeline."""
    utils.setup_logger(
        warning_level=[
            "apache_beam.runners.portability",
            "apache_beam.runners.worker.bundle_processor",
            "apache_beam.io.filesystem",
            "apache_beam.io.gcp.bigquery_tools",
            "urllib3"
        ],
        error_level=[
            "apache_beam.coders.coder_impl"
            # to supress warning "Using fallback deterministic coder for type..."
        ]
    )

    p = argparse.ArgumentParser(
        prog=NAME,
        description=DESCRIPTION,
        epilog=EPILOG,
        formatter_class=formatter(),
        argument_default=argparse.SUPPRESS
    )

    add = p.add_argument
    add("-c", "--config-file", type=Path, default=None, metavar=" ", help=HELP_CONFIG_FILE)
    add("--pipe-type", type=str, metavar=" ", help=HELP_PIPE_TYPE)
    add("--save-stats", default=None, action=BooleanOptionalAction, help=HELP_SAVE_STATS)
    add("--work-dir", type=Path, metavar=" ", help=HELP_WORK_DIR)
    add("-v", "--verbose", action="store_true", default=False, help=HELP_VERBOSE)

    # TODO: see if we want to support these CLI args.

    # add("--save-json", default=None, action=BooleanOptionalAction, help=HELP_SAVE_JSON)

    # add = p.add_argument_group("pipeline inputs").add_argument
    # add("-i", "--input-file", type=Path, metavar=" ", help=HELP_INPUT_FILE)
    # add("-s", "--side-input-file", type=Path, metavar=" ", help=HELP_SIDE_INPUT_FILE)
    # add("--start-date", type=str, metavar=" ", help=HELP_START_DATE)
    # add("--end-date", type=str, metavar=" ", help=HELP_END_DATE)
    # add("--ssvids", type=str, nargs="+", metavar=" ", help=HELP_SSVIDS)
    # add("--mock-db-client", default=None, action=BooleanOptionalAction, help=HELP_MOCK_DB_CLIENT)

    add = p.add_argument_group("pipeline core process").add_argument
    add("--threshold", type=float, metavar=" ", help=HELP_THRESHOLD)
    add("--sort-method", type=str, metavar=" ", help=HELP_SORT_METHOD)
    add("--show-progress", default=None, action=BooleanOptionalAction, help=HELP_SHOW_PROGRESS)
    add("--eval-last", default=None, action=BooleanOptionalAction, help=HELP_EVAL_LAST)

    CORE_KEYS = ["threshold", "show_progress", "eval_last"]

    ns, unknown = p.parse_known_args(args=args or ["--help"])

    config_file = ns.config_file
    verbose = ns.verbose

    # Delete CLI configuration from parsed namespace.
    del ns.verbose
    del ns.config_file

    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    config = {}
    # Load config file if exists.
    if config_file is not None:
        config = utils.json_load(config_file)

    # Convert namespace of args to dict.
    cli_args = vars(ns)

    for arg in list(cli_args):
        if cli_args[arg] is None:
            del cli_args[arg]

    for k in CORE_KEYS:
        if k in cli_args:
            cli_args.setdefault("core", {})[k] = cli_args.pop(k)

    # Parse unknown arguments to dict.
    options = dict(zip(unknown[:-1:2], unknown[1::2]))
    options = {k.replace("--", ""): v for k, v in options.items()}

    # Build config dictionary from CLI params
    cli_config = {}
    pipe_type = cli_args.pop("pipe_type", None)
    if pipe_type is not None:
        cli_config["pipe_type"] = pipe_type

    cli_config["pipe_config"] = cli_args
    cli_config["pipe_config"]["options"] = options

    # Override config file with CLI params
    config = dict_update(config, cli_config)

    # Run pipeline with parsed config.
    try:
        pipeline.factory.from_config(config).run()
    except pipeline.PipelineError as e:
        logger.error(e)


def main():
    cli(sys.argv[1:])


if __name__ == "__main__":
    main()
