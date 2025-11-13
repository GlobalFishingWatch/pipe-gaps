"""This module implements a CLI for the gaps pipeline."""
import sys
import logging


from gfw.common.cli import CLI, Option
from gfw.common.cli.actions import NestedKeyValueAction
from gfw.common.logging import LoggerConfig
from gfw.common.cli.formatting import default_formatter

from pipe_gaps.version import __version__
from pipe_gaps.cli.commands import DetectGaps, PublishGaps


logger = logging.getLogger(__name__)


NAME = "pipe-gaps"
DESCRIPTION = "Tools for creating gap events (time gaps in AIS position messages)."
HELP_LABELS = "Labels to audit costs over the queries."


def run(args):
    gaps_cli = CLI(
        name=NAME,
        description=DESCRIPTION,
        formatter=default_formatter(max_pos=120),
        subcommands=[
            DetectGaps,
            PublishGaps,
        ],
        options=[  # Common options for all subcommands.
            Option(
                "--labels", type=str, nargs="*", action=NestedKeyValueAction, help=HELP_LABELS
            ),
        ],
        version=__version__,
        examples=[
            "pipe-gaps detect -c config/sample-from-file-to-file.json --min-gap-length 1.3",
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
