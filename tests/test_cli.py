import pytest

from pipe_gaps import cli
from pipe_gaps.data import sample_messages_path


def test_cli(tmp_path):
    args = [
        "--input-file",
        f"{sample_messages_path()}",
        "--work-dir",
        str(tmp_path),
        "--threshold",
        "12",
        "--start-date",
        "2024-04-12",
        "--end-date",
        "2024-04-13",
    ]

    cli.cli(args)

    # With verbose flag.
    args.extend(["-v"])
    cli.cli(args)

    # Fetching from mocked client.
    args = args[2:]
    args.append("--mock-db-client")
    cli.cli(args)

    with pytest.raises(SystemExit):
        cli.cli([])
