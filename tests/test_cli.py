import pytest

from pipe_gaps import cli


def test_cli(tmp_path):
    args = [
        "--work-dir",
        str(tmp_path)
    ]

    # With config file and verbose flag.
    args.extend(["--config-file", "config/sample-from-file.json", "--pipe-type", "naive"])
    args.extend(["-v"])
    cli.cli(args)

    with pytest.raises(SystemExit):
        cli.cli([])
