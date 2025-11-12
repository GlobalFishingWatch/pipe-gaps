from pipe_gaps.cli import main


def test_cli_executes_run(tmp_path):
    args = [
        "raw-gaps-events",
        "--bq-input-source-raw-gaps", "project.dataset.table",
        "--bq-output", "project.dataset.output",
        "--date-range", "2024-01-01,2024-01-02",
        "--project", "test-project",
        "--mock-bq-clients",
    ]

    main.run(args)
