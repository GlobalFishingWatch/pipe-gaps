from pipe_gaps.cli import main


def test_cli_executes_run(tmp_path):
    args = [
        "raw-gaps",
        "--bq-input-messages", "project.dataset.table",
        "--bq-input-segments", "project.dataset.segments",
        "--bq-output-gaps", "project.dataset.output",
        "--date-range", "2024-01-01,2024-01-02",
        "--work-dir", str(tmp_path),
        "--filter-good-seg",
        "--filter-not-overlapping-and-short",
        "--project", "test-project",
        "--mock-bq-clients",
        "--save-json",
    ]

    main.run(args)
