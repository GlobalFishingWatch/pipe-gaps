from pipe_gaps.cli import main


def test_cli_executes_run(tmp_path):
    args = [
        "detect",
        "--bq-input-messages", "project.dataset.table",
        "--bq-input-segments", "project.dataset.segments",
        "--bq-output-gaps", "project.dataset.output",
        "--date-range", "2024-01-01,2024-01-02",
        "--min-gap-length", "4",
        "--open-gaps-start-date", "2020-01-01",
        "--work-dir", str(tmp_path),
        "--filter-good-seg",
        "--filter-not-overlapping-and-short",
        "--project", "test-project",
        "--mock-bq-clients",
        "--save-json",
    ]

    main.run(args)
