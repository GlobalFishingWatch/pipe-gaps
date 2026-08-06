from pipe_gaps.cli import main


def test_cli_executes_run(tmp_path):
    args = [
        "gap-events",
        "--bq-in-raw-gaps", "project.dataset.table",
        "--bq-in-segment-info", "project.dataset.table",
        "--bq-in-segs-activity", "project.dataset.table",
        "--bq-in-voyages", "project.dataset.table",
        "--bq-in-port-visits", "project.dataset.table",
        "--bq-in-regions", "project.dataset.table",
        "--bq-in-vessels-byyear", "project.dataset.table",
        "--bq-out-gap-events", "project.dataset.output",
        "--date-range", "2024-01-01,2024-01-02",
        "--project", "test-project",
        "--mock-bq-clients",
    ]

    main.run(args)
