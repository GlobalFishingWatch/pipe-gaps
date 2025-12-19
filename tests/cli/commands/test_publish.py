from pipe_gaps.cli import main


def test_cli_executes_run(tmp_path):
    args = [
        "publish",
        "--bq-input-gaps", "project.dataset.table",
        "--bq-input-segment-info", "project.dataset.table",
        "--bq-input-segs-activity", "project.dataset.table",
        "--bq-input-voyages", "project.dataset.table",
        "--bq-input-port-visits", "project.dataset.table",
        "--bq-input-regions", "project.dataset.table",
        "--bq-input-vessels-byyear", "project.dataset.table",
        "--bq-output", "project.dataset.output",
        "--date-range", "2024-01-01,2024-01-02",
        "--project", "test-project",
        "--mock-bq-clients",
    ]

    main.run(args)
