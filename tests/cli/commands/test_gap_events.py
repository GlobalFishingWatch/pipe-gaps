from pipe_gaps.cli import main


BASE_ARGS = [
    "gap-events",
    "--bq-in-raw-gaps", "project.dataset.table",
    "--bq-in-segment-info", "project.dataset.table",
    "--bq-in-segs-activity", "project.dataset.table",
    "--bq-in-voyages", "project.dataset.table",
    "--bq-in-port-visits", "project.dataset.table",
    "--bq-in-regions", "project.dataset.table",
    "--bq-in-regions-registry", "project.dataset.registry",
    "--bq-in-vessels-byyear", "project.dataset.table",
    "--bq-out-gap-events", "project.dataset.output",
    "--date-range", "2024-01-01,2024-01-02",
    "--project", "test-project",
]


def test_cli_executes_run(tmp_path):
    main.run(BASE_ARGS + ["--mock-bq-clients"])


def test_cli_executes_run_with_disabling_classification(tmp_path):
    args = BASE_ARGS + [
        "--mock-bq-clients",
        "--classify-disabling",
        "--bq-in-sat-reception", "project.dataset.sat_reception",
        "--disabling-min-gap-duration-h", "6",
        "--disabling-min-distance-from-shore-m", "1000",
        "--disabling-min-reception-positions-per-day", "5",
        "--disabling-min-positions-before", "2",
    ]

    main.run(args)


def test_labels_resolve_from_cli():
    # --labels is a common option shared by every subcommand (see cli/main.py) -- confirms
    # it still resolves for gap-events now that it's no longer declared per-command. See
    # PIPELINE-4643.
    args = BASE_ARGS + ["--only-render", "--labels", "team=pipeline", "env=prod"]
    _, config = main.run(args)

    assert config["labels"] == {"team": "pipeline", "env": "prod"}
