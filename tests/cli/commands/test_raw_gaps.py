from pipe_gaps.cli import main


BASE_ARGS = [
    "raw-gaps",
    "--bq-in-messages", "project.dataset.table",
    "--bq-in-segments", "project.dataset.segments",
    "--bq-out-gaps", "project.dataset.output",
    "--start-date", "2024-01-01",
    "--end-date", "2024-01-02",
    "--labels", "env=test",
    "--min-gap-length", "4",
    "--open-gaps-start-date", "2020-01-01",
    "--project", "test-project",
]


def test_cli_executes_run(tmp_path):
    args = BASE_ARGS + [
        "--work-dir", str(tmp_path),
        "--filter-good-seg",
        "--filter-not-overlapping-and-short",
        "--mock-bq-clients",
        "--save-json",
    ]

    main.run(args)


def test_labels_resolve_from_cli():
    # --labels is a common option shared by every subcommand (see cli/main.py), forwarded
    # by PipelineFactory to the Dataflow job -- see PIPELINE-4643.
    args = BASE_ARGS + ["--only-render", "--labels", "team=pipeline", "env=prod"]
    _, config = main.run(args)

    assert config["labels"] == {"team": "pipeline", "env": "prod"}
