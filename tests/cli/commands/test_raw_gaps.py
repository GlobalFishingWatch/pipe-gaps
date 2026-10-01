from pipe_gaps.cli import main


BASE_ARGS = [
    "raw-gaps",
    "--bq-in-messages", "project.dataset.table",
    "--bq-in-segments", "project.dataset.segments",
    "--bq-out-gaps", "project.dataset.output",
    "--date-range", "2024-01-01,2024-01-02",
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


def test_eval_last_defaults_to_true_when_not_passed():
    """Regression test: --eval-last must default to True, not None.

    A CLI bool Option without an explicit default resolves to None when
    the flag isn't passed, which overrides RawGapsConfig.eval_last's own
    True default and silently disables open-gap detection on incremental
    runs (only closed gaps got written from 2026-08-14 onward on the
    gaps_v1.raw_gaps table).
    """
    _, config = main.run(BASE_ARGS + ["--only-render"])

    assert config["eval_last"] is True


def test_eval_last_flag_explicitly_enables():
    _, config = main.run(BASE_ARGS + ["--only-render", "--eval-last"])

    assert config["eval_last"] is True


def test_no_eval_last_flag_disables():
    _, config = main.run(BASE_ARGS + ["--only-render", "--no-eval-last"])

    assert config["eval_last"] is False
