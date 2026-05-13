"""Unit tests for the helpers in ``tests/integration/mode_equivalence.py``.

The integration script itself is not run from pytest -- it is invoked
manually against Dataflow. These tests cover the *pure-function* pieces
that the --resume mechanism depends on:

* ``_dataflow_job_name`` -- canonical job-name encoding. The whole resume
  contract is "the encoding is deterministic and used by both the
  submission path and the resume probe", so the encoding's regression
  surface lives here.

* ``_list_completed_units`` -- parses ``gcloud dataflow jobs list``
  output. Mocked subprocess test.

* The link between the two: a small probe that constructs a cfg, calls
  ``_dataflow_job_name``, and asserts the result is the exact string a
  ``_list_completed_units`` match would need.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from tests.integration import mode_equivalence as me


# ---------------------------------------------------------------------------
# _dataflow_job_name: the canonical encoding.
# ---------------------------------------------------------------------------


def _cfg(bq_output_gaps: str, start: str, end: str) -> SimpleNamespace:
    """Minimal cfg with just the fields _dataflow_job_name reads."""
    return SimpleNamespace(bq_output_gaps=bq_output_gaps, date_range=(start, end))


def test_dataflow_job_name_regression_known_case():
    """Lock in a known-good encoding observed in the wild.

    If this test fails after a refactor, the resume contract is broken:
    a job submitted under the new encoding will not be matched against
    a job listed under the old encoding from a prior run.
    """
    cfg = _cfg(
        "world-fishing-827.tech_great_expectations.three_way_d83164e_a23bd2_2_bfd",
        "2026-04-28", "2026-05-02",
    )
    assert me._dataflow_job_name(cfg) == (
        "three-way-eq-three-way-d83164e-a23bd2-2-bfd-2026-04-28-2026-05-02"
    )


def test_dataflow_job_name_uses_basename_not_full_fqn():
    """Project + dataset prefix must be stripped from the encoding."""
    full = _cfg(
        "world-fishing-827.tech_great_expectations.three_way_abc_def_1_bf",
        "2012-01-01", "2026-05-11",
    )
    bare = _cfg("three_way_abc_def_1_bf", "2012-01-01", "2026-05-11")
    assert me._dataflow_job_name(full) == me._dataflow_job_name(bare)


def test_dataflow_job_name_replaces_underscores_with_hyphens():
    """GCE label rules: hyphens only. All underscores in the basename
    must be converted, otherwise Dataflow auto-truncates+normalises and
    the resume probe can no longer match by exact name."""
    cfg = _cfg("scratch.three_way_abc_def_2_bfd", "2026-04-28", "2026-05-02")
    name = me._dataflow_job_name(cfg)
    assert "_" not in name
    assert name.startswith("three-way-eq-three-way-abc-def-2-bfd-")


def test_dataflow_job_name_is_deterministic():
    """Same input -> same output, byte-for-byte."""
    cfg = _cfg(
        "world-fishing-827.tech_great_expectations.three_way_d83164e_a23bd2_3_bftruncate",
        "2026-04-30", "2026-05-04",
    )
    assert me._dataflow_job_name(cfg) == me._dataflow_job_name(cfg)


def test_dataflow_job_name_distinguishes_pipelines():
    """The pipeline_part (1_bf / 2_bfd / 3_bftruncate / 4_mutate_recover)
    is embedded in the basename, so different pipelines on the same
    date_range produce different names."""
    bf = _cfg("scratch.three_way_abc_def_1_bf", "2012-01-01", "2026-05-11")
    bfd = _cfg("scratch.three_way_abc_def_2_bfd", "2012-01-01", "2026-05-11")
    bft = _cfg("scratch.three_way_abc_def_3_bftruncate", "2012-01-01", "2026-05-11")
    mr = _cfg("scratch.three_way_abc_def_4_mutate_recover", "2012-01-01", "2026-05-11")
    names = {me._dataflow_job_name(c) for c in (bf, bfd, bft, mr)}
    assert len(names) == 4


def test_dataflow_job_name_distinguishes_date_ranges():
    """Each daily-tail iter has a distinct ``(d-W, d)`` window, so the
    name varies per iter -- the resume probe needs that to skip only
    actually-completed iters."""
    base = "scratch.three_way_abc_def_2_bfd"
    iter1 = _cfg(base, "2026-04-28", "2026-05-02")
    iter2 = _cfg(base, "2026-04-29", "2026-05-03")
    assert me._dataflow_job_name(iter1) != me._dataflow_job_name(iter2)


# ---------------------------------------------------------------------------
# _run_dataflow uses _dataflow_job_name. This guards against drift between
# the submitted job name and the name the resume probe looks for. The link
# is "they share the same helper" -- if a future refactor inlines the name
# back into _run_dataflow, this test fails.
# ---------------------------------------------------------------------------


def test_run_dataflow_uses_canonical_job_name():
    """Verify _run_dataflow builds the same job name _dataflow_job_name does.

    Static inspection: both call sites must come through the helper. We
    assert this by checking the helper is called when _run_dataflow runs
    (without actually running it -- we mock out everything inside).
    """
    import inspect
    src = inspect.getsource(me._run_dataflow)
    assert "_dataflow_job_name(cfg)" in src, (
        "Resume contract broken: _run_dataflow must compute job_name via "
        "_dataflow_job_name() so the probe at startup looks for the right "
        "string. If you've refactored, port the helper through both call sites."
    )


def test_run_dataflow_short_circuits_when_unit_completed():
    """If the unit's canonical name is already in _COMPLETED_UNITS, the
    Dataflow runner returns without submitting -- the core of the resume
    short-circuit."""
    cfg = SimpleNamespace(
        bq_output_gaps="scratch.three_way_abc_def_1_bf",
        date_range=("2012-01-01", "2026-05-11"),
        unknown_parsed_args={},
        unknown_unparsed_args=[],
    )
    job_name = me._dataflow_job_name(cfg)

    # Ensure no other test left state behind.
    me._COMPLETED_UNITS.clear()
    me._COMPLETED_UNITS.add(job_name)
    try:
        # If short-circuit works, _run_dataflow returns without importing
        # apache_beam / talking to Dataflow. We confirm by patching the
        # Beam import path -- if reached, it would explode.
        with patch.dict(
            "sys.modules",
            {"gfw.common.beam.pipeline.factory": None},  # would ImportError if used
        ):
            me._run_dataflow(cfg)  # must not raise
    finally:
        me._COMPLETED_UNITS.clear()


# ---------------------------------------------------------------------------
# _list_completed_units: parsing gcloud output.
# ---------------------------------------------------------------------------


_GCLOUD_OUTPUT = (
    "three-way-eq-three-way-d83164e-a23bd2-1-bf-2012-01-01-2026-05-11\tDone\n"
    "three-way-eq-three-way-d83164e-a23bd2-2-bfd-2012-01-01-2026-05-01\tDone\n"
    "three-way-eq-three-way-d83164e-a23bd2-3-bftruncate-2012-01-01-2026-05-11\tDone\n"
    "three-way-eq-three-way-d83164e-a23bd2-2-bfd-2026-04-28-2026-05-02\tDone\n"
    "three-way-eq-three-way-d83164e-84a760-1-bf-2012-01-01-2026-05-11\tFailed\n"
    "three-way-eq-three-way-d83164e-84a760-2-bfd-2012-01-01-2026-05-01\tCancelled\n"
)


def test_list_completed_units_keeps_only_done():
    """Failed / Cancelled jobs must not be treated as completed -- they
    should re-run on resume."""
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = SimpleNamespace(stdout=_GCLOUD_OUTPUT, returncode=0)
        completed = me._list_completed_units("d83164e_a23bd2")

    assert completed == {
        "three-way-eq-three-way-d83164e-a23bd2-1-bf-2012-01-01-2026-05-11",
        "three-way-eq-three-way-d83164e-a23bd2-2-bfd-2012-01-01-2026-05-01",
        "three-way-eq-three-way-d83164e-a23bd2-3-bftruncate-2012-01-01-2026-05-11",
        "three-way-eq-three-way-d83164e-a23bd2-2-bfd-2026-04-28-2026-05-02",
    }


def test_list_completed_units_filters_on_dashed_suffix():
    """Filter passed to gcloud must use the suffix's dashed form -- jobs
    are named with hyphens, not underscores."""
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = SimpleNamespace(stdout="", returncode=0)
        me._list_completed_units("d83164e_a23bd2")

    args = mock_run.call_args[0][0]
    filter_arg = next(a for a in args if a.startswith("--filter="))
    assert "three-way-eq-three-way-d83164e-a23bd2" in filter_arg
    assert "d83164e_a23bd2" not in filter_arg  # the underscored form must NOT appear


def test_list_completed_units_empty_on_no_match():
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = SimpleNamespace(stdout="", returncode=0)
        assert me._list_completed_units("nonexistent_suffix") == set()


# ---------------------------------------------------------------------------
# End-to-end probe: from cfg -> job name -> set membership. This is the
# resume hot-path and the integration of the two layers above.
# ---------------------------------------------------------------------------


def test_resume_probe_recognises_a_previously_completed_unit():
    """Build a cfg that matches a name in the gcloud output; ensure the
    set returned by _list_completed_units contains the cfg's job name."""
    cfg = _cfg(
        "world-fishing-827.tech_great_expectations.three_way_d83164e_a23bd2_2_bfd",
        "2026-04-28", "2026-05-02",
    )
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = SimpleNamespace(stdout=_GCLOUD_OUTPUT, returncode=0)
        completed = me._list_completed_units("d83164e_a23bd2")

    assert me._dataflow_job_name(cfg) in completed


def test_resume_probe_does_not_recognise_a_failed_unit():
    """An earlier failed run (suffix d83164e_84a760, state=Failed) must
    not be treated as completed even though its name appears in the
    gcloud output."""
    cfg = _cfg(
        "world-fishing-827.tech_great_expectations.three_way_d83164e_84a760_1_bf",
        "2012-01-01", "2026-05-11",
    )
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = SimpleNamespace(stdout=_GCLOUD_OUTPUT, returncode=0)
        completed = me._list_completed_units("d83164e_84a760")

    assert me._dataflow_job_name(cfg) not in completed
