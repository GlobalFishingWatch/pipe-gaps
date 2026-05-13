"""Mode equivalence integration test for the pipe-gaps detect pipeline.

Drives the same pipeline several different ways ("modes") and asserts all
modes produce identical output on the ``..._last_versions`` views. The
triggering pattern should never affect the result; any divergence between
modes signals a bug in either the rewrite path, the DELETE pre-hook, or
the v1-seed/close-path mechanism.

Output tables (each gets its own UUID-suffixed output in
``world-fishing-827.tech_great_expectations``):

* ``_1_bf``: a single backfill ``main.run`` over ``[start, end)``. The
  range-mode oracle.
* ``_2_bfd``: a backfill ending ``--tail-days`` short, then per-day daily
  loads. Each daily load uses the production-style ``[d - W, d)`` window
  (W = ``--backfill-days``). Mirrors steady-state daily reprocessing.
* ``_3_bftruncate``: a full backfill, then the same daily-tail re-runs as
  ``_2_bfd``. Tests that re-running already-processed days truncates and
  re-emits idempotently.
* ``_4_mutate_recover`` (opt-in via ``--enable-pipeline-4``): a partial
  backfill on global ssvids, then a daily-tail with ``--restricted-ssvids``
  (or ``--auto-restrict``), then a daily-tail on the global set. Simulates
  recovery from a daily run that saw partial source data (e.g. data not
  yet visible for some ssvids), then reprocessing once full data became
  available. Compared against ``_1_bf``.

When ``--auto-restrict`` is set, the script queries
``_1_bf_last_versions`` after the parallel phase finishes and picks
``~|G|/2`` non-triggering ssvids -- so the complement is guaranteed to
contain every ssvid whose gap shape would expose Bug A under the
candidate fix being validated. This is automatic; no manual ssvid list is
required.

Equivalence is checked by shelling out to ``table-check summary`` for each
pair (``_1_bf`` vs ``_2_bfd``, ``_1_bf`` vs ``_3_bftruncate``,
``_2_bfd`` vs ``_3_bftruncate``, plus ``_1_bf`` vs ``_4_mutate_recover``
when pipeline 4 is enabled) on the SCD-2-deduplicated ``_last_versions``
views.

Manual invocation -- not a pytest test::

    # Three-mode run, full year:
    python -m tests.integration.mode_equivalence --runner dataflow --parallel

    # Four-mode run with auto-picked restricted ssvid set:
    python -m tests.integration.mode_equivalence --runner dataflow --parallel \\
        --enable-pipeline-4 --auto-restrict

    # Local development against a small ssvid cohort:
    python -m tests.integration.mode_equivalence --runner local \\
        --ssvids 412345678,477334300

Runner modes (``--runner``):

* ``local``: in-process Python (DirectRunner). Fast; useful with
  ``--ssvids`` for development.
* ``docker``: ``docker compose run dev pipe-gaps detect ...`` against the
  dev image (DirectRunner inside the container). The dev image is rebuilt
  once per invocation so the run reflects the current source tree.
* ``dataflow``: in-process Python orchestrator that submits the pipeline
  to Dataflow and blocks until each job completes.

Pass ``--parallel`` (alias ``--async``) to run the top-level pipelines
concurrently via a thread pool. This roughly cuts wall-clock time by Nx
for ``docker`` and ``dataflow`` runners. With ``--auto-restrict``,
pipeline 4 always runs sequentially after the parallel phase because it
needs ``_1_bf`` populated first. Not recommended for ``local``: multiple
Beam DirectRunner pipelines in the same process are not known to be safe.

The default settings target VMS production parameters
(``min_gap_length=1``, ``n_hours_before=12``, ``filter_good_seg=True``,
``W=4``) over the staging dataset ``pipe_ais_test_202408290000_published``
and calendar year 2020. Note: ``--window-period-d`` defaults to 2 here for
historical reasons; production VMS leaves it unset, which auto-derives to
the 4-day date_range size for daily runs.
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import subprocess
import sys
import threading
import uuid

from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Optional

from pipe_gaps.pipelines.detect import main as detect_main


logger = logging.getLogger(__name__)


def _git_info(repo_dir: str) -> tuple[str, bool]:
    """Returns (short commit hash, dirty?) for the repo.

    "dirty" means a *tracked* file has been modified or is staged but not
    committed. Untracked files don't count -- they don't affect the imported
    pipeline code, and rejecting on them would be noisy (local scratch
    directories, editor swap files, etc.).
    """
    short = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        cwd=repo_dir, check=True, capture_output=True, text=True,
    ).stdout.strip()
    porcelain = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=no"],
        cwd=repo_dir, check=True, capture_output=True, text=True,
    ).stdout.strip()
    return short, bool(porcelain)


def _resolve_suffix(args: argparse.Namespace, repo_dir: str) -> str:
    """Build the run suffix, embedding the commit hash for traceability.

    With --suffix explicitly set, return as-is (re-runs against an existing run
    skip the git inspection). Otherwise derive suffix from <commit>[-dirty]-<uuid8>.
    Refuses to run with a dirty tree unless --allow-dirty-tree is set, since the
    embedded commit hash would not actually identify the code that ran.
    """
    if args.suffix is not None:
        return args.suffix
    commit, dirty = _git_info(repo_dir)
    if dirty and not args.allow_dirty_tree:
        raise SystemExit(
            f"Refusing to run with uncommitted changes (commit={commit}, dirty=True). "
            "Commit your changes so the run is traceable, or pass --allow-dirty-tree to "
            "override (the suffix will include 'dirty' to flag this)."
        )
    # Use underscores instead of hyphens: pipe-gaps' DELETE pre-hook template
    # does not backtick the table identifier (assets/queries/gaps_delete.sql.j2),
    # so a hyphen in the table name renders as a SQL minus sign and the query
    # fails to parse. Underscores are safe in BQ table identifiers.
    suffix = f"{commit}_dirty_{uuid.uuid4().hex[:6]}" if dirty else f"{commit}_{uuid.uuid4().hex[:6]}"
    return suffix


PROJECT = "world-fishing-827"
DEST_DATASET = "tech_great_expectations"

# Default source dataset (matches pipe-events' staging script).
DEFAULT_SOURCE_DATASET = "pipe_ais_test_202408290000_published"

# Pipeline parameters (VMS-matching, taken from
# config/sample-vms-from-bq-2-days.json plus the W=4 backfill width
# observed in production DELETE statements).
DEFAULT_MIN_GAP_LENGTH = 1.0
DEFAULT_N_HOURS_BEFORE = 12
DEFAULT_WINDOW_PERIOD_D = 2
DEFAULT_FILTER_GOOD_SEG = True
DEFAULT_BACKFILL_DAYS_W = 4

# Default date range -- same as the pipe-events staging example.
DEFAULT_START = "2020-01-01"
DEFAULT_END = "2021-01-01"
DEFAULT_TAIL_DAYS = 4

# Service account used to run Dataflow worker VMs. Must have the roles
# expected by the framework (Dataflow Worker, BigQuery Data Editor on the
# destination dataset, BigQuery Job User on PROJECT, and read on the source
# dataset/staging bucket). Override with --service-account if needed.
DEFAULT_DATAFLOW_SA = "automated-testing@world-fishing-827.iam.gserviceaccount.com"

# By default, ``ReadFromBigQuery`` with method=EXPORT auto-generates a
# throwaway dataset named ``bq_temp_<uuid>`` to hold the query result before
# exporting it to GCS, which requires ``bigquery.datasets.create`` at
# project level. We don't want to grant that to the test SA, so we point
# Beam at an existing dataset instead -- it then creates only a temp
# *table* there and deletes it at job end, requiring only dataEditor on
# this dataset. Set to None / empty to use Beam's default behavior.
DEFAULT_BQ_TEMP_DATASET = f"{PROJECT}.{DEST_DATASET}"

# Dataflow region / temp+staging bucket / subnet. gfw-common's
# default_options hardcodes us-east1 + pipe-temp-us-east-ttl7 +
# gfw-internal-us-east1, but the test SA's GCS write permissions are on
# pipe-temp-us-central-ttl7. The us-central1 values below match the ones
# used in ``config/historical-backfill--2020-01-01.json``.
DEFAULT_DATAFLOW_REGION = "us-central1"
DEFAULT_DATAFLOW_TEMP_BUCKET = "pipe-temp-us-central-ttl7"
DEFAULT_DATAFLOW_SUBNETWORK = "regions/us-central1/subnetworks/gfw-internal-us-central1"


# --------------------------------------------------------------------------
# Pipeline runner dispatch
# --------------------------------------------------------------------------


def _make_config(
    *,
    start: date,
    end: date,
    bq_input_messages: str,
    bq_input_segments: str,
    bq_output_gaps: str,
    ssvids: tuple[str, ...],
    min_gap_length: float,
    n_hours_before: int,
    window_period_d: int,
    filter_good_seg: bool,
    skip_open_gaps: bool,
    service_account: Optional[str] = None,
    bq_temp_dataset: Optional[str] = None,
    dataflow_region: Optional[str] = None,
    dataflow_temp_bucket: Optional[str] = None,
    dataflow_subnetwork: Optional[str] = None,
    max_num_workers: Optional[int] = None,
    as_of_timestamp: Optional[str] = None,
) -> SimpleNamespace:
    """Build the SimpleNamespace expected by ``detect.main.run``."""
    cfg = SimpleNamespace(
        date_range=(start.isoformat(), end.isoformat()),
        bq_input_messages=bq_input_messages,
        bq_input_segments=bq_input_segments,
        as_of_timestamp=as_of_timestamp,
        bq_output_gaps=bq_output_gaps,
        ssvids=ssvids,
        min_gap_length=min_gap_length,
        n_hours_before=n_hours_before,
        window_period_d=window_period_d,
        filter_good_seg=filter_good_seg,
        skip_open_gaps=skip_open_gaps,
        # The detect pipeline reads opens from the same table it writes to
        # when bq_input_open_gaps is unset.
        #
        # NOTE: do not set ``sdk_container_image`` here. The framework's
        # ``Pipeline.pipeline_options`` checks ``if "sdk_container_image"
        # not in options`` and falls back to ``setup_file = "./setup.py"``
        # when absent. Setting it (even to "") suppresses that fallback,
        # which on Dataflow leaves workers without the pipe-gaps source
        # distribution -- they fail with "Unable to open file: ...
        # pipeline.pb" because the staging step doesn't run.
        unknown_parsed_args={"project": PROJECT},
        unknown_unparsed_args=[],
    )
    # Carried through to _run_dataflow as ``service_account_email``.
    # Stashed as a plain attribute (not in unknown_parsed_args) so the
    # local/docker runners ignore it.
    cfg.service_account = service_account
    # Same pattern: a Dataflow-only knob the local/docker runners ignore.
    # ``DetectGapsConfig.from_namespace`` doesn't know this field, so
    # _run_dataflow strips it before constructing the dataclass.
    cfg.bq_temp_dataset = bq_temp_dataset
    cfg.dataflow_region = dataflow_region
    cfg.dataflow_temp_bucket = dataflow_temp_bucket
    cfg.dataflow_subnetwork = dataflow_subnetwork
    cfg.max_num_workers = max_num_workers
    return cfg


def _run_local(cfg: SimpleNamespace) -> None:
    """Invoke the pipeline in-process via the imported main function (DirectRunner)."""
    detect_main.run(cfg)


def _cfg_to_cli_flags(cfg: SimpleNamespace) -> list[str]:
    """Translate a SimpleNamespace cfg to ``pipe-gaps detect`` CLI flags.

    Skips fields handled by Beam pipeline options (``unknown_*``); those are
    appended separately.
    """
    flags: list[str] = []

    def _add(name: str, value: object) -> None:
        if value is None or value == "" or value == ():
            return
        flags.append(f"--{name.replace('_', '-')}")
        flags.append(str(value))

    _add("date-range", ",".join(cfg.date_range))
    _add("bq-input-messages", cfg.bq_input_messages)
    _add("bq-input-segments", cfg.bq_input_segments)
    _add("bq-output-gaps", cfg.bq_output_gaps)
    _add("min-gap-length", cfg.min_gap_length)
    _add("n-hours-before", cfg.n_hours_before)
    _add("window-period-d", cfg.window_period_d)
    if cfg.filter_good_seg:
        _add("filter-good-seg", "true")
    if cfg.skip_open_gaps:
        _add("skip-open-gaps", "true")
    if cfg.ssvids:
        _add("ssvids", ",".join(cfg.ssvids))
    return flags


_DOCKER_BUILT = False
_DOCKER_BUILD_LOCK = threading.Lock()


def _ensure_docker_built() -> None:
    """Build the dev image once per script invocation. Thread-safe."""
    global _DOCKER_BUILT
    with _DOCKER_BUILD_LOCK:
        if _DOCKER_BUILT:
            return
        logger.info("docker: building dev image...")
        subprocess.run(["docker", "compose", "build", "dev"], check=True)
        _DOCKER_BUILT = True


def _run_docker(cfg: SimpleNamespace) -> None:
    """Invoke the pipeline via ``docker compose run`` against the dev image.

    The dev image is rebuilt once per script invocation so we run against the
    current source tree.

    Each invocation gets its own compose project name (``-p``) so concurrent
    runs do not race on creating the default network. (Without this, three
    parallel ``docker compose run`` calls all try to create
    ``pipe-gaps_default`` and the daemon errors with "network with name X
    already exists" for the laggers.)
    """
    _ensure_docker_built()

    project_name = f"pipe-gaps-eq-{uuid.uuid4().hex[:8]}"
    cmd = [
        "docker", "compose", "-p", project_name, "run", "--rm",
        "--entrypoint", "pipe-gaps",
        "dev", "detect",
        *_cfg_to_cli_flags(cfg),
        "--project", PROJECT,
    ]
    logger.info("docker: %s", " ".join(cmd))
    subprocess.run(cmd, check=True)


_DATAFLOW_SUBMIT_LOCK = threading.Lock()


# Set of Dataflow job names known to have completed successfully -- populated
# at run start when --resume is passed. _run_dataflow checks against this set
# and skips re-submission of any unit whose canonical job name matches a
# completed run.
_COMPLETED_UNITS: set[str] = set()


def _dataflow_job_name(cfg: SimpleNamespace) -> str:
    """Canonical Dataflow job name for a pipeline unit.

    The name deterministically encodes the unit's identity: ``(suffix,
    pipeline_part, start, end)`` -- all derivable from
    ``cfg.bq_output_gaps`` (which embeds suffix + pipeline_part) and
    ``cfg.date_range``. Both the submission code path (``_run_dataflow``)
    and the resume probe (``_list_completed_units``) call this helper, so
    the encoding stays in lock-step. Treat any change here as breaking the
    resume contract.

    Underscores become hyphens to satisfy GCE label rules (Dataflow itself
    truncates+normalises for monitoring labels, but the *job name* it
    accepts and we query against is what this function returns).
    """
    output_basename = cfg.bq_output_gaps.rsplit(".", 1)[-1]
    start, end = cfg.date_range
    return f"three-way-eq-{output_basename}-{start}-{end}".replace("_", "-")


def _list_completed_units(
    suffix: str,
    *,
    project: str = PROJECT,
    region: str = DEFAULT_DATAFLOW_REGION,
) -> set[str]:
    """One-shot fetch of all Dataflow job names matching ``suffix`` in state Done.

    Used at startup when --resume is passed. Shells out to ``gcloud dataflow
    jobs list`` rather than depending on the dataflow_v1beta3 client library.
    Returns a set so the per-unit check in ``_run_dataflow`` is O(1).
    """
    # Suffix in the job name is dash-separated, so filter on the dashed form.
    filter_prefix = f"three-way-eq-three-way-{suffix.replace('_', '-')}"
    result = subprocess.run(
        [
            "gcloud", "dataflow", "jobs", "list",
            f"--region={region}",
            f"--project={project}",
            "--status=terminated",
            "--format=value(name,state)",
            f"--filter=name:{filter_prefix}",
        ],
        check=False, capture_output=True, text=True,
    )
    completed: set[str] = set()
    for line in result.stdout.splitlines():
        parts = line.split()
        # gcloud's value(name,state) format emits TSV-style "name<tab>state";
        # split() handles either whitespace.
        if len(parts) >= 2 and parts[-1] == "Done":
            completed.add(parts[0])
    return completed


def _run_dataflow(cfg: SimpleNamespace) -> None:
    """Invoke the pipeline via the in-process orchestrator with DataflowRunner.

    Submits the job to Dataflow and blocks until completion. Submission
    (which includes Beam's sdist build via ``python -m build``) is
    serialized via a module-level lock so that concurrent invocations under
    ``--parallel`` don't race on the build's temporary source directory.
    The long ``wait_until_finish()`` runs outside the lock so multiple jobs
    execute concurrently on Dataflow.

    This bypasses ``pipe_gaps.pipelines.detect.main.run`` because that
    function calls ``pipeline.run()`` without exposing the
    ``wait_until_finish`` parameter; we need to split submission from
    waiting to drop the lock between the two.
    """
    # Resume short-circuit: if a previous run completed this exact unit
    # (canonical job name), skip re-submission entirely. The set is
    # populated at main() start when --resume is passed; an empty set
    # (the default) means no-op. We check this BEFORE the Beam imports
    # below so a skipped unit pays no import cost either.
    job_name = _dataflow_job_name(cfg)
    if job_name in _COMPLETED_UNITS:
        logger.info("Resume: skipping completed Dataflow unit %s", job_name)
        return

    # Late imports so the local-runner code path doesn't pay the cost.
    import apache_beam as beam
    from apache_beam.io.gcp.internal.clients import bigquery as bq_clients
    from apache_beam.runners.runner import PipelineState

    from gfw.common.beam.pipeline.factory import PipelineFactory

    from pipe_gaps.pipelines.detect.config import DetectGapsConfig
    from pipe_gaps.pipelines.detect.factory import DetectGapsLinearDagFactory
    from pipe_gaps.version import __version__ as pipe_gaps_version

    parsed = dict(cfg.unknown_parsed_args)
    parsed.setdefault("runner", "DataflowRunner")
    # Make the Dataflow job easy to find in the console AND give --resume
    # a stable key to recognise this unit. The canonical name is computed
    # via _dataflow_job_name so submission and resume-probing stay in sync.
    parsed.setdefault("job_name", job_name)
    if getattr(cfg, "service_account", None):
        parsed.setdefault("service_account_email", cfg.service_account)
    # Override gfw-common's hardcoded us-east1 defaults so workers run in
    # the region the test SA has GCS access to. ChainMap merges parsed_args
    # with highest priority, so setting these here suppresses the defaults.
    region = getattr(cfg, "dataflow_region", None)
    temp_bucket = getattr(cfg, "dataflow_temp_bucket", None)
    subnetwork = getattr(cfg, "dataflow_subnetwork", None)
    if region:
        parsed.setdefault("region", region)
    if temp_bucket:
        parsed.setdefault("temp_location", f"gs://{temp_bucket}/dataflow_temp")
        parsed.setdefault("staging_location", f"gs://{temp_bucket}/dataflow_staging")
    if subnetwork:
        parsed.setdefault("subnetwork", subnetwork)
    max_num_workers = getattr(cfg, "max_num_workers", None)
    if max_num_workers:
        parsed.setdefault("max_num_workers", max_num_workers)

    # ``DetectGapsConfig.from_namespace`` passes namespace attrs through as
    # dataclass kwargs, so any field not on the dataclass blows up
    # construction. Strip the runner-only attributes here.
    runner_only_attrs = {
        "service_account",
        "bq_temp_dataset",
        "dataflow_region",
        "dataflow_temp_bucket",
        "dataflow_subnetwork",
        "max_num_workers",
    }
    cfg_attrs = {k: v for k, v in vars(cfg).items() if k not in runner_only_attrs}
    df_cfg = SimpleNamespace(**cfg_attrs)
    df_cfg.unknown_parsed_args = parsed

    config = DetectGapsConfig.from_namespace(df_cfg, version=pipe_gaps_version)

    # Choose the DAG factory: by default Beam's EXPORT read path creates a
    # throwaway dataset (requires bigquery.datasets.create at project
    # level). When bq_temp_dataset is set, we override the read factory to
    # inject ``temp_dataset=<existing dataset>``, which Beam reuses --
    # creating only a temp table inside it. Strictly more permission-
    # friendly; functionally equivalent for our purposes (same query, same
    # GCS export, same downstream DAG).
    dag_factory_cls = DetectGapsLinearDagFactory
    bq_temp_dataset = getattr(cfg, "bq_temp_dataset", None)
    if bq_temp_dataset:
        temp_proj, temp_ds = bq_temp_dataset.split(".", 1)
        temp_dataset_ref = bq_clients.DatasetReference(
            projectId=temp_proj, datasetId=temp_ds,
        )

        class _DagFactoryWithTempDataset(DetectGapsLinearDagFactory):
            @property
            def read_from_bigquery_factory(self):
                def _factory(**kwargs):
                    kwargs.setdefault("temp_dataset", temp_dataset_ref)
                    return beam.io.ReadFromBigQuery(**kwargs)
                return _factory

        dag_factory_cls = _DagFactoryWithTempDataset

    pipeline = PipelineFactory(config, dag_factory=dag_factory_cls(config)).build_pipeline()

    # Replicates gfw.common.beam.pipeline.Pipeline.run (which has no
    # wait_until_finish parameter in gfw-common 0.4.2) so we can release
    # the lock between submission and waiting. Reaches into a few private
    # attributes (_pre_hooks, _post_hooks); acceptable for a manual
    # integration script.
    with _DATAFLOW_SUBMIT_LOCK:
        for hook in pipeline._pre_hooks:
            hook(pipeline)
        pipeline.apply_dag()
        result = pipeline.pipeline.run()  # Beam's submit -- returns on submission, doesn't wait.

    # Wait outside the lock so concurrent submissions wait in parallel.
    result.wait_until_finish()

    if result.state == PipelineState.DONE:
        for hook in pipeline._post_hooks:
            hook(pipeline)
    else:
        logger.warning(
            "Dataflow pipeline did not finish successfully (state=%s); skipping post-hooks.",
            result.state,
        )


_RUNNERS = {
    "local": _run_local,
    "docker": _run_docker,
    "dataflow": _run_dataflow,
}


def run_pipeline(runner: str, cfg: SimpleNamespace) -> None:
    """Dispatch to the chosen runner."""
    logger.info(
        "[%s] start=%s end=%s out=%s",
        runner, cfg.date_range[0], cfg.date_range[1], cfg.bq_output_gaps,
    )
    _RUNNERS[runner](cfg)


# --------------------------------------------------------------------------
# Three pipeline patterns
# --------------------------------------------------------------------------


def _daterange_inclusive(start: date, end: date):
    """Yield each calendar day d with start <= d < end."""
    cur = start
    while cur < end:
        yield cur
        cur += timedelta(days=1)


def execute_bf(runner: str, *, base_cfg: dict, start: date, end: date, output: str) -> None:
    """Pipeline 1: a single full-range backfill."""
    cfg = _make_config(start=start, end=end, bq_output_gaps=output, **base_cfg)
    run_pipeline(runner, cfg)


def execute_bfd(
    runner: str,
    *,
    base_cfg: dict,
    start: date,
    end: date,
    tail_days: int,
    backfill_days_w: int,
    output: str,
) -> None:
    """Pipeline 2: partial backfill, then daily loads filling in the tail."""
    mid = end - timedelta(days=tail_days)
    cfg = _make_config(start=start, end=mid, bq_output_gaps=output, **base_cfg)
    run_pipeline(runner, cfg)

    for day_end in _daterange_inclusive(mid + timedelta(days=1), end + timedelta(days=1)):
        day_start = day_end - timedelta(days=backfill_days_w)
        cfg = _make_config(start=day_start, end=day_end, bq_output_gaps=output, **base_cfg)
        run_pipeline(runner, cfg)


def execute_bftruncate(
    runner: str,
    *,
    base_cfg: dict,
    start: date,
    end: date,
    tail_days: int,
    backfill_days_w: int,
    output: str,
) -> None:
    """Pipeline 3: full backfill, then re-run the tail days as daily loads."""
    cfg = _make_config(start=start, end=end, bq_output_gaps=output, **base_cfg)
    run_pipeline(runner, cfg)

    tail_start = end - timedelta(days=tail_days)
    for day_end in _daterange_inclusive(tail_start + timedelta(days=1), end + timedelta(days=1)):
        day_start = day_end - timedelta(days=backfill_days_w)
        cfg = _make_config(start=day_start, end=day_end, bq_output_gaps=output, **base_cfg)
        run_pipeline(runner, cfg)


# Threshold above which pipeline 4 step 2 stops inlining `restricted_ssvids`
# as a literal ``ssvid IN (...)`` clause in the messages query and instead
# materialises a temp restricted-messages table (see
# ``materialise_restricted_messages_table``).
#
# Rationale: BQ enforces a 1MB hard limit on standard-SQL query length
# (including comments and whitespace). Production VMS uses 64-char
# SHA-256-style hashes for ssvids; wrapped in quotes and separated by
# ``, `` each contributes ~70 chars, so 5000 ssvids is already ~350KB of
# raw IN-list -- well below the limit on its own, but combined with the
# rest of the query template (SELECT fields, segment-filter subquery, BQ's
# wrapper for ReadFromBigQuery, etc.) the headroom shrinks fast. 5000 leaves
# comfortable margin while staying well above any realistic AIS run
# (handfuls to low hundreds of ssvids).
RESTRICTED_SSVIDS_INLINE_THRESHOLD = 5000


def materialise_restricted_messages_table(
    client,
    *,
    source_messages: str,
    ssvids: tuple[str, ...],
    suffix: str,
    temp_dataset: str,
    start_date: date,
    end_date: date,
) -> str:
    """Materialise a filtered messages table for use in pipeline 4 step 2.

    Workaround for BQ's 1MB query-length cap when ``ssvids`` is very large
    (production VMS: tens of thousands of 64-char hashes). Instead of
    inlining ``ssvid IN ('h1', 'h2', ...)`` into the messages query template,
    we:

    1. Load the ssvid set into a temp BQ table via the load API (avoids the
       1MB cap that ``CREATE TABLE ... AS SELECT FROM UNNEST([...])`` would
       hit).
    2. Materialise ``source_messages`` filtered by an IN-subquery on that
       table plus a ``DATE(timestamp)`` range filter. The IN-subquery
       contains no literals, so the query stays small.

    The date filter is required when ``source_messages`` is the published
    ``pipe_vms_v4_published.messages`` view (or any source whose underlying
    table has ``require_partition_filter=true`` on ``timestamp`` -- which is
    the case for ``gfw-int-vms-v3.pipe_vms_v3_internal.messages_regions``).
    The temp table is partitioned by ``DATE(timestamp)`` so downstream daily-
    tail reads partition-prune efficiently.

    Both tables live in ``temp_dataset`` and are named with the run ``suffix``
    so they are identifiable per run. Cleanup is left to the dataset's
    table-expiration TTL.

    Args:
        client: ``google.cloud.bigquery.Client`` instance.
        source_messages: FQN of the source messages table.
        ssvids: tuple of ssvid strings to restrict to. Must be non-empty.
        suffix: run suffix (commit hash + uuid8), used in the temp table names.
        temp_dataset: ``project.dataset`` to hold the temp tables.
        start_date: inclusive lower bound on ``DATE(timestamp)``.
        end_date: exclusive upper bound on ``DATE(timestamp)``.

    Returns:
        FQN (``project.dataset.table``) of the filtered messages temp table.
    """
    from google.cloud import bigquery

    if not ssvids:
        raise ValueError("materialise_restricted_messages_table requires non-empty ssvids.")

    ssvids_table = f"{temp_dataset}.mode_eq_tmp_{suffix}_restricted_ssvids"
    msgs_table = f"{temp_dataset}.mode_eq_tmp_{suffix}_restricted_msgs"

    # Step 1: load ssvid set via the BQ load API. Using load_table_from_json
    # (newline-delimited JSON via Python iterables) so we don't need pandas
    # as a dependency. WRITE_TRUNCATE makes the helper idempotent across
    # re-runs against the same suffix.
    job_config = bigquery.LoadJobConfig(
        schema=[bigquery.SchemaField("ssvid", "STRING", mode="REQUIRED")],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    rows = [{"ssvid": s} for s in ssvids]
    logger.info(
        "Loading %d restricted ssvids into %s via the BQ load API",
        len(rows), ssvids_table,
    )
    load_job = client.load_table_from_json(rows, ssvids_table, job_config=job_config)
    load_job.result()

    # Step 2: materialise the filtered messages table.
    # - The IN-subquery contains no literals so the query stays below 1MB.
    # - The DATE(timestamp) range satisfies require_partition_filter on the
    #   underlying messages_regions table.
    # - PARTITION BY month (not day) on the output: daily partitioning
    #   over 14 years exceeds BQ's 4000-partitions-per-table cap. Monthly
    #   gives ~12*N_years partitions (well within the cap) and is still
    #   fine for downstream daily-tail reads that filter on a small date
    #   range -- they prune to at most 2 monthly partitions per 4-day window.
    ctas_sql = f"""
        CREATE OR REPLACE TABLE `{msgs_table}`
        PARTITION BY TIMESTAMP_TRUNC(timestamp, MONTH) AS
        SELECT *
        FROM `{source_messages}`
        WHERE ssvid IN (SELECT ssvid FROM `{ssvids_table}`)
          AND DATE(timestamp) >= '{start_date.isoformat()}'
          AND DATE(timestamp) <  '{end_date.isoformat()}'
    """
    logger.info(
        "Materialising filtered messages %s <- %s WHERE ssvid IN <%d ssvids> "
        "AND DATE(timestamp) IN [%s, %s)",
        msgs_table, source_messages, len(rows), start_date, end_date,
    )
    client.query(ctas_sql).result()

    return msgs_table


def execute_mutate_recover(
    runner: str,
    *,
    base_cfg: dict,
    start: date,
    end: date,
    tail_days: int,
    backfill_days_w: int,
    output: str,
    restricted_ssvids: tuple[str, ...],
    suffix: Optional[str] = None,
    temp_dataset: Optional[str] = None,
) -> None:
    """Pipeline 4: simulate reprocessing after a daily run saw partial source data.

    Three steps, intended to expose the open-v1 seed fix:

    1. Range load ``[start, end - tail_days)`` on the global ssvid set.
    2. Daily-tail loop on ``(mid, end]``, restricted to ``restricted_ssvids``
       (the daily run only sees data for these ssvids; the unfiltered DELETE
       still wipes closed v2 rows for *all* ssvids in the daily window).
    3. Daily-tail loop on ``(mid, end]`` on the global ssvid set (the reprocess
       once full data is visible).

    For ssvids in ``global \\ restricted`` whose closed gaps satisfy the
    triggering shape (OFF predates the daily messages window, ON falls in the
    daily DELETE scope, duration > 24h), recovery in step 3 is only possible
    via the surviving open v1 row written in step 1 by the new fix. Without
    the fix, those gaps stay deleted.

    Equivalence is asserted against ``_1_bf`` (the single full-range backfill).

    When ``len(restricted_ssvids) > RESTRICTED_SSVIDS_INLINE_THRESHOLD`` and
    ``suffix`` + ``temp_dataset`` are provided, step 2 reads from a
    materialised filtered messages table instead of inlining the ssvid set
    into the messages query (which would exceed BQ's 1MB query-length cap on
    prod-scale VMS runs). Step 3 uses the global ssvid set so it is unaffected.
    """
    if not restricted_ssvids:
        raise ValueError(
            "execute_mutate_recover requires a non-empty restricted_ssvids tuple."
        )

    mid = end - timedelta(days=tail_days)

    # Step 1: full-range backfill on global ssvids.
    cfg = _make_config(start=start, end=mid, bq_output_gaps=output, **base_cfg)
    run_pipeline(runner, cfg)

    # Step 2: daily-tail loop, restricted ssvids.
    #
    # If the restricted set is large enough that inlining would risk
    # blowing past BQ's 1MB query cap, materialise a filtered messages
    # temp table and route step 2 at it (with ssvids=() so no IN clause
    # is emitted by the template). Otherwise keep the inline path -- it's
    # cheaper and is what the AIS-sized tests exercise.
    if (
        len(restricted_ssvids) > RESTRICTED_SSVIDS_INLINE_THRESHOLD
        and suffix is not None
        and temp_dataset is not None
    ):
        from google.cloud import bigquery
        client = bigquery.Client(project=PROJECT)
        # Pipeline 4 step 2 reads messages within ``[mid - W, end)`` across
        # its daily-tail iterations (each iter scans ``[d - W, d)``). The
        # temp table only needs to cover that union, plus the n_hours_before
        # backwards pad the messages query applies. Use ``[start, end)`` for
        # simplicity -- it strictly contains the iteration union and is what
        # the pipeline 1 backfill already covered.
        filtered_msgs_table = materialise_restricted_messages_table(
            client,
            source_messages=base_cfg["bq_input_messages"],
            ssvids=restricted_ssvids,
            suffix=suffix,
            temp_dataset=temp_dataset,
            start_date=start,
            end_date=end,
        )
        logger.info(
            "Pipeline 4 step 2: routing through filtered messages table %s "
            "(restricted_ssvids=%d > threshold=%d)",
            filtered_msgs_table, len(restricted_ssvids),
            RESTRICTED_SSVIDS_INLINE_THRESHOLD,
        )
        restricted_cfg = {
            **base_cfg,
            "bq_input_messages": filtered_msgs_table,
            "ssvids": (),
        }
    else:
        restricted_cfg = {**base_cfg, "ssvids": restricted_ssvids}

    for day_end in _daterange_inclusive(mid + timedelta(days=1), end + timedelta(days=1)):
        day_start = day_end - timedelta(days=backfill_days_w)
        cfg = _make_config(
            start=day_start, end=day_end, bq_output_gaps=output, **restricted_cfg,
        )
        run_pipeline(runner, cfg)

    # Step 3: daily-tail loop, global ssvids (the reprocess). Uses the
    # original ``base_cfg`` -- no restricted set, no inline-IN issue.
    for day_end in _daterange_inclusive(mid + timedelta(days=1), end + timedelta(days=1)):
        day_start = day_end - timedelta(days=backfill_days_w)
        cfg = _make_config(
            start=day_start, end=day_end, bq_output_gaps=output, **base_cfg,
        )
        run_pipeline(runner, cfg)


def compute_restricted_ssvids(
    bf_table: str,
    *,
    mid: date,
    backfill_days_w: int,
    n_hours_before: int,
    seed: int = 42,
    project: str = PROJECT,
) -> tuple[str, ...]:
    """Pick a ~half-size sample of ssvids whose data was 'visible' during step 2.

    The complement (ssvids excluded from the sample) is guaranteed to contain
    every ssvid with a *triggering* closed gap in ``bf_table_last_versions`` --
    a gap that:

    * Has ``DATE(start_timestamp) < mid - W`` -- OFF predates step 3's
      messages window, so the rewrite path can't reconstruct from raw.
    * Has ``DATE(end_timestamp) >= mid - W`` AND ``< mid`` -- ON falls in
      step 2's daily DELETE scope (under the bugged predicate), so closed
      v2 gets wiped.
    * Has ``duration_h > 24`` -- so the new fix's open v1 seed actually
      gets emitted (the seed condition requires at least one full day
      between OFF and ON).

    Pre-condition: ``{bf_table}_last_versions`` already exists.

    Returns:
        A tuple of ssvid strings sized ``|G| / 2`` (rounded down), drawn at
        random from the non-triggering subset. Reproducible via ``seed``.
    """
    from google.cloud import bigquery

    client = bigquery.Client(project=project)

    # n_hours_before pads the messages window backwards. Apply the same
    # padding to the OFF-cutoff so we don't classify a gap as "triggering"
    # when its OFF actually sits inside step 3's messages window.
    off_cutoff = mid - timedelta(days=backfill_days_w + 1)  # +1d safety margin

    sql = f"""
        WITH all_ssvids AS (
            SELECT DISTINCT ssvid FROM `{bf_table}_last_versions`
        ),
        triggering AS (
            SELECT DISTINCT ssvid
            FROM `{bf_table}_last_versions`
            WHERE is_closed = TRUE
              AND DATE(start_timestamp) < DATE('{off_cutoff.isoformat()}')
              AND DATE(end_timestamp) >= DATE('{(mid - timedelta(days=backfill_days_w)).isoformat()}')
              AND DATE(end_timestamp) < DATE('{mid.isoformat()}')
              AND duration_h > 24
        )
        SELECT
          a.ssvid,
          t.ssvid IS NOT NULL AS is_triggering
        FROM all_ssvids a
        LEFT JOIN triggering t USING (ssvid)
    """

    logger.info("Querying %s for triggering ssvids (n_hours_before=%d unused)",
                bf_table, n_hours_before)
    rows = list(client.query(sql).result())
    triggering = [r["ssvid"] for r in rows if r["is_triggering"]]
    non_triggering = [r["ssvid"] for r in rows if not r["is_triggering"]]
    target_size = len(rows) // 2

    rng = random.Random(seed)
    rng.shuffle(non_triggering)

    if len(non_triggering) >= target_size:
        restricted = non_triggering[:target_size]
        triggering_in_complement = len(triggering)
    else:
        # Edge case: more than half the ssvids are triggering. Keep the
        # restricted set strictly non-triggering (smaller than half) so the
        # complement still contains all of T -- the test signal matters more
        # than hitting exactly 50%.
        restricted = non_triggering
        triggering_in_complement = len(triggering)

    logger.info(
        "Restricted ssvids: %d / %d total (%d triggering); "
        "complement size %d, contains %d triggering ssvids",
        len(restricted), len(rows), len(triggering),
        len(rows) - len(restricted), triggering_in_complement,
    )

    return tuple(restricted)


# --------------------------------------------------------------------------
# Comparison via table-check
# --------------------------------------------------------------------------


def compare_tables(table_a: str, table_b: str) -> int:
    """Run ``table-check summary`` between two output tables.

    Compares the ``..._last_versions`` views (SCD-2 deduplicated) so version
    drift between runs does not show up as a difference. Returns the
    table-check exit code.
    """
    view_a = f"{table_a}_last_versions"
    view_b = f"{table_b}_last_versions"
    cmd = [
        "table-check", "summary",
        "--table-a", view_a,
        "--table-b", view_b,
        "--keys", "gap_id,start_timestamp",
        "--format", "table",
    ]
    logger.info("running: %s", " ".join(cmd))
    result = subprocess.run(cmd, check=False)
    return result.returncode


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n", 1)[0])
    p.add_argument("--runner", choices=list(_RUNNERS), default="local")
    p.add_argument("--source-dataset", default=DEFAULT_SOURCE_DATASET,
                   help=f"BQ dataset for messages and segs_activity (default: {DEFAULT_SOURCE_DATASET})")
    p.add_argument("--source-messages", default=None,
                   help="Override messages table (default: <source-dataset>.messages)")
    p.add_argument("--source-segments", default=None,
                   help="Override segs_activity table (default: <source-dataset>.segs_activity)")
    p.add_argument("--start", default=DEFAULT_START)
    p.add_argument("--end", default=DEFAULT_END)
    p.add_argument("--tail-days", type=int, default=DEFAULT_TAIL_DAYS,
                   help="Number of days at the end of the range to process via daily loads")
    p.add_argument("--backfill-days", type=int, default=DEFAULT_BACKFILL_DAYS_W,
                   help="Backfill width W for each daily load (date_range = [d - W, d))")
    p.add_argument("--ssvids", default="",
                   help="Comma-separated ssvids to filter to (default: no filter)")
    p.add_argument("--min-gap-length", type=float, default=DEFAULT_MIN_GAP_LENGTH)
    p.add_argument("--n-hours-before", type=int, default=DEFAULT_N_HOURS_BEFORE)
    p.add_argument("--window-period-d", type=int, default=DEFAULT_WINDOW_PERIOD_D)
    p.add_argument("--filter-good-seg", default=str(DEFAULT_FILTER_GOOD_SEG),
                   choices=["True", "False"])
    p.add_argument("--skip-open-gaps", action="store_true",
                   help="Pass through to the pipeline (skips reading open precursors)")
    p.add_argument("--suffix", default=None,
                   help="Output-table suffix (default: <commit-hash>-<uuid6>). Use to re-compare an existing run.")
    p.add_argument("--allow-dirty-tree", action="store_true",
                   help="Allow running with uncommitted changes. The suffix will include 'dirty' "
                        "to flag that the embedded commit hash does not actually identify the code "
                        "that ran. Default: refuse to run with a dirty tree.")
    p.add_argument("--skip-pipelines", action="store_true",
                   help="Skip the pipeline runs and only run the comparisons (use with --suffix)")
    p.add_argument("--only-pipeline-4", action="store_true",
                   help="Skip pipelines 1-3 and run only pipeline 4 against an existing run's "
                        "_1_bf table (use with --suffix and --enable-pipeline-4). Useful for "
                        "resuming a run where the parallel phase completed but pipeline 4 failed.")
    p.add_argument("--skip-comparisons", action="store_true",
                   help="Skip the table-check comparisons; just run the pipelines.")
    p.add_argument("--parallel", "--async", dest="parallel", action="store_true",
                   help="Run the three top-level pipelines in parallel (threads). "
                        "Recommended only with --runner=docker or --runner=dataflow.")
    p.add_argument("--service-account", default=DEFAULT_DATAFLOW_SA,
                   help=f"Service account for Dataflow worker VMs (default: {DEFAULT_DATAFLOW_SA}). "
                        "Ignored by the local/docker runners.")
    p.add_argument("--bq-temp-dataset", default=DEFAULT_BQ_TEMP_DATASET,
                   help=f"Existing BQ dataset (project.dataset) to reuse as ReadFromBigQuery's "
                        f"temp_dataset (default: {DEFAULT_BQ_TEMP_DATASET}). Avoids needing "
                        "bigquery.datasets.create at project level. Pass empty string to use "
                        "Beam's default (auto-create throwaway dataset). Ignored by the "
                        "local/docker runners.")
    p.add_argument("--dataflow-region", default=DEFAULT_DATAFLOW_REGION,
                   help=f"Dataflow region (default: {DEFAULT_DATAFLOW_REGION}). Overrides "
                        "gfw-common's hardcoded us-east1 default. Ignored by local/docker.")
    p.add_argument("--dataflow-temp-bucket", default=DEFAULT_DATAFLOW_TEMP_BUCKET,
                   help=f"GCS bucket for Dataflow temp_location and staging_location (default: "
                        f"{DEFAULT_DATAFLOW_TEMP_BUCKET}). Should be in the same region as "
                        "--dataflow-region. Ignored by local/docker.")
    p.add_argument("--dataflow-subnetwork", default=DEFAULT_DATAFLOW_SUBNETWORK,
                   help=f"Subnetwork (full path) for Dataflow workers (default: "
                        f"{DEFAULT_DATAFLOW_SUBNETWORK}). Must exist in --dataflow-region. "
                        "Ignored by local/docker.")
    p.add_argument("--enable-pipeline-4", action="store_true",
                   help="Run the 4th pipeline (mutate-recover): step 1 backfill on global "
                        "ssvids, step 2 daily-tail with --restricted-ssvids, step 3 daily-tail "
                        "on global ssvids. Compares against _1_bf. Tests the open-v1 seed fix.")
    p.add_argument("--restricted-ssvids", default="",
                   help="Comma-separated ssvids used in pipeline 4 step 2 (the 'data became "
                        "visible later' subset). Mutually exclusive with --auto-restrict.")
    p.add_argument("--auto-restrict", action="store_true",
                   help="For pipeline 4: query the run's _1_bf table to find ssvids with "
                        "triggering closed gaps, then pick ~half of all ssvids as restricted, "
                        "ensuring the complement contains every triggering ssvid. Mutually "
                        "exclusive with --restricted-ssvids. Forces pipeline 4 to run "
                        "sequentially after pipeline 1.")
    p.add_argument("--auto-restrict-seed", type=int, default=42,
                   help="Random seed for --auto-restrict sampling (default: 42).")
    p.add_argument("--max-num-workers", type=int, default=None,
                   help="Cap Dataflow autoscaling at this many workers per job. "
                        "Ignored by local/docker. When unset, falls back to gfw-common's "
                        "default (100).")
    p.add_argument("--resume", action="store_true",
                   help="Skip any pipeline unit (single Dataflow job, identified by its "
                        "canonical job name) that already completed successfully in a prior "
                        "run with the same --suffix. Only takes effect with --runner=dataflow. "
                        "Pairs well with --source-snapshot-ts so that re-runs see the same "
                        "source state -- otherwise iters from the original and resumed runs "
                        "may disagree on data that changed between them.")
    p.add_argument("--source-snapshot-ts", default=None,
                   help="Pin the source messages/segments reads to a BigQuery time-travel "
                        "snapshot via FOR SYSTEM_TIME AS OF TIMESTAMP(<ts>). Eliminates "
                        "snapshot-timing drift when running against continuously-updated "
                        "sources (e.g. research_messages). BQ time travel supports up to 7 "
                        "days back. Special value 'today-utc-midnight' (or omitting; this is "
                        "the default) pins to 00:00:00 UTC of the current day. Pass 'none' to "
                        "disable. Otherwise must be a BQ-parseable timestamp literal, e.g. "
                        "'2026-05-13 00:00:00 UTC'.")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    repo_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    suffix = _resolve_suffix(args, repo_dir)
    logger.info("Run suffix: %s", suffix)

    # If --resume is set, fetch the set of already-completed Dataflow units
    # for this suffix once. _run_dataflow then short-circuits any unit whose
    # canonical job name appears here. Empty set on a fresh run -> no-op.
    if args.resume:
        if args.runner != "dataflow":
            raise SystemExit("--resume is only supported with --runner=dataflow")
        region = args.dataflow_region or DEFAULT_DATAFLOW_REGION
        _COMPLETED_UNITS.update(_list_completed_units(suffix, region=region))
        logger.info(
            "Resume mode active: %d completed Dataflow units found for suffix %s",
            len(_COMPLETED_UNITS), suffix,
        )

    source_messages = args.source_messages or f"{args.source_dataset}.messages"
    source_segments = args.source_segments or f"{args.source_dataset}.segs_activity"

    # Resolve --source-snapshot-ts. Default + sentinel "today-utc-midnight"
    # both expand to today's UTC midnight as a BQ-parseable timestamp literal.
    # "none" / empty string disables time-travel.
    raw_snapshot = (args.source_snapshot_ts or "today-utc-midnight").strip().lower()
    if raw_snapshot in ("none", ""):
        as_of_timestamp = None
    elif raw_snapshot == "today-utc-midnight":
        midnight = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0,
        )
        as_of_timestamp = midnight.strftime("%Y-%m-%d %H:%M:%S UTC")
    else:
        # User supplied a literal; pass through verbatim.
        as_of_timestamp = args.source_snapshot_ts
    if as_of_timestamp is not None:
        logger.info("Pinning source reads to FOR SYSTEM_TIME AS OF TIMESTAMP('%s')",
                    as_of_timestamp)

    base_cfg = dict(
        bq_input_messages=source_messages,
        bq_input_segments=source_segments,
        as_of_timestamp=as_of_timestamp,
        ssvids=tuple(s.strip() for s in args.ssvids.split(",") if s.strip()),
        min_gap_length=args.min_gap_length,
        n_hours_before=args.n_hours_before,
        window_period_d=args.window_period_d,
        filter_good_seg=(args.filter_good_seg == "True"),
        skip_open_gaps=args.skip_open_gaps,
        service_account=args.service_account,
        bq_temp_dataset=args.bq_temp_dataset or None,
        dataflow_region=args.dataflow_region or None,
        dataflow_temp_bucket=args.dataflow_temp_bucket or None,
        dataflow_subnetwork=args.dataflow_subnetwork or None,
        max_num_workers=args.max_num_workers,
    )

    base = f"{PROJECT}.{DEST_DATASET}.three_way_{suffix}"
    bf_table = f"{base}_1_bf"
    bfd_table = f"{base}_2_bfd"
    bft_table = f"{base}_3_bftruncate"
    mr_table = f"{base}_4_mutate_recover"

    logger.info("output tables:")
    logger.info("  %s", bf_table)
    logger.info("  %s", bfd_table)
    logger.info("  %s", bft_table)
    if args.enable_pipeline_4:
        logger.info("  %s", mr_table)

    # Validate pipeline-4-specific args.
    explicit_restricted = tuple(s.strip() for s in args.restricted_ssvids.split(",") if s.strip())
    if args.enable_pipeline_4:
        if explicit_restricted and args.auto_restrict:
            raise SystemExit(
                "--restricted-ssvids and --auto-restrict are mutually exclusive."
            )
        if not explicit_restricted and not args.auto_restrict:
            raise SystemExit(
                "--enable-pipeline-4 requires either --restricted-ssvids or --auto-restrict."
            )

    if not args.skip_pipelines:
        # Pre-build the docker image once before launching parallel work,
        # so the build only runs once even in --parallel mode.
        if args.runner == "docker" and not args.only_pipeline_4:
            _ensure_docker_built()

        bf_kwargs = dict(
            runner=args.runner, base_cfg=base_cfg, start=start, end=end, output=bf_table,
        )
        bfd_kwargs = dict(
            runner=args.runner, base_cfg=base_cfg, start=start, end=end,
            tail_days=args.tail_days, backfill_days_w=args.backfill_days,
            output=bfd_table,
        )
        bft_kwargs = dict(
            runner=args.runner, base_cfg=base_cfg, start=start, end=end,
            tail_days=args.tail_days, backfill_days_w=args.backfill_days,
            output=bft_table,
        )

        # Determine pipeline 4's restricted ssvids ahead of time (or None if
        # we'll auto-pick after pipeline 1 completes).
        mr_restricted: Optional[tuple[str, ...]] = (
            explicit_restricted if (args.enable_pipeline_4 and explicit_restricted) else None
        )

        if args.only_pipeline_4:
            # --only-pipeline-4: skip pipelines 1-3 entirely. _1_bf is
            # expected to exist already from a prior run (used by
            # --auto-restrict below).
            logger.info(
                "Skipping pipelines 1-3 (--only-pipeline-4); will run pipeline 4 "
                "against existing _1_bf at %s", bf_table,
            )
        elif args.parallel:
            if args.runner == "local":
                logger.warning(
                    "--parallel with --runner=local runs three Beam DirectRunner "
                    "pipelines in the same process simultaneously. "
                    "This may not be safe; prefer --runner=docker or --runner=dataflow.",
                )

            # Worker count: 3 base pipelines + 1 if pipeline 4 enabled with
            # explicit restricted ssvids. With --auto-restrict, pipeline 4 runs
            # serially after pipeline 1 finishes.
            can_parallel_p4 = args.enable_pipeline_4 and mr_restricted is not None
            max_workers = 4 if can_parallel_p4 else 3

            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futures = [
                    ex.submit(execute_bf, **bf_kwargs),
                    ex.submit(execute_bfd, **bfd_kwargs),
                    ex.submit(execute_bftruncate, **bft_kwargs),
                ]
                if can_parallel_p4:
                    futures.append(ex.submit(
                        execute_mutate_recover,
                        runner=args.runner, base_cfg=base_cfg,
                        start=start, end=end,
                        tail_days=args.tail_days, backfill_days_w=args.backfill_days,
                        output=mr_table,
                        restricted_ssvids=mr_restricted,
                        suffix=suffix,
                        temp_dataset=args.bq_temp_dataset or None,
                    ))
                for f in futures:
                    f.result()  # propagate exceptions
        else:
            execute_bf(**bf_kwargs)
            execute_bfd(**bfd_kwargs)
            execute_bftruncate(**bft_kwargs)

        # Pipeline 4 with --auto-restrict has to run after _1_bf is populated.
        if args.enable_pipeline_4 and args.auto_restrict:
            mid = end - timedelta(days=args.tail_days)
            mr_restricted = compute_restricted_ssvids(
                bf_table,
                mid=mid,
                backfill_days_w=args.backfill_days,
                n_hours_before=args.n_hours_before,
                seed=args.auto_restrict_seed,
            )
            execute_mutate_recover(
                runner=args.runner, base_cfg=base_cfg,
                start=start, end=end,
                tail_days=args.tail_days, backfill_days_w=args.backfill_days,
                output=mr_table,
                restricted_ssvids=mr_restricted,
                suffix=suffix,
                temp_dataset=args.bq_temp_dataset or None,
            )
        elif args.enable_pipeline_4 and not args.parallel:
            # Sequential mode with explicit restricted ssvids: run pipeline 4
            # at the end (could be before/after others; doesn't matter -- they
            # all write to disjoint tables).
            execute_mutate_recover(
                runner=args.runner, base_cfg=base_cfg,
                start=start, end=end,
                tail_days=args.tail_days, backfill_days_w=args.backfill_days,
                output=mr_table,
                restricted_ssvids=mr_restricted,
                suffix=suffix,
                temp_dataset=args.bq_temp_dataset or None,
            )

    if args.skip_comparisons:
        return 0

    pairs = [
        ("1_bf vs 2_bfd",        bf_table, bfd_table),
        ("1_bf vs 3_bftruncate", bf_table, bft_table),
        ("2_bfd vs 3_bftruncate", bfd_table, bft_table),
    ]
    if args.enable_pipeline_4:
        pairs.append(("1_bf vs 4_mutate_recover", bf_table, mr_table))
    rcs = []
    for label, a, b in pairs:
        logger.info("=" * 80)
        logger.info("comparison: %s", label)
        logger.info("=" * 80)
        rcs.append(compare_tables(a, b))

    n_failed = sum(1 for rc in rcs if rc != 0)
    if n_failed:
        logger.error("%d/%d comparisons reported differences", n_failed, len(rcs))
        return 1
    logger.info("all %d comparisons passed", len(rcs))
    return 0


if __name__ == "__main__":
    sys.exit(main())
