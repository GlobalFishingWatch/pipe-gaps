# Proposal: separate open-gaps table, no versioning

Status: **proposal / untested**. Written 2026-10-14; updated 2026-10-15 with the
clustering-aware query rewrite (PIPELINE-4670). A smaller change to the current v5
raw-gaps design than the full redesign in
[design-stateless-gaps.md](design-stateless-gaps.md); it can be done first, and either
alone or before the redesign.

## Problem

Every raw-gaps run reads the currently open gaps as a side input. The query
(`assets/queries/gaps.sql.j2`) runs
`QUALIFY ROW_NUMBER() OVER (PARTITION BY gap_id, start_timestamp ORDER BY version DESC)`
over the **whole** `raw_gaps_versioned` table before filtering `NOT is_closed`, so it
scans all of history on every run.

### Measured (2026-10-14)

From `world-fishing-827` `region-us.INFORMATION_SCHEMA.JOBS`, service account
`executor@gfw-int-ais-v5`, last 45 days. Jobs classified by query text.

| Normal daily run | GB billed | Slot-hours |
|---|---|---|
| Messages read (D-1, D) | ~51 | ~1.5–4 |
| **Open-gaps side input** | **~336** (grows ~0.1 GB/day) | ~6–10 |
| Total | **~387** | ~8–14 |

Reruns re-scan it once per day: ~15 TB of side-input reads on 2026-09-11/12 (45 runs)
and 15.4 TB on 2026-10-08 (46 runs). The ~60-day rerun after the 2026 backfill bucket
costs ~23 TB.

`raw_gaps_versioned`: 1.01B rows, 332 GB. 698M closed versions, 315M open versions
(almost all superseded). Gaps currently open: **3.58M** (~1.2 GB, estimated from the
average row size). Narrow columns (`gap_id`, `start_timestamp`, `version`,
`is_closed`): 52 GB.

### Rewriting the query helps, but only ~3x

Updated 2026-10-15 (PIPELINE-4670). The table is partitioned by month on
`start_timestamp` and clustered by `(is_closed, version, ssvid)`.

**Why the current query can't prune.** Two reasons:

1. The `is_closed` filter runs after `QUALIFY ROW_NUMBER()` over the whole table.
2. BigQuery prunes clustered blocks only for comparisons like `is_closed = FALSE`.
   `NOT is_closed` prunes nothing. Measured on one column (`start_seg_id`, 42.3 GB):
   `NOT is_closed` 43.3 GB, `is_closed = FALSE` 14.9 GB.

An earlier rewrite attempt (latest version with narrow columns, then join back) billed
331.7 GB because it used `NOT is_closed`, not because clustering doesn't work.

**`version` can't tell which gaps are still open.** It is a message timestamp, not a
run time: the OFF timestamp for an open row, the ON timestamp for a closed row
(`core/gap_detector.py`). A gap gets one open row when it opens, never re-stamped;
closing appends a closed row and the open row stays. So the 322M open rows hold ~3.7M
gaps still open and ~318M stale rows of gaps that closed later, with nothing on the row
to tell them apart: only the existence of a closed row with the same `gap_id` does.

**Rewrite that uses the clustering.** Read the open blocks fully and only two narrow
columns of the closed blocks:

```sql
WITH
  open_rows AS (
    SELECT * FROM raw_gaps_versioned
    WHERE is_closed = FALSE AND DATE(start_timestamp) >= @start_date
  ),
  closed_ids AS (
    SELECT DISTINCT gap_id, start_timestamp FROM raw_gaps_versioned
    WHERE is_closed = TRUE AND DATE(start_timestamp) >= @start_date
  )
SELECT <fields>
FROM open_rows LEFT JOIN closed_ids USING (gap_id, start_timestamp)
WHERE closed_ids.gap_id IS NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY gap_id, start_timestamp ORDER BY version DESC) = 1
```

Equivalent to the current logic: a closed row's `version` (ON) is always later than its
open row's (OFF), so if a closed row exists it is the latest version. The final
`QUALIFY` only dedups duplicate open versions (see below).

| Query (measured 2026-10-15) | GB billed | Result |
|---|---|---|
| Current | ~338 | 3,679,174 open gaps |
| Rewrite with `NOT is_closed` | 338 | same |
| Rewrite with `is_closed = FALSE` / `= TRUE` | **116** (~2 slot-hours) | same 3,679,174; 0 differences either way |

It can't go much lower with this table: it still reads all columns of the ~322M open
rows and `gap_id`/`start_timestamp` of all closed rows. A separate open-gaps table reads
~1.2 GB. The rewrite is a small, low-risk first step (one template, same tables, same
output); this proposal is the larger one.

### Data issue found

The same query returned 3,583,671 rows vs 3,577,429 open gaps: ~6k open versions share
the same `(gap_id, start_timestamp, version)`. The current `ROW_NUMBER` silently picks
one. Worth a separate ticket.

## Why versioning exists today

A gap's row changes exactly once: from open to closed. Beam can't cheaply update
BigQuery rows, so v5 appends a new version and the `raw_gaps` view picks the latest
(SCD2). The delete pre-hook exists so reruns don't leave stale versions. Version
history came along as a side effect; nothing downstream is known to use it
(gap_events is a full load of the current state).

If open and closed gaps live in different tables, no row ever changes, so versioning
is no longer needed.

## Proposal

| Table | Content | Written |
|---|---|---|
| `gaps_closed` | Closed gaps, final | Append only. Each gap written once, when it closes. |
| `open_gaps` | Gaps open as of the last processed date | Replaced each run (~3.6M rows, ~1.2 GB) |
| `raw_gaps` (view) | `gaps_closed UNION ALL open_gaps` | No `QUALIFY`, no versions |

Closing a gap = appending its closed row to `gaps_closed`; the next `open_gaps` simply
doesn't contain it.

### Open state for a run starting at S

The run needs the gaps that were open at S. It can be rebuilt from the data, without
versions:

```sql
-- Open state as of S: started before S, and still open, or closed on/after S.
SELECT
  gap_id, ssvid,
  start_*,                                    -- all OFF-message fields
  positions_hours_before, positions_hours_before_sat,
  positions_hours_before_ter, positions_hours_before_dyn,
  FALSE AS is_closed
  -- end_*, duration_h, distance_m, implied_speed_knots: NULL
FROM `{{ gaps_closed }}`
WHERE start_timestamp < '{{ S }}' AND end_timestamp >= '{{ S }}'

UNION ALL

SELECT * FROM `{{ open_gaps }}`
WHERE start_timestamp < '{{ S }}'
```

A closed row carries the same `start_*` fields and lookback counts as its open version
(both come from the OFF message), so the open version can be rebuilt from it.

**More robust than today.** A rerun from S currently depends on the old open versions
still being in the versioned table: the pre-hook deletes the closed versions with
`end >= S`, and the latest remaining version is the old open one. That is what broke in
PIPELINE-4652: the 2026 bucket never wrote open rows, so there was nothing to fall back
to. Rebuilding from closed rows removes that dependency.

The same query serves daily runs (S = yesterday) and reprocesses (S in the past), so
there is a single code path. It is cheap only if `gaps_closed` is partitioned by
**end** month (see [Partitioning](#partitioning)).

### Order of operations (important)

The rebuild reads closed gaps with `end >= S`, which are exactly what the delete
pre-hook removes. So the state must be captured **before** deleting:

1. Pre-hook 1: materialize the open state at S into a staging table
   `open_gaps_staging` (query above).
2. Pre-hook 2: delete from `gaps_closed` where `end_timestamp >= S`, and from
   `open_gaps` where `start_timestamp >= S` (same logic as today's `GapsDeleteQuery`).
3. Pipeline: read `open_gaps_staging` as the side input (instead of the versioned
   table); detect gaps for [S, E).
4. Sinks:
   * closed gaps -> append to `gaps_closed`;
   * new open set -> replace `open_gaps`:
     `(staging ∪ run output) -> latest per gap_id -> keep open`.

Today the pre-hook runs before the side input is read; with this proposal that order
would lose data, so it must change.

After a run over [S, E), the tables represent "processed up to E": `gaps_closed` has
nothing ending at or after E (deleted, not yet re-added) and `open_gaps` is the state
at E. Consistent for backfill buckets and for daily runs.

### Code sketch (Beam)

```python
# Same "latest version per gap, keep if open" logic as gaps.sql.j2,
# but over ~3.6M rows instead of ~1B.
open_set = (
    (open_gaps_side_input, run_output_gaps)
    | "UnionOpenAndRun" >> beam.Flatten()
    | "GroupByGapId" >> beam.GroupBy(lambda g: g["gap_id"])
    | "LatestVersion" >> beam.Map(lambda kv: max(kv[1], key=lambda g: g["version"]))
    | "KeepOpen" >> beam.Filter(lambda g: not g["is_closed"])
)

closed = run_output_gaps | "KeepClosed" >> beam.Filter(lambda g: g["is_closed"])

closed | WriteToBigQuery(gaps_closed, write_disposition="WRITE_APPEND")
open_set | WriteToBigQuery(open_gaps, write_disposition="WRITE_TRUNCATE")
```

`WRITE_TRUNCATE` on `open_gaps` from a Beam job is atomic per load job; if the write
is split across several load jobs, write to a temp table and copy instead.

### Partitioning

* `gaps_closed`: partitioned by **end** month, clustered by `start_timestamp`,
  `ssvid`. The rebuild (`end >= S`) and the delete pre-hook (`end >= S`) prune to
  partitions from S onward. For a daily run that's the current month: ~8–10M closed
  gaps, ~3 GB (estimated). Queries filtering by start date prune by clustering only;
  gap_events is a full load and is unaffected. Monthly, not daily, because of the
  4,000-partition limit (2012 -> today is ~5,400 days).
* `open_gaps`: small; no partitioning needed (or by start month for consistency).

## Cost (estimated)

| | Current v5 | Query rewrite (PIPELINE-4670) | Proposal |
|---|---|---|---|
| Daily: messages | ~51 GB | ~51 GB | ~51 GB |
| Daily: open state | ~336 GB | 116 GB (measured) | ~3 GB (rebuild, current end-month partition) + ~1.2 GB (`open_gaps`) |
| Daily total | ~387 GB | ~167 GB (~2.3x less) | **~55 GB** (~7x less) |
| ~60-day rerun | ~23 TB | ~10 TB | ~3–4 TB |
| Storage | 332 GB versioned (growing with every version) | same | ~230 GB (one row per gap) |

Proposal figures are estimates from row counts and average row size, not measured. The
rewrite's open-state figure is measured; its totals add the measured messages cost.
Dataflow cost not included (not in `INFORMATION_SCHEMA`). Pricing model (on-demand vs
slots) to be confirmed; on-demand at $6.25/TB, daily goes from ~$2.40 to ~$0.35.

## What this removes

* `raw_gaps_versioned` (SCD2) and the `QUALIFY ROW_NUMBER` last-version view.
* The 315M superseded open rows and the duplicate versions.
* The dependency on old open versions when reprocessing.
* `versioned_suffix` and the version-based logic in `GapsQuery`.

## Trade-offs

* **Version history is lost.** If needed: BigQuery time travel (7 days), a table
  snapshot before each reprocess (zero-copy), or periodic snapshots. Check first that
  nobody queries `raw_gaps_versioned` for history (`INFORMATION_SCHEMA.JOBS`).
* **Still a sequential fold.** A fix in the middle of history still requires rerunning
  up to the present, and a bad run still propagates. Only the redesign fixes that.
* **`version`** loses its role. Keep it as the run time or drop it.

## Migration

1. One-time build from the current versioned table (~332 GB, once):
   * `gaps_closed` = latest versions with `is_closed`;
   * `open_gaps` = latest versions with `NOT is_closed`.
2. Point the `raw_gaps` view at `gaps_closed UNION ALL open_gaps`.
3. Deploy the pipeline change (sinks, side input, pre-hook order).
4. Keep `raw_gaps_versioned` (or a snapshot of it) for a while, then drop it.

## Validation plan

1. **Rebuild query vs current behaviour:** for a few past S, compare the rebuilt open
   state with what the current versioned table gives as of S (gap_ids and start
   fields).
2. **Parallel run:** for a couple of weeks, run the new pipeline to separate tables
   next to production and compare `raw_gaps` row by row (gap_id, start/end,
   is_closed, counts).
3. **Costs:** measure bytes billed for a daily run and a multi-day rerun.

## Open points

1. Duplicate open versions (~6k) in `raw_gaps_versioned`: cause and cleanup.
2. Confirm nobody needs `raw_gaps_versioned` history.
3. Confirm the BigQuery pricing model; measure Dataflow cost.
4. Whether to do this before, instead of, or alongside the redesign.
5. The query rewrite (PIPELINE-4670) is independent and can ship first; this proposal
   would then save ~110 GB/day more.
