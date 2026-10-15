# Design sketch: stateless raw gaps (per-day detection + pairing)

Status: **draft / untested**. Written 2026-10-13, updated 2026-10-14, from a design
discussion after PIPELINE-4652 (the 2026 backfill bucket lost its open gaps and could
only be fixed by rerunning from 2026-01-01 up to today). Nothing here has been run.

## Problem

`raw_gaps` is computed as a sequential fold: each run reads the open gaps written by
the previous run (side input), so a change at day *t* changes the state for every day
after *t*. Consequences:

* Fixing a period in the middle of history requires rerunning from there to today.
* A full reprocess is sequential (yearly buckets chained by `depends_on_past`).
* A bad run propagates forward (the 2026 bucket / `eval_last` incident).
* Every run re-scans all of history for the open-gaps side input (see [Cost](#cost)).

## Idea

Only one thing crosses time: pairing "last message before a silence" with "first
message after it". Everything else is local to a day.

1. **Step 1 (per day, Beam).** For each (ssvid, day D), read messages of D-1, D, D+1.
   * **Ownership rule:** a gap belongs to the day of its OFF message.
   * OFF on D, next message within the window: **complete gap** -> `gaps_daily`.
   * OFF on D, no next message within the window (gap > 24h): **unresolved OFF** ->
     `gap_endpoints`, stored with the `positions_hours_before*` counts already computed.
   * ON on D, no previous message within the window (gap > 24h): **unresolved ON** ->
     `gap_endpoints`, only its own `end_*` fields.
   * Gaps starting on D-1 or D+1 are skipped (owned by those days).
   * Full-day windows on both sides are what make this consistent: the OFF side calls
     a gap resolved iff the next message is in D or D+1, the ON side iff the previous
     message is in D-1 or D, so both sides always agree. A partial window (e.g. 4h
     ahead, 12h back) would let them disagree and duplicate or lose gaps.
   * The skip decision depends only on input **messages**, never on another run's
     **output**. Each day is a pure function of the messages of [D-1, D+1]: days can
     run in any order or in parallel, and a failed day doesn't affect its neighbours.
2. **Step 2 (pairing).** Per ssvid, order unresolved endpoints and `LEAD()`:
   * unresolved OFF followed by unresolved ON = closed long gap;
   * OFF with nothing after = open gap;
   * ON with no OFF before = vessel's first message in the data, dropped.

   An unresolved OFF can only be followed by its ON, since no messages exist between
   them. **Endpoints are not open gaps**: they are half-gaps that only become closed
   or open gaps at pairing time; an open gap closes by itself when its ON arrives.
   Step 2 writes the table `gaps_long` (see [Tables](#tables)).

`raw_gaps` becomes a plain view: `gaps_daily UNION ALL gaps_long`.

### Reprocess rule

Fixing [a, b]:

1. Run step 1 for days [a-1, b+1] (windows reach one day each way). The job reads
   messages for [a-2, b+2].
2. Replace days [a-1, b+1] in **both** `gaps_daily` and `gap_endpoints` (see
   [Atomicity](#atomicity) for how, given monthly partitions).
3. Run step 2.

Nothing after b+1 reruns. Endpoint rows outside [a-1, b+1] never need rewriting: each
depends only on the messages of its own day ±1. What can change for them is their
**pairing**, which step 2 derives. Example: a long gap OFF(x) -> ON(y), x < a-1,
y > b+1, and the reprocess adds an isolated message inside [a, b]. Step 1 writes a new
ON and OFF inside the range; step 2 now pairs OFF(x) -> ON(new) and OFF(new) -> ON(y).
The OFF(x) and ON(y) rows (gap_id, counts) are unchanged.

<a id="atomicity"></a>**Atomicity and partitioning.** BigQuery allows at most 4,000
partitions per table and 2012 -> today is ~5,400 days, so the tables must be
partitioned by **month** (as `raw_gaps_versioned` is) and clustered by `day`. Day-level
partition replacement (`table$YYYYMMDD`) is therefore not available. If days are
deleted and then written as separate steps, a reader in between sees wrong pairings
(e.g. an OFF before the range paired with an ON after it). Options:

* Write step 1 output to a staging table, then `DELETE WHERE day BETWEEN ...` +
  `INSERT ... SELECT` from staging inside one multi-statement **transaction**
  (preferred; works for any range, including a single day).
* Round reprocess ranges to whole months and `WRITE_TRUNCATE` the month partition
  (`table$YYYYMM`). Fine for backfills, too expensive for daily runs.

gap_events already runs after gaps, so in the DAG nobody reads mid-run anyway.

### Changing N (`n_hours_before`) or `min_gap_length`

The counts are computed in step 1, so changing N means rerunning step 1 for all
history. Same as today (where it is a full sequential reprocess), but fully parallel.
Optionally store counts for several N (e.g. 12 and 24) as extra columns.

### Constraints

* `min_gap_length < 24h` (unresolved endpoints are assumed to be gaps).
* `n_hours_before <= 24h` with a one-day lookback. Larger N needs a longer read back
  (`ceil(N / 24)` days, as `messages_query_start_date` does) for the counts only;
  ownership windows stay at D-1 / D+1.

### Daily latency

AIS daily gaps for D already wait for D+1 (`aggregate_segs_stabilization_days=1`,
sensors `wait_for_next_day_segs_activity` and next-day `measures.add_measures` in
composer-dags-production `dags/core/ais/v5.py`), so reading D+1 adds no latency and
results for D are final, not provisional.

**Subtlety:** D's good_seg2 is settled at that point, D+1's is not (it needs D+2). If
the look-ahead filters D+1 by good_seg2, D's job may see a different D+1 than D+1's own
job later. Options: each daily run also rewrites the previous day (preferred; D+1's run
already reads D), or wait 2 days.

## Tables

| Table | Written by | Partitioned by | Content |
|---|---|---|---|
| `gaps_daily` | Step 1 | Start month, clustered by `day` | Gaps resolved within their window |
| `gap_endpoints` | Step 1 | Endpoint month, clustered by `day` | Unresolved OFFs and ONs (internal) |
| `gaps_long` | Step 2 | Start month | Paired long gaps, closed or open |
| `raw_gaps` (view) | — | — | `gaps_daily UNION ALL gaps_long` |

**One writer per table.** Storing long gaps in `gaps_daily` would put two jobs on the
same data: step 1 replacing its days would also have to spare step 2's long gaps, and a
step 2 full rebuild (`CREATE OR REPLACE`) would wipe the daily gaps. Both would need
`DELETE ... WHERE kind = ...` + `INSERT`, and step 2's delete would touch every
partition on every run; concurrent DML on the same table can also conflict (e.g.
during a backfill).
The view already gives consumers a single table and prunes by date in both branches.
If something downstream needs a physical table, add a third step that rebuilds it.

**Why step 2 writes a table instead of being a view.** `LEAD` needs every endpoint of
an ssvid, and the matching ON can be in any later partition, so BigQuery can't prune by
date before the window function: every query on a pairing view (even for one month)
would scan and re-pair all of `gap_endpoints` (~50 GB). Only `ssvid` filters push down.
BigQuery materialized views don't support window functions.

**Step 2 modes:**

* **Full rebuild** (start here): each run recomputes all of `gaps_long` from all of
  `gap_endpoints` (`CREATE OR REPLACE TABLE ... AS SELECT ... LEAD ...`, or a small
  Python job). Reads endpoints only, never messages. One simple query, always correct.
* **Incremental** (only if cost matters): re-pair only ssvids with endpoints in the
  days step 1 just rewrote and `MERGE` them. Reads less, more logic.

A Python pairing job (calling `GapDetector.create_gap`) also keeps the geodesic
distance identical to short gaps (see open point 1).

### Versioned table

Probably unnecessary. SCD2 exists in v5 as a mechanism to "update" open gaps through
append-only writes (an open gap gets closed by a later run), plus the delete pre-hook
for reruns. Here nothing is updated in place: step 1 partitions are replaced
idempotently and closing an open gap is not a write (step 2 just sees the ON). History
came along in v5 as a side effect; nothing downstream is known to use it (gap_events is
a full load of the current state).

Versioning *is* possible if needed: append the changed gaps (rewritten step 1
partitions + re-paired long gaps) with a new `version` (gap ids are stable), keep the
last-version view, and write tombstones (`is_deleted`) for gaps that disappear. Cheaper
options if history is needed: BigQuery time travel (7 days), table snapshots before
each reprocess (zero-copy), or periodic snapshots. **Check first** that nobody queries
`raw_gaps_versioned` for history (`INFORMATION_SCHEMA.JOBS`).

## Comparison (cost and operations; output is the same by design)

| | Prototype | Current v5 | Proposed |
|---|---|---|---|
| Daily message reads | OFF query D..D+1 + ON query D-1..D (~4 day-reads) | D-1, D (2) | D-1..D+1 in one pass (3) |
| Daily pairing | Join of all historical OFF/ON every day, RANK (grows over time) | None, but side input that scans the whole versioned table | Step 2 over unresolved endpoints only (~20% of gaps) |
| Full reprocess | Days parallel, then one pairing | Sequential, `depends_on_past` chain | Parallel ranges, each read once + 2 edge days |
| Mid-history fix | Recompute those days, next pairing picks it up | Rerun from fix to today | Recompute [a-1, b+1] + step 2 |
| A bad run | Local | Propagates forward | Local |
| Moving parts | 3 SQL queries/day + pairing | Beam + open-gap side input + `eval_last` + SCD2 + delete hook + `depends_on_past` | Beam + 3 tables + pairing step + view |
| Change N / threshold | Rerun all, parallel | Full rerun, sequential | Full rerun, parallel |
| Latency | Waits D+1 | Waits D+1 (good_seg2) | Waits D+1 (same) |

The proposal is the prototype's own split (per-day detection + pairing), with: pairing
restricted to unresolved endpoints instead of a growing full-history join, an explicit
ownership rule and reprocess rule (the prototype's pairing query has a WHERE that
"keeps only valid matches", patching mismatched pairs), and the current Python
detector. Prototype description from `pipe-gaps-prototype/assets/ais_off_on_events.sql.j2`
and `ais_gap_events.sql.j2`.

## Cost

### Measured: current v5 (BigQuery only)

From `world-fishing-827` `region-us.INFORMATION_SCHEMA.JOBS`, service account
`executor@gfw-int-ais-v5`, last 45 days as of 2026-10-14. Jobs classified by query
text (the account is shared by all v5 stages, but message reads and side-input reads
pair 1:1 per day).

A normal daily run:

| Query | GB billed | Slot-hours |
|---|---|---|
| Messages read (D-1, D) | ~51 | ~1.5–4 |
| **Open-gaps side input** | **~336** (grows ~0.1 GB/day) | ~6–10 |
| Total | **~387** | ~8–14 |

The side input is 87% of the bytes: `gaps.sql.j2` runs
`QUALIFY ROW_NUMBER() OVER (PARTITION BY gap_id ...)` over the whole
`raw_gaps_versioned` (332 GB, 1.01B rows) **before** filtering `NOT is_closed`, so
nothing is pruned.

Reruns of daily periods re-scan it once per day:

* 2026-09-11/12: 45 runs, ~15 TB of side-input reads.
* 2026-10-08: 46 runs, 2.3 TB messages + 15.4 TB side input.
* The 08-15 -> today rerun after the 2026 bucket (~60 days): ~60 × 387 GB ≈ 23 TB.

### Estimated: proposed

| Query | GB |
|---|---|
| Messages read (D-1, D, D+1) | ~77 (51 × 1.5) |
| Step 2 full rebuild (read `gap_endpoints`) | ~50 |
| Total | **~127** (~3x less than v5) |

A 60-day rerun: one step 1 job (~1.5–3 TB of messages) + one step 2 (~50 GB), ~10x
less than v5, and parallel instead of sequential.

### Caveats

* **Dataflow not included** (not in `INFORMATION_SCHEMA`, no billing export access).
  The proposal removes the side input from worker memory but adds the 3x shuffle
  fan-out; net effect unknown without measuring.
* **Pricing model unknown.** On-demand at $6.25/TB: v5 daily ~$2.40, proposed ~$0.80,
  a 60-day v5 rerun ~$145. On slot reservations, compare slot-hours instead.
* Proposed numbers are estimates, not measurements.

### Cheaper fix inside v5

Most of the **daily** saving doesn't need the redesign: keep open gaps in a small
separate table (~336 GB -> ~4 GB per run, estimated), or rewrite the side-input query to
use the table's clustering (~336 -> 116 GB per run, measured; PIPELINE-4670, see
[proposal-open-gaps-table.md](proposal-open-gaps-table.md)). That does **not** fix rerunning to the present or the sequential
chain. So the redesign's value depends mostly on how often we reprocess (three large
reruns in about a month as of 2026-10-14).

## Sizes

### Input (sampled, `summarized_positions_v1.research_messages`)

The gaps stage input in production (composer-dags-production `dags/core/ais/v5.py`,
`GapsInputs.messages = summarize_positions_config.outputs.research_messages`).

| Day | Messages | Vessels |
|---|---|---|
| 2013-06-15 | 5.4M | 117k |
| 2017-06-15 | 24.3M | 194k |
| 2021-06-15 | 40.2M | 246k |
| 2025-06-15 | 92.4M | 358k |

Messages table: ~200B rows, 47.2 TB. Storing 12h of messages per endpoint instead of
precomputed counts would be ~half the messages table, so counts must be precomputed in
step 1. (An earlier variant stored first/last message for every vessel-day: ~2.3B rows,
~0.7 TB. The hybrid stores only unresolved endpoints.)

### Output (from current `raw_gaps`, 2026-10-14)

A gap is long if it is open or ends 2+ calendar days after its start day.

| | All gaps | `gaps_long` | `gaps_daily` |
|---|---|---|---|
| All history | 701.7M | 143.7M (20.5%) | 558.0M (79.5%) |
| 2025 | 96.8M | 20.2M (20.9%) | 76.6M (79.1%) |

Bytes, from `raw_gaps_versioned` metadata (332 GB / 1.01B rows ≈ 330 B/row): latest
versions ≈ 230 GB, so `gaps_daily` ≈ 185 GB and `gaps_long` ≈ 47 GB. `gap_endpoints`
≈ 2 rows per long gap (~290M rows), each about half a gap row, so ≈ 47 GB too.
Derived from metadata and an average row size, not measured per table. `raw_gaps` was
missing 2026-08-15 onward at the time (daily rerun in progress); proportions unaffected.

## Code sketch

### 1. Core per-day logic (`pipelines/raw_gaps/fns/detect_day_gaps.py`)

```python
"""Detects the gaps owned by one (ssvid, day), from the messages of [day - 1, day + 1]."""

from datetime import timedelta
from itertools import pairwise

import apache_beam as beam

from gfw.common.datetime import datetime_from_timestamp

from pipe_gaps.core import GapDetector
from pipe_gaps.common.sorting import timestamp_msgid_key

ONE_DAY = timedelta(days=1)

TAG_GAPS = "gaps"
TAG_ENDPOINTS = "endpoints"

KIND_OFF = "OFF"
KIND_ON = "ON"


class DetectDayGaps(beam.DoFn):
    """Emits gaps and unresolved endpoints for one (ssvid, day).

    Ownership rule: a gap belongs to the day of its OFF message. Both sides of a gap
    see the same messages (adjacent days), so they always agree on who owns it:

        * OFF on `day`, next message in the window: complete gap, emitted here.
        * OFF on `day`, no next message in the window: unresolved OFF (gap > 24h).
        * ON on `day`, no previous message in the window: unresolved ON (gap > 24h).
        * Anything starting on day - 1 or day + 1: owned by that day, skipped.
    """

    def __init__(self, gap_detector: GapDetector):
        if gap_detector.min_gap_length >= ONE_DAY:
            raise ValueError("min_gap_length must be < 24h: unresolved endpoints assume it.")

        if gap_detector._n_hours_before > ONE_DAY:
            raise ValueError("n_hours_before must be <= 24h: lookback only reads day - 1.")

        self._gd = gap_detector

    def process(self, element):
        (ssvid, day), messages = element
        messages = sorted(messages, key=timestamp_msgid_key(GapDetector.KEY_TIMESTAMP))

        def day_of(m):
            return datetime_from_timestamp(m[GapDetector.KEY_TIMESTAMP]).date()

        # Complete gaps owned by this day.
        for off_m, on_m in pairwise(messages):
            if day_of(off_m) == day and self._gd._gap_condition(off_m, on_m):
                gap = self._gd.create_gap(
                    off_m, on_m, previous_positions=self._gd._previous_positions(messages, off_m)
                )
                yield beam.pvalue.TaggedOutput(TAG_GAPS, {**gap, "day": day})

        # Unresolved OFF: nothing until at least day + 2, so the gap is longer than 24h.
        # Stored like an open gap: it already carries the lookback counts and start_* fields.
        last = messages[-1]
        if day_of(last) == day:
            open_gap = self._gd.create_gap(
                last, previous_positions=self._gd._previous_positions(messages, last)
            )
            yield beam.pvalue.TaggedOutput(
                TAG_ENDPOINTS, {**open_gap, "kind": KIND_OFF, "day": day}
            )

        # Unresolved ON: nothing since before day - 1. Needs only its own fields.
        first = messages[0]
        if day_of(first) == day:
            on_fields = {
                f"{GapDetector.MESSAGE_PREFIX_ON}{k}": v
                for k, v in first.items() if k != GapDetector.KEY_SSVID
            }
            yield beam.pvalue.TaggedOutput(
                TAG_ENDPOINTS, {"ssvid": ssvid, **on_fields, "kind": KIND_ON, "day": day}
            )
```

### 2. PTransform (replaces `DetectGaps`)

```python
class DetectDailyGaps(beam.PTransform):
    """Each day's output depends only on the messages of [day - 1, day + 1].

    Days can run in any order or in parallel. Reprocessing [a, b] means
    running [a - 1, b + 1]. No side inputs, no state between runs.
    """

    def __init__(self, gap_detector: GapDetector, date_range: tuple[str, str], **kwargs):
        super().__init__(**kwargs)
        self._gap_detector = gap_detector
        self._start, self._end = [date.fromisoformat(x) for x in date_range]  # end exclusive

    def expand(self, messages):
        return (
            messages
            | "FanOutToNeighbourDays" >> beam.FlatMap(self._fan_out)
            | "GroupBySsvidDay" >> beam.GroupByKey()
            | beam.ParDo(DetectDayGaps(self._gap_detector)).with_outputs(TAG_GAPS, TAG_ENDPOINTS)
        )

    def _fan_out(self, message):
        # Each message is needed by its own day and by both neighbours.
        day = datetime_from_timestamp(message[GapDetector.KEY_TIMESTAMP]).date()
        for d in (day - ONE_DAY, day, day + ONE_DAY):
            if self._start <= d < self._end:
                yield (message[GapDetector.KEY_SSVID], d), message
```

The fan-out shuffles each message 3 times. If too costly, the existing
`ApplySlidingWindows` with a 1-day period and 1-day offset gives the same grouping.

### 3. Writes (factory)

```python
@property
def sinks(self):
    # One writer per table. Beam writes to staging tables; a post-hook swaps the days in.
    return {
        TAG_GAPS: WriteToBigQueryWrapper(self.config.bq_staging_gaps_daily, ...),
        TAG_ENDPOINTS: WriteToBigQueryWrapper(self.config.bq_staging_gap_endpoints, ...),
    }

@property
def post_hooks(self):
    # Tables are partitioned by month (4,000-partition limit), so days are replaced
    # with DELETE + INSERT in one transaction: running a range twice gives the same
    # result and readers never see a half-written range. No delete pre-hook.
    return [ReplaceDaysFromStaging(
        targets=[
            (self.config.bq_staging_gaps_daily, self.config.bq_out_gaps_daily),
            (self.config.bq_staging_gap_endpoints, self.config.bq_out_gap_endpoints),
        ],
        date_range=self.config.date_range,
    )]
```

```sql
-- ReplaceDaysFromStaging
BEGIN TRANSACTION;
DELETE FROM `{{ gaps_daily }}` WHERE day BETWEEN '{{ start }}' AND '{{ end }}';
INSERT INTO `{{ gaps_daily }}` SELECT * FROM `{{ staging_gaps_daily }}`;
DELETE FROM `{{ gap_endpoints }}` WHERE day BETWEEN '{{ start }}' AND '{{ end }}';
INSERT INTO `{{ gap_endpoints }}` SELECT * FROM `{{ staging_gap_endpoints }}`;
COMMIT TRANSACTION;
```

Messages are read for `[start - 1, end + 1]` (`messages_query_start_date` already does
the left side).

### 4. Pairing, full rebuild (`gaps_long.sql.j2`)

```sql
CREATE OR REPLACE TABLE `{{ gaps_long }}`
PARTITION BY DATE_TRUNC(DATE(start_timestamp), MONTH)
CLUSTER BY ssvid
AS
WITH paired AS (
  SELECT
    *,
    LEAD(kind) OVER w AS next_kind,
    LEAD(end_timestamp) OVER w AS next_end_timestamp,
    LEAD(end_lat) OVER w AS next_end_lat,
    LEAD(end_lon) OVER w AS next_end_lon,
    -- ... one LEAD per end_* column
  FROM `{{ gap_endpoints }}`
  -- A message isolated more than 24h on both sides is both an ON and an OFF.
  -- ON goes first, so it closes the previous gap before opening the next one.
  WINDOW w AS (
    PARTITION BY ssvid
    ORDER BY COALESCE(start_timestamp, end_timestamp), kind DESC
  )
)
-- An unresolved OFF closed by the next unresolved ON, or open.
-- An ON with no OFF before it (first message ever) is dropped by kind = 'OFF'.
SELECT
  gap_id,
  ssvid,
  next_kind = 'ON' AS is_closed,
  IF(next_kind = 'ON', TIMESTAMP_DIFF(next_end_timestamp, start_timestamp, SECOND) / 3600, NULL)
    AS duration_h,
  start_timestamp, start_lat, start_lon,  -- ... start_*
  IF(next_kind = 'ON', next_end_timestamp, NULL) AS end_timestamp,
  -- ... end_*, distance_m, implied_speed_knots
  positions_hours_before, positions_hours_before_sat,
  positions_hours_before_ter, positions_hours_before_dyn
FROM paired
WHERE kind = 'OFF'
```

Alternatively a small Python job that reads endpoints per ssvid and calls
`GapDetector.create_gap`, so long gaps get the same geodesic distance as short ones.

### 5. `raw_gaps` view

```sql
CREATE OR REPLACE VIEW `{{ raw_gaps }}` AS
SELECT * EXCEPT (day) FROM `{{ gaps_daily }}`
UNION ALL
SELECT * FROM `{{ gaps_long }}`
```

### 6. What goes away

* `RawGapsConfig`: `open_gaps_start_date`, `skip_open_gaps`, `bq_in_open_gaps`,
  `json_in_open_gaps`, `eval_last`, `window_period_d`, `versioned_suffix`.
  New: `bq_out_gaps_daily`, `bq_out_gap_endpoints`, `bq_out_gaps_long` (+ staging).
* `ProcessBoundaries`, `ExtractGroupBoundary`, `ProcessGroup`, open-gaps side input,
  `GapsDeleteQuery` and its delete pre-hook, SCD2.
* Composer: `depends_on_past` on gaps, open-gap handling between backfill and daily.
  Mid-history reprocess = clear [a-1, b+1], then step 2.

## Open points

1. **Distance:** a SQL pairing uses `ST_DISTANCE` (sphere); `GapDetector` uses geopy
   `geodesic` (ellipsoid), ~0.5% difference. A Python pairing job avoids it.
2. **Consistency checks:** OFF followed by OFF, or ON followed by ON, can't happen by
   construction; if seen, a partial reprocess left endpoints inconsistent. Add a
   validation query.
3. `_previous_positions` and `_gap_condition` would become public on `GapDetector`.
4. `version` loses its meaning: drop it or set it to run time.
5. UTC days assumed, as today.
6. Duplicate timestamps; segment filtering changing upstream (affects every design).
7. good_seg2 of D+1 not settled when D runs (see [Daily latency](#daily-latency)).
8. Check `raw_gaps_versioned` history usage before dropping SCD2.
9. Measure Dataflow cost of both designs; confirm the BigQuery pricing model.
10. Decide between the redesign and the cheaper v5 side-input fix (or both).
11. Cost of the DELETE + INSERT transaction per daily run (DML on monthly partitions
    clustered by `day`).

## Validation plan

Prototype on a few weeks of data and compare row by row against current `raw_gaps`
(gap_id, start/end, is_closed, counts). Focus cases: gaps of exactly 1-2 days, gaps
crossing midnight, isolated single messages (both ON and OFF), D+1 segments changing
between runs. Measure bytes and Dataflow cost for one day and for a multi-week rerun
under each design.
