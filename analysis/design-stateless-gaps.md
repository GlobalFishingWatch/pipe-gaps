# Design sketch: stateless raw gaps (per-day detection + pairing)

Status: **draft / untested**. Written 2026-10-13 from a design discussion, after
PIPELINE-4652 (the 2026 backfill bucket lost its open gaps and could only be fixed
by rerunning from 2026-01-01 up to today). Nothing here has been run.

## Problem

`raw_gaps` is computed as a sequential fold: each run reads the open gaps written by
the previous run (side input), so a change at day *t* changes the state for every day
after *t*. Consequences:

* Fixing a period in the middle of history requires rerunning from there to today.
* A full reprocess is sequential (yearly buckets chained by `depends_on_past`).
* A bad run propagates forward (the 2026 bucket / `eval_last` incident).

## Idea

Only one thing crosses time: pairing "last message before a silence" with "first
message after it". Everything else is local to a day.

1. **Step 1 (per day, Beam).** For each (ssvid, day D), read messages of D-1, D, D+1.
   * **Ownership rule:** a gap belongs to the day of its OFF message.
   * OFF on D, next message within the window: **complete gap** -> `gaps_daily`.
   * OFF on D, no next message within the window (gap > 24h): **unresolved OFF** ->
     `gap_endpoints`, stored as an open gap carrying the `positions_hours_before*` counts.
   * ON on D, no previous message within the window (gap > 24h): **unresolved ON** ->
     `gap_endpoints`, only its own `end_*` fields.
   * Gaps starting on D-1 or D+1 are skipped (owned by those days). Both sides see the
     same messages, so they always agree; the decision depends on input messages, never
     on another run's output.
2. **Step 2 (pairing).** Per ssvid, order unresolved endpoints and `LEAD()`:
   unresolved OFF followed by unresolved ON = closed long gap; OFF with nothing after =
   open gap; ON with no OFF before = first message ever, dropped. An unresolved OFF can
   only be followed by its ON, since no messages exist between them.
   Can be a plain view: `raw_gaps = gaps_daily UNION ALL paired(gap_endpoints)`.

**Reprocess rule:** fixing [a, b] = rerun step 1 for [a-1, b+1] (windows reach one day
each way), replacing those partitions. The view picks it up. Nothing after b+1 reruns.

**Changing N (`n_hours_before`) or `min_gap_length`:** full rerun of step 1 (same as
today) but fully parallel. Optionally store counts for several N as extra columns.

### Constraints

* `min_gap_length < 24h` (unresolved endpoints are assumed to be gaps).
* `n_hours_before <= 24h` with a one-day lookback. Larger N needs a longer read back
  for the counts only; ownership windows stay at D-1 / D+1.

### Daily latency

AIS daily gaps for D already wait for D+1 (`aggregate_segs_stabilization_days=1`,
sensors `wait_for_next_day_segs_activity` and next-day `measures.add_measures` in
composer-dags-production `dags/core/ais/v5.py`), so reading D+1 adds no latency.
**Subtlety:** D's good_seg2 is settled at that point, D+1's is not. Options: each
daily run also rewrites the previous day (preferred), or wait 2 days.

### Versioned table

Probably unnecessary. SCD2 exists in v5 as a mechanism to "update" open gaps through
append-only writes, plus the delete pre-hook for reruns. Here nothing is updated in
place: step 1 partitions are replaced idempotently and pairing is derived. If history
is needed: BigQuery time travel (7 days), table snapshots before reprocesses, or
periodic snapshots. **Check first** that nobody queries `raw_gaps_versioned` for
history (`INFORMATION_SCHEMA.JOBS`).

## Comparison (cost and operations; output is the same by design)

| | Prototype | Current v5 | Proposed |
|---|---|---|---|
| Daily message reads | OFF query D..D+1 + ON query D-1..D (~4 day-reads) | D-1, D (2) | D-1..D+1 in one pass (3) |
| Daily pairing | Join of all historical OFF/ON every day, RANK (grows over time) | None, but side input of every open gap since 2012 | `LEAD` over unresolved endpoints only (small), or a view |
| Full reprocess | Days parallel, then one pairing | Sequential, `depends_on_past` chain | Parallel ranges, each read once + 2 edge days |
| Mid-history fix | Recompute those days, next pairing picks it up | Rerun from fix to today | Recompute [a-1, b+1] |
| A bad run | Local | Propagates forward | Local |
| Moving parts | 3 SQL queries/day + pairing | Beam + open-gap side input + `eval_last` + SCD2 + delete hook + `depends_on_past` | Beam + 2 tables + view |
| Change N / threshold | Rerun all, parallel | Full rerun, sequential | Full rerun, parallel |
| Latency | Waits D+1 | Waits D+1 (good_seg2) | Waits D+1 (same) |

Qualitative only, based on how much each design reads; nothing measured. The prototype
description comes from `pipe-gaps-prototype/assets/ais_off_on_events.sql.j2` and
`ais_gap_events.sql.j2` (its pairing query has a WHERE that "keeps only valid matches",
patching mismatched pairs).

### Size estimates (sampled, `measures_v1.messages_positions`)

| Day | Messages | Vessels |
|---|---|---|
| 2013-06-15 | 6.7M | 117k |
| 2017-06-15 | 28.1M | 194k |
| 2021-06-15 | 62.2M | 246k |
| 2025-06-15 | 136.2M | 358k |

Messages table: ~341B rows, 95.6 TB. A one-row-per-vessel-day boundary table would be
~2.3B rows (~0.7 TB); the hybrid above stores only unresolved endpoints (gaps > ~1 day),
far fewer. Vessel counts include ssvids dropped later by segment filtering, so they
overestimate. Storing 12h of messages per boundary instead of precomputed counts would
be ~half the messages table, so counts must be precomputed in step 1.

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
        # Stored as an open gap: it already carries the lookback counts and start_* fields.
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
def pre_hooks(self):
    # Running a range twice gives the same result: replace the range in both tables.
    return [
        DeleteDateRange(self.config.bq_out_gaps_daily, "day", self.config.date_range),
        DeleteDateRange(self.config.bq_out_gap_endpoints, "day", self.config.date_range),
    ]

@property
def sinks(self):
    return {
        TAG_GAPS: WriteToBigQueryWrapper(self.config.bq_out_gaps_daily, ...),
        TAG_ENDPOINTS: WriteToBigQueryWrapper(self.config.bq_out_gap_endpoints, ...),
    }
```

Messages are read for `[start - 1, end + 1]` (`messages_query_start_date` already does
the left side).

### 4. Pairing view (`raw_gaps.sql.j2`)

```sql
CREATE OR REPLACE VIEW `{{ raw_gaps }}` AS
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

-- Long gaps: an unresolved OFF closed by the next unresolved ON, or open.
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

UNION ALL

-- Gaps fully resolved within their day window.
SELECT * EXCEPT (day) FROM `{{ gaps_daily }}`
```

An unresolved ON with no OFF before it (first message ever) is dropped by
`WHERE kind = 'OFF'`.

### 5. What goes away

* `RawGapsConfig`: `open_gaps_start_date`, `skip_open_gaps`, `bq_in_open_gaps`,
  `json_in_open_gaps`, `eval_last`, `window_period_d`, `versioned_suffix`.
  New: `bq_out_gaps_daily`, `bq_out_gap_endpoints`.
* `ProcessBoundaries`, `ExtractGroupBoundary`, `ProcessGroup`, open-gaps side input,
  `GapsDeleteQuery` open/closed logic, SCD2.
* Composer: `depends_on_past` on gaps, open-gap handling between backfill and daily.
  Mid-history reprocess = clear [a-1, b+1].

## Open points

1. **Distance:** the view uses `ST_DISTANCE` (sphere); `GapDetector` uses geopy
   `geodesic` (ellipsoid), ~0.5% difference. For identical output, pairing would be a
   small Python job calling `create_gap` instead of a view.
2. **Consistency checks:** OFF followed by OFF, or ON followed by ON, can't happen by
   construction; if seen, a partial reprocess left endpoints inconsistent. Add a
   validation query.
3. `_previous_positions` and `_gap_condition` would become public on `GapDetector`.
4. `version` loses its meaning: drop it or set it to run time.
5. UTC days assumed, as today.
6. Duplicate timestamps; segment filtering changing upstream (affects every design).
7. Check `raw_gaps_versioned` history usage before dropping SCD2.

## Validation plan

Prototype on a few weeks of data and compare row by row against current `raw_gaps`
(gap_id, start/end, is_closed, counts). Focus cases: gaps of exactly 1-2 days, gaps
crossing midnight, isolated single messages (both ON and OFF), D+1 segments changing
between runs. Also measure bytes read for one day under each design.
