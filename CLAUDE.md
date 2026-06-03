# pipe-gaps — Claude conventions

This file is auto-loaded by Claude Code. Rules apply to the LLM working in
this repo.

## Data sources for testing, exploration, and integration tests

**Default to the dit staging cohort.** When writing or running any
BigQuery-touching code in this repo or via the sibling
[data_integration_tests](../data_integration_tests) framework — ad-hoc
`bq query`, pipe-gaps invocations against BQ, dit workflows, smoke
tests, forensic queries — the source-data CLI flags (`--source-messages`,
`--source-segments`, `--bq-input-messages`, `--bq-input-segments`, etc.)
**must default to the `pipe_ais_test_202408290000` staging cohort** in
project `world-fishing-827`. The canonical table inventory lives in
[data_integration_tests/README.md § "Staging data sources"][stagedoc].

Production datasets — `gfw-int-vms-v3.*`, `gfw-int-ais-v3.*`,
`global-fishing-watch.pipe_*_published.*`, etc. — are **opt-in per
invocation, with explicit user clearance in the chat turn**. Defaulting
any flag to a prod FQN is not allowed; defaulting "off" but with a
prod-pointing flag value sitting in the CLI is not allowed either.

To target prod for a specific forensic reproduction (e.g. the 9cc...
sub-threshold-close case in VMS), the user passes the prod FQN
explicitly. Before issuing such a command, the LLM:

1. Confirms in chat that the target is prod and names the specific
   tables.
2. Gets explicit "yes proceed" before invoking.
3. Logs the resulting BQ job IDs / Dataflow job names so the user can
   trace and bound cost.

### Why this rule exists

- **Cost.** Production scans are billed at full size; a smoke-test
  scan against `gfw-int-vms-v3` is real money. Staging is small
  (~few GB per input) and safe to query freely.
- **Org boundary.** Prod VMS lives in `gfw-int-vms-v3`, which sits in
  a different GCP org from `world-fishing-827`. BQ's
  `CREATE SNAPSHOT TABLE ... CLONE` refuses cross-org sources, so any
  dit workflow that snapshots prod-VMS sources into world-fishing-827
  fails. This bit us once (PR #43 outage_recovery default).
- **Reproducibility.** Staging is a pinned snapshot of a representative
  slice; prod is a moving target. Forensic snapshots that pass today
  may not be reproducible next week (BQ time-travel expires at 7 days).
- **Blast radius.** Bugs in test fixtures that accidentally hit prod
  can produce real writes to user-facing datasets. Staging defaults
  scope the blast radius to a scratch project.

[stagedoc]: ../data_integration_tests/README.md
