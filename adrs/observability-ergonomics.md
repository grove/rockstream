# ADR: Operational Observability Ergonomics

**Status**: Proposed  
**Date**: 2026-05-29

---

## Context

RockStream's design already commits to being "unable to surprise you" (DESIGN.md v3.3).
The error-code taxonomy, audit log, and support bundle are all specified. But the
design stops short of specifying *how* errors and diagnostics are presented to the user.

Three gaps stand out:

1. **Error messages name the problem but not the solution.** An `RS-XXXX` code tells
   you what went wrong. It does not tell you what to do next.

2. **Workload membership is invisible at query time.** You cannot see which workload a
   view is running in from `EXPLAIN` output, which makes it hard to diagnose resource
   contention.

3. **Schema evolution breakage is discovered too late.** When an upstream source adds
   or changes a column, dependent views fail at query time rather than at schema change
   time.

---

## Decision

### 1. Actionable error messages

Every user-facing error message must include:
- What went wrong (the existing RS-XXXX code and description)
- Why it happened (a one-sentence cause where determinable)
- What to do next (a specific SQL command or config change)

Example — view lagging behind SLO:

```
❌ RS-4023: View reporting.daily_summary is lagging.
   Frontier age: 47s  |  SLO target: 10s
   Cause: Workload 'batch_analytics' is at MAX_PARALLELISM limit (8/8 workers busy).
   Next steps:
     ALTER WORKLOAD batch_analytics SET MAX_PARALLELISM = 16;
     — or —
     ALTER MATERIALIZED VIEW reporting.daily_summary SET FRESHNESS_SLO = '60s';
   Details: EXPLAIN INCREMENTAL reporting.daily_summary;
```

Example — workload over memory limit:

```
❌ RS-5011: Workload 'batch_analytics' exceeded MEMORY_LIMIT.
   Used: 108 GB  |  Limit: 100 GB
   Cause: View data_science.model_features triggered a large state expansion.
   Next steps:
     ALTER WORKLOAD batch_analytics SET MEMORY_LIMIT = '150GB';
     — or —
     Move data_science.model_features to a separate workload.
   Details: SELECT * FROM rockstream_catalog.workloads WHERE name = 'batch_analytics';
```

This is the difference between a monitoring alert that pages someone at 3am and one
that lets an on-call engineer resolve it without escalation.

**Implementation note**: error message templates are maintained alongside error code
definitions in the `RS-XXXX` registry. Each code has a required `next_steps` field in
its definition. Codes without `next_steps` fail CI.

### 2. Workload visibility in `EXPLAIN INCREMENTAL`

`EXPLAIN INCREMENTAL` output must include the effective workload for every view in the
plan, along with its current utilisation:

```sql
EXPLAIN INCREMENTAL SELECT * FROM reporting.daily_summary;
```

```
Materialized View: reporting.daily_summary
  Workload:    batch_analytics  (source: schema_default)
  Priority:    low
  Parallelism: 6 / 8 workers active
  Memory:      42 GB / 100 GB limit
  Freshness:   lag 3s, SLO 10s  ✓

  Operators:
    HashAggregate  [merge-safe: SUM via WeightAdd/v1]
    TableScan      reporting.orders
```

The `source` field (`view`, `schema_default`, or `system_default`) makes inherited
workload assignments explicit, preventing silent inheritance confusion.

### 3. Schema evolution visibility

When an upstream source's schema changes, RockStream surfaces the impact to dependent
views before queries start failing.

New command:

```sql
SHOW SCHEMA_EVOLUTION STATUS FOR SCHEMA reporting;
```

```
 View                         | Status           | Detail
------------------------------+------------------+---------------------------------------------
 reporting.daily_summary      | COMPATIBLE       | 2 columns added (nullable, no action needed)
 reporting.critical_metric    | ACTION REQUIRED  | Column 'price' type changed: INT → FLOAT
 reporting.weekly_rollup      | COMPATIBLE       | No changes detected
```

The system continuously checks source-to-view schema compatibility as part of the
control-plane health loop. Incompatible changes raise a named event in the audit log
(`RS-6001: SCHEMA_INCOMPATIBILITY_DETECTED`) before views break.

Users can also inspect evolution history:

```sql
SHOW SCHEMA_EVOLUTION HISTORY FOR MATERIALIZED VIEW reporting.critical_metric;
```

```
 Timestamp            | Event                        | Status
----------------------+------------------------------+------------------
 2026-05-29 08:14:22  | source 'orders' added col    | COMPATIBLE
 2026-05-29 09:01:05  | source 'orders' changed type | ACTION REQUIRED
```

---

## Consequences

### What gets better

- On-call engineers can self-serve most common incidents without reading internal docs.
- Workload attribution is always visible, making resource contention diagnosable in
  seconds rather than minutes.
- Schema breakage is caught proactively, not reactively after queries fail in
  production.

### What we accept

- Actionable error messages are harder to write than plain error descriptions.
  The `next_steps` requirement in the error registry adds authoring cost but ensures
  completeness.
- Schema evolution monitoring adds a background control-plane heartbeat. This is a
  bounded, low-frequency check (not per-query), so the overhead is negligible.
- `EXPLAIN INCREMENTAL` now includes runtime state (worker utilisation, memory usage).
  This means it is no longer a pure static plan tool — it requires a round-trip to the
  control plane. This should be documented clearly.

---

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| Leave next steps to documentation | Docs are not visible at 3am when an alert fires. The message must be self-contained. |
| Show workload only in catalog queries, not EXPLAIN | EXPLAIN is the first place an operator looks when debugging a slow view. Hiding workload there means a second lookup every time. |
| Schema evolution warnings only in logs | Logs are noisy. A dedicated command and named error code are actionable; a log line gets lost. |
