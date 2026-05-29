# ADR: Resource Usage Visibility

**Status**: Accepted  
**Date**: 2026-05-29

---

## Context

The design specifies `STATE_BUDGET` enforcement and workload `MEMORY_LIMIT` quotas.
When either is exceeded, error codes fire (`RS-5011`, `RS-5021`). But users have no
way to see how much they are *currently consuming* relative to their budgets until
something breaks.

This is the same problem as a bank account that only sends a notification when you go
into overdraft — you want to see your balance *before* the problem, not after.

The "unable to surprise you" principle requires that resource consumption is visible at
any time, without waiting for an alert.

---

## Decision

### `SHOW RESOURCE USAGE`

A top-level command that shows consumption across all workloads and views:

```sql
SHOW RESOURCE USAGE;
```

```
  Workload          | Views | State Used | State Budget | Memory Used | Memory Limit | SLO Health
  ------------------+-------+------------+--------------+-------------+--------------+-----------
  batch_analytics   |    12 | 42 GB      | 100 GB  (42%)| 61 GB       | 100 GB  (61%)| ✓ all meeting SLO
  realtime          |     3 | 8 GB       | 50 GB   (16%)| 14 GB       | 50 GB   (28%)| ✓ all meeting SLO
  system_default    |     2 | 1 GB       | unlimited    | 2 GB        | unlimited    | ⚠ 1 SLO breach
```

A breakdown per view within a workload:

```sql
SHOW RESOURCE USAGE FOR WORKLOAD batch_analytics;
```

```
  View                     | State Used | State Budget  | Freshness | SLO   | Priority
  -------------------------+------------+---------------+-----------+-------+---------
  reporting.daily_summary  | 18 GB      | 20 GB   (90%) | 3s        | 5s ✓  | high
  reporting.weekly_rollup  | 12 GB      | unlimited     | 4s        | 5s ✓  | normal
  data_science.model_input | 11 GB      | 30 GB   (37%) | 8s        | 10s ✓ | low
  ...
```

A warning is surfaced for any view consuming more than 80% of its budget:
```
  ⚠ reporting.daily_summary is at 90% of its STATE_BUDGET (18 GB / 20 GB).
    Consider: ALTER MATERIALIZED VIEW reporting.daily_summary SET STATE_BUDGET = '30 GB';
```

### Catalog query surface

The same data is available via SQL for dashboards and alerting:

```sql
-- All views with their resource consumption
SELECT
  schema_name,
  view_name,
  workload_name,
  state_bytes,
  state_budget_bytes,
  ROUND(100.0 * state_bytes / NULLIF(state_budget_bytes, 0), 1) AS state_pct,
  memory_bytes,
  memory_limit_bytes,
  freshness_lag_ms,
  slo_ms
FROM rockstream_catalog.view_resource_usage
ORDER BY state_pct DESC NULLS LAST;

-- Workloads approaching limits
SELECT workload_name, memory_bytes, memory_limit_bytes
FROM rockstream_catalog.workload_resource_usage
WHERE memory_bytes > 0.8 * memory_limit_bytes;
```

### Proactive warnings

At 80% of any budget, the system emits a `NOTICE` (not an error) via the active
session and writes an audit event:

```
NOTICE RS-5018: View 'reporting.daily_summary' is at 90% of STATE_BUDGET (18 GB / 20 GB).
```

At 95%, the warning escalates to a `WARNING` level visible in monitoring:

```
WARNING RS-5019: View 'reporting.daily_summary' is at 95% of STATE_BUDGET (18 GB / 20 GB).
  Action recommended before the view enters DEGRADED state.
  ALTER MATERIALIZED VIEW reporting.daily_summary SET STATE_BUDGET = '30 GB';
```

The 80% and 95% thresholds are configurable per workload:
```sql
ALTER WORKLOAD batch_analytics
  SET STATE_WARNING_THRESHOLD = 0.75,
      STATE_CRITICAL_THRESHOLD = 0.90;
```

### Cluster-wide summary

For operators managing a multi-workload cluster:

```sql
SHOW CLUSTER RESOURCE USAGE;
```

```
  Cluster resource summary
  -------------------------
  Total workers:     8
  Active views:      17
  Total state:       51 GB
  Total memory:      77 GB / ~available from workers

  Workloads:         3 defined
  SLO compliance:    16/17 views meeting SLO (94%)
  DLQ entries (24h): 847  ⚠

  Top state consumers:
    reporting.daily_summary     18 GB
    data_science.model_input    11 GB
    reporting.weekly_rollup     12 GB
```

---

## Consequences

### What gets better

- Users see resource pressure building up before limits are hit.
- A single command gives an immediate health overview of the whole system.
- The catalog tables enable integration with external dashboards (Grafana, Datadog)
  without requiring custom metric instrumentation.
- Proactive warnings give users time to raise budgets before a view enters DEGRADED.

### What we accept

- `SHOW RESOURCE USAGE` requires a round-trip to the control plane to aggregate stats
  across workers. This is a low-frequency operational command, not a query-path
  concern; the latency is acceptable.
- The 80%/95% threshold system adds minor configuration surface, but the defaults
  are sensible for most users.

---

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| Only expose via Prometheus metrics | Metrics require external infrastructure to query. Operators need answers without standing up a monitoring stack first. |
| Only warn when a limit is hit | Too late. The point is to prevent surprises, not to document them after they happen. |
| Per-view resource commands only | A workload-level summary is essential for multi-view diagnosis. You need to see the whole picture at once. |
