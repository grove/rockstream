# ADR: View Lifecycle States and Backfill Visibility

**Status**: Proposed  
**Date**: 2026-05-29

---

## Context

When a user creates a materialized view over a large existing dataset, the view goes
through a backfill phase before it can serve live queries. The design specifies a
`BUILDING → READY` lifecycle and mentions backfill progress tracking, but provides no
user-facing commands for observing or managing this.

This leaves users in the dark:
- How long will backfill take?
- Is it still making progress?
- Can I query the view while it's building?
- What do I do when a view enters `DEGRADED` or `RECOVERING`?

The design's operability principle ("unable to surprise you") requires that every state
a view can be in has a named reason, a user-visible command, and a clear recovery path.

---

## Decision

### View lifecycle states

Every materialized view is in exactly one of these states at any time:

| State | Meaning | Queryable? |
|---|---|---|
| `BUILDING` | Initial backfill in progress | Yes (returns backfilled rows so far; may be incomplete) |
| `READY` | Live and meeting SLO | Yes |
| `PAUSED` | Manually paused by user | Yes (stale snapshot) |
| `RECOVERING` | Recovering from failure; catching up | Yes (stale snapshot) |
| `DEGRADED` | Running but breaching SLO or STATE_BUDGET | Yes (with staleness warning) |
| `REPLACED` | Being replaced by a newer version (see application-ergonomics ADR) | No (swap pending) |
| `DROPPED` | Scheduled for deletion | No |

Views in `BUILDING`, `RECOVERING`, and `DEGRADED` are always queryable. They return
results with an attached staleness or completeness annotation in the response metadata,
which ORMs and clients can surface to users.

### Backfill visibility commands

```sql
-- Status for one view
SHOW BACKFILL STATUS FOR MATERIALIZED VIEW reporting.daily_summary;
```

```
  View:       reporting.daily_summary
  State:      BUILDING
  Progress:   2.1B / 4.8B rows  (44%)
  Elapsed:    1h 47m
  ETA:        ~2h 10m  (confidence: medium)
  Throughput: 340k rows/s
  Note:       Streaming updates are buffering during backfill and will be
              applied automatically when BUILDING completes.
```

```sql
-- Status for all views in a schema
SHOW VIEW STATUS FOR SCHEMA reporting;
```

```
  View                   | State      | Freshness | SLO   | Note
  -----------------------+------------+-----------+-------+-----
  daily_summary          | BUILDING   | —         | 5s    | 44% complete, ETA 2h
  critical_metric        | READY      | 1s        | 5s ✓  |
  weekly_rollup          | PAUSED     | 4d ago    | —     | Paused manually
  user_cohorts           | DEGRADED   | 47s       | 10s ✗ | SLO breach: RS-4023
```

```sql
-- Status for all views in the cluster
SHOW VIEW STATUS;
```

These commands are also exposed as catalog queries for tooling and alerting:

```sql
SELECT view_name, state, freshness_lag_ms, slo_ms, state_reason
FROM rockstream_catalog.materialized_views
WHERE state != 'READY';
```

### Recovery path for each non-READY state

The system must always tell the user what to do. For each non-READY state:

**BUILDING** — Normal. Wait, or query with awareness of incompleteness. No action
needed unless backfill has stalled (no progress for > 10 minutes), in which case
`RS-4030: BACKFILL_STALLED` fires with a cause.

**PAUSED** — Resume with:
```sql
ALTER MATERIALIZED VIEW reporting.weekly_rollup RESUME;
```

**RECOVERING** — Normal after a failure. Monitor with `SHOW BACKFILL STATUS`. If
recovering too slowly: `RS-4031: RECOVERY_SLO_BREACH` fires with:
```
Next steps:
  ALTER WORKLOAD batch_analytics SET MAX_PARALLELISM = 16;
  -- or increase FRESHNESS_SLO to accept slower recovery:
  ALTER MATERIALIZED VIEW ... SET FRESHNESS_SLO = '60 seconds';
```

**DEGRADED** — Investigate with `EXPLAIN INCREMENTAL ANALYZE`. Common causes have
named error codes with next-steps (see observability-ergonomics ADR).

### Background DDL

Backfill runs in the background by default. Users can check in, disconnect, and
reconnect without affecting the backfill. The session that issued `CREATE MATERIALIZED
VIEW` does not need to stay open.

For users who want an explicit background signal:

```sql
SET BACKGROUND_DDL = ON;
CREATE MATERIALIZED VIEW reporting.large_view AS SELECT ...;
-- Returns immediately:
-- INFO: View 'reporting.large_view' is building in background (job_id: abc123).
-- Use SHOW BACKFILL STATUS FOR MATERIALIZED VIEW reporting.large_view to monitor.

-- Later, optionally wait for it:
WAIT FOR MATERIALIZED VIEW reporting.large_view TO BE READY TIMEOUT '1 hour';
```

---

## Consequences

### What gets better

- Users always know what state a view is in and what to do about it.
- Backfill progress gives a meaningful ETA rather than a black box.
- Views are queryable in all non-DROPPED states, with clear completeness annotations.
- The system never surprises users with unexplained staleness.

### What we accept

- The `WAIT FOR ... TO BE READY` syntax requires a server-side wait mechanism that
  polls or subscribes to view state changes. This is bounded in scope but adds
  implementation surface.
- ETA estimates will be imprecise for non-uniform data distributions. The output must
  always show a confidence label.

---

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| Block CREATE until READY | Unusable for large datasets. Backfill of billions of rows cannot be a synchronous operation. |
| Only expose state via catalog tables | Too low-level. `SHOW BACKFILL STATUS` is the right first surface for interactive users. Catalog tables are for tooling. |
| Disallow queries on BUILDING views | Users often want to validate correctness during backfill. Making views unqueryable during building adds a painful wait with no benefit. |
