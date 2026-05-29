# ADR: SUBSCRIBE — Change Stream Ergonomics

**Status**: Accepted  
**Date**: 2026-05-29

---

## Context

The design mentions a subscribe API for consuming view change streams (v0.42) but
never specifies the user-facing DDL or ergonomics. This is a core developer-facing
API: it is how applications consume real-time changes from a materialized view without
polling.

Decisions that must be made before implementation:
- What SQL syntax does a developer use to open a change stream?
- What does each change event look like?
- What happens on disconnect — can you resume from a position?
- How long are changes retained?
- How does this interact with the frontier / freshness model?

---

## Decision

### Basic syntax

```sql
SUBSCRIBE reporting.daily_summary;
```

This opens a live stream of change events from the view. The stream never ends until
the client disconnects or sends a cancel. Each row in the result set is a change event:

```
  mz_timestamp  | mz_diff | region   | total
  --------------+---------+----------+-------
  1748511262003 |      +1  | us-east  | 42500
  1748511262003 |      -1  | us-east  | 41800  ← previous value retracted
  1748511263041 |      +1  | eu-west  | 18200
  ...
```

- `mz_timestamp`: the logical timestamp of the change (epoch boundary)
- `mz_diff`: `+1` for an insert or updated-to value; `-1` for a delete or updated-from value
- The remaining columns are the view's output columns

Updates arrive as a retraction/insertion pair: the old value with `mz_diff = -1`
followed by the new value with `mz_diff = +1` at the same timestamp. Clients can
reconstruct the current state by applying these diffs in order.

### Starting position

By default, `SUBSCRIBE` delivers all **future** changes from the moment the command
is issued. To receive current state plus future changes:

```sql
SUBSCRIBE reporting.daily_summary AS OF NOW WITH SNAPSHOT;
```

This first emits the current contents of the view (all rows with `mz_diff = +1`) and
then continues with live changes. This is the recommended pattern for applications
that need to bootstrap their local state.

To resume from a specific position after a disconnect:

```sql
SUBSCRIBE reporting.daily_summary AS OF EPOCH 1748511262003;
```

This replays all changes from that epoch forward, allowing gap-free resumption. The
epoch must be within the view's retention window (configurable via `CHANGE_RETENTION`
on the view; default 1 hour).

### Retention

Change retention is configurable per view:

```sql
CREATE MATERIALIZED VIEW reporting.daily_summary
  WITH (CHANGE_RETENTION = '6 hours')
AS SELECT ...;

ALTER MATERIALIZED VIEW reporting.daily_summary
  SET CHANGE_RETENTION = '24 hours';
```

Default is 1 hour. Setting `CHANGE_RETENTION = '0'` disables retention entirely —
subscribers that disconnect cannot resume and must restart with `WITH SNAPSHOT`.

Attempting to subscribe from an epoch outside the retention window returns:
```
RS-2005: epoch outside retention window
  Oldest available epoch: 1748507662003
  Requested epoch:        1748497662003  (2.7 hours ago)
  View CHANGE_RETENTION:  1 hour
  Use: SUBSCRIBE ... AS OF EPOCH 1748507662003
  Or:  SUBSCRIBE ... AS OF NOW WITH SNAPSHOT (restart from current state)
```

### Filtering and projection

Subscribers can filter the change stream to reduce network traffic:

```sql
SUBSCRIBE reporting.daily_summary
  WHERE region = 'us-east';

-- Projection (receive only specific columns):
SUBSCRIBE reporting.daily_summary (region, total);
```

Filters and projections are evaluated on the server side before transmission.

### Interaction with freshness

`SUBSCRIBE` respects the view's frontier. Changes are delivered only after an epoch
closes — the client always sees a consistent snapshot at each timestamp, never a
partial epoch. The maximum latency from a real-world event to delivery in the
subscriber is bounded by the view's epoch size (which is auto-tuned to meet the
`FRESHNESS_SLO`).

### Programmatic access

Via pgwire, `SUBSCRIBE` behaves like a long-running `SELECT` that keeps returning
rows. Standard Postgres clients handle this correctly; the stream ends when the server
closes the connection or the client cancels.

Example in pseudocode:

```python
with conn.cursor() as cur:
    cur.execute("SUBSCRIBE reporting.daily_summary AS OF NOW WITH SNAPSHOT")
    for row in cur:
        ts, diff, region, total = row
        if diff == 1:
            state[region] = total
        elif diff == -1:
            pass  # retraction; the +1 for the new value follows
```

### `SUBSCRIBE` vs. `SELECT` with freshness tokens

| Need | Use |
|---|---|
| Current snapshot, once | `SELECT * FROM view` |
| Current snapshot, wait for a specific write | `SELECT * FROM view` (session read-after-write is automatic) |
| Live stream of future changes | `SUBSCRIBE view` |
| Live stream + current state bootstrap | `SUBSCRIBE view AS OF NOW WITH SNAPSHOT` |
| Resume after disconnect | `SUBSCRIBE view AS OF EPOCH <n>` |

---

## Consequences

### What gets better

- Developers have a single, clear mental model for consuming live view changes.
- Resume-from-position prevents data loss on disconnect without re-bootstrapping.
- Server-side filtering reduces network traffic for high-volume views.
- The `mz_diff` convention mirrors Materialize's change stream format, giving
  developers familiar with Materialize a zero-learning-curve experience.

### What we accept

- `CHANGE_RETENTION` adds storage overhead: the system must retain change log entries
  for the configured window beyond the normal frontier. This cost is charged against
  the view's `STATE_BUDGET`.
- `WITH SNAPSHOT` initial delivery may be slow for large views. The client will
  receive a large batch of `mz_diff = +1` rows before the live stream begins. Clients
  should handle this without timeout.
- The `mz_timestamp` / `mz_diff` naming borrows from Materialize convention. If a
  cleaner namespace is preferred, `rs_epoch` / `rs_diff` are the alternatives.

---

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| Polling with freshness tokens | Requires clients to implement polling loops. `SUBSCRIBE` is push-based and more efficient. |
| Kafka as the only change delivery mechanism | Requires external Kafka infrastructure. The built-in subscribe API is the right first surface. |
| Blocking `SELECT` with `WAIT FOR CHANGES` | Non-standard SQL; harder to implement correctly across pgwire. `SUBSCRIBE` is a well-understood pattern (Materialize, CockroachDB). |
| Deliver partial epochs to reduce latency | Partial epochs break consistency. Clients would see a mix of old and new values for the same timestamp. Not acceptable. |
