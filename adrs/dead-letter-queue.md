# ADR: Dead Letter Queue — User-Facing Surface

**Status**: Accepted  
**Date**: 2026-05-29

---

## Context

The design specifies that connector decode errors route to a dead letter queue (DLQ)
sink as `RS-1003`. This prevents malformed records from crashing a source connector,
but the DLQ has no user-facing surface in the current design.

Without a way to inspect, replay, or dismiss DLQ entries, a connector decode error
causes:
- Silent data loss (the view is missing rows)
- No visible signal unless the user monitors `RS-1003` error counters
- No recovery path short of restarting from an earlier offset

This violates the "unable to surprise you" principle. A view that is quietly missing
rows because of upstream format changes is one of the hardest classes of bugs to
diagnose in a streaming system.

---

## Decision

### The DLQ is queryable via catalog tables

Every source connector maintains a per-source DLQ table. Entries are retained for
a configurable period (default: 7 days).

```sql
-- Inspect recent errors for a specific source
SELECT
  arrived_at,
  source_offset,
  error_code,
  error_message,
  raw_bytes_hex
FROM rockstream_catalog.dead_letter_queue
WHERE source_name = 'kafka_orders'
ORDER BY arrived_at DESC
LIMIT 20;
```

```
  arrived_at               | source_offset | error_code | error_message
  -------------------------+---------------+------------+-----------------------------------------------
  2026-05-29 09:14:22 UTC  | partition=3   | RS-1003    | decode error: expected INT got STRING in col 'price'
                           | offset=9821   |            |
  2026-05-29 08:57:11 UTC  | partition=1   | RS-1003    | decode error: unknown field 'discount_pct'
                           | offset=7204   |            |
```

The `raw_bytes_hex` column contains the original record bytes, allowing users to
inspect the malformed payload directly.

### DLQ health is surfaced proactively

The system does not require users to poll the DLQ. Two proactive signals:

1. **`RS-1004: DLQ_GROWING`** fires when a source has accumulated more than
   `dlq_warn_threshold` entries (default: 100) in a rolling 1-hour window. The error
   message names the source and the most common error:
   ```
   ⚠ RS-1004: Source 'kafka_orders' dead letter queue is growing.
     New entries (last 1h): 847
     Most common error: RS-1003 decode error in column 'price' (99% of entries)
     Inspect: SELECT * FROM rockstream_catalog.dead_letter_queue WHERE source_name = 'kafka_orders';
     Replay after fixing: ALTER SOURCE kafka_orders REPLAY DEAD_LETTER_QUEUE;
   ```

2. **`SHOW VIEW STATUS`** (see view-lifecycle ADR) includes a DLQ column:
   ```
   View              | State | DLQ entries (7d) | Note
   ------------------+-------+------------------+-----
   daily_summary     | READY | 847              | ⚠ RS-1004: DLQ growing
   critical_metric   | READY | 0                |
   ```

### Replay and dismiss commands

Once the upstream issue is fixed (e.g. a schema change is deployed), users can
replay DLQ entries:

```sql
-- Replay all DLQ entries for a source
ALTER SOURCE kafka_orders REPLAY DEAD_LETTER_QUEUE;

-- Replay entries from a specific time window
ALTER SOURCE kafka_orders REPLAY DEAD_LETTER_QUEUE
  SINCE '2026-05-29 08:00:00'
  UNTIL '2026-05-29 10:00:00';

-- Dismiss entries that are known-bad and should not be replayed
ALTER SOURCE kafka_orders DISMISS DEAD_LETTER_QUEUE
  WHERE arrived_at < now() - INTERVAL '7 days';

-- Dismiss a specific entry by offset
ALTER SOURCE kafka_orders DISMISS DEAD_LETTER_QUEUE
  WHERE source_offset = 'partition=3,offset=9821';
```

Replay is idempotent: replaying an entry that was already successfully processed has
no effect. Replay uses the same decode path as live ingestion; if the issue is not
fixed, the entry goes back to the DLQ with a `replay_attempt` counter incremented.

### DLQ retention policy

DLQ retention is configurable per source:

```sql
CREATE SOURCE kafka_orders FROM KAFKA ...
  WITH (DLQ_RETENTION = '30 days');

ALTER SOURCE kafka_orders SET DLQ_RETENTION = '14 days';
```

The system default is 7 days. Setting `DLQ_RETENTION = '0'` disables the DLQ for
that source — decode errors are logged and the record is dropped silently. This is
not recommended.

---

## Consequences

### What gets better

- Silent data loss from decode errors is no longer silent.
- Users can recover from upstream schema changes without manual offset rewind.
- The `raw_bytes_hex` column makes it possible to diagnose exactly what the
  malformed payload looked like.
- Dismissing entries explicitly is cleaner than relying on TTL for known-bad records.

### What we accept

- Storing raw payload bytes in the DLQ consumes storage. The `raw_bytes_hex` field is
  bounded per entry by the source's `max_record_bytes` limit; entries exceeding this
  are truncated with a `truncated=true` flag.
- Replay re-processes entries through the live connector decode path. If the schema has
  been updated in a way that's still incompatible, the entry simply re-enters the DLQ.
  The `replay_attempt` counter bounds runaway replay loops.

---

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| Only log errors, no DLQ table | Logs are ephemeral and not queryable. The DLQ must be a first-class catalog object. |
| External DLQ (write to a Kafka error topic) | Adds an external dependency. The built-in DLQ is sufficient for the vast majority of use cases; users who need external routing can add a sink on the DLQ table. |
| Auto-replay after detect | Re-ingesting possibly-malformed records automatically could cause worse damage. Manual confirmation is the right gate. |
