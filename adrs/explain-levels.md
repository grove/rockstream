# ADR: EXPLAIN INCREMENTAL — Three Output Levels

**Status**: Accepted  
**Date**: 2026-05-29

---

## Context

`EXPLAIN INCREMENTAL` is the primary tool for understanding a materialized view. The
design specifies rich output: merge law IDs, law versions, not-merge-safe reasons,
operator assignments, frontier lag, state budget, parallelism, memory usage, workload
attribution, and more.

This is a firehose. A developer running `EXPLAIN INCREMENTAL` for the first time should
not need to understand antichain frontiers, merge law registries, or RMW-avoidance
ratios to answer the question "why is my view slow?"

The design commits to being "unable to surprise you," which means the default output
must be readable in under 30 seconds by a developer who is not a RockStream internals
expert. The detailed output should still be available — just not as the default.

---

## Decision

`EXPLAIN INCREMENTAL` has three levels, each building on the previous:

### Level 1 — Default (human-readable summary)

```sql
EXPLAIN INCREMENTAL reporting.daily_summary;
```

```
Materialized View: reporting.daily_summary
  Workload:   batch_analytics  (inherited from schema)
  State:      READY
  Freshness:  lag 3s, SLO 10s  ✓
  State size: 42 GB / 100 GB budget

  Operators:
    HashAggregate  region → SUM(amount), COUNT(*)
    TableScan      reporting.orders

  Upstream sources:
    kafka_orders  lag 1s, offset current
```

The goal: a developer can answer "is this view healthy?" in one glance. No merge law
IDs, no antichain notation, no internal system identifiers.

### Level 2 — VERBOSE (full plan with resource detail)

```sql
EXPLAIN INCREMENTAL VERBOSE reporting.daily_summary;
```

Adds to the default output:

```
  Operators:
    HashAggregate  region → SUM(amount), COUNT(*)
      merge-safe:   yes  (SumCount/v1)
      combiner:     enabled (reduces shuffle ~60%)
      state:        8 GB across 4 shards
      parallelism:  4 / 8 workers

    TableScan      reporting.orders
      merge-safe:   n/a (source operator)
      shard-local:  yes
      rows/s:       ~42,000

  Workload detail:
    batch_analytics: priority=low, MAX_PARALLELISM=8, MEMORY_LIMIT=100GB
    Memory used: 61 GB / 100 GB (61%)

  Frontier:
    reporting.daily_summary: T=2026-05-29T09:14:22.003Z
    kafka_orders:             T=2026-05-29T09:14:22.001Z  (lag 2ms)
```

This level is for operators diagnosing a resource or performance issue.

### Level 3 — ANALYZE (live runtime statistics)

```sql
EXPLAIN INCREMENTAL ANALYZE reporting.daily_summary;
```

Adds live per-operator statistics collected over the last 60 seconds:

```
  Operators:
    HashAggregate  region → SUM(amount), COUNT(*)
      rows processed:  1.2M / 60s  (20k/s)
      state reads:     840k / 60s  (14k/s)
      RMW avoided:     92% via SumCount/v1
      hot groups:      ['us-east', 'eu-west'] (top 2 by write volume)
      p99 latency:     4ms

    TableScan      reporting.orders
      rows scanned:    1.2M / 60s
      decode errors:   0
      DLQ entries:     0
```

This level is for diagnosing hot paths, skew, and throughput bottlenecks. It requires
a live round-trip to workers and will take slightly longer than the other levels.

### Level 4 — ESTIMATE (cost preview before creation)

```sql
EXPLAIN INCREMENTAL ESTIMATE
  CREATE MATERIALIZED VIEW reporting.daily_summary AS SELECT ...;
```

Already specified in the first-run-ergonomics ADR. Included here for completeness:

```
  Backfill estimate:
    Source rows:   2.4 billion  (3 upstream tables)
    State size:    ~18 GB
    Time at current parallelism: ~4 hours
    Confidence: medium (source statistics are 2 days old)
```

---

## Default Output Rules

The default level (`EXPLAIN INCREMENTAL`) must follow these rules:
- No internal IDs (operator IDs, shard IDs, law IDs)
- No antichain notation
- No raw byte counts (use human-readable units: GB, MB)
- No unexplained acronyms
- Freshness lag shown as a human duration, not a timestamp delta
- ✓ / ⚠ / ✗ visual indicators for SLO compliance

VERBOSE and ANALYZE are explicitly labelled in output headers so users know what level
they are reading.

---

## Consequences

### What gets better

- New users get useful output immediately without reading internals documentation.
- Experienced operators still have full access to law IDs, frontier antichains, and
  per-operator statistics.
- The three levels create a natural debugging workflow: default → VERBOSE → ANALYZE.

### What we accept

- Maintaining three output renderers adds code surface. The internal data model is the
  same; only the presentation differs.
- ANALYZE requires a live worker round-trip. It should document its own latency
  overhead in the output header.

---

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| Single verbose output always | Overwhelming for new users; violates "unable to surprise you." |
| VERBOSE as the default, simple as opt-in | Backwards. Safe defaults should be the simple case. |
| Separate commands (`EXPLAIN`, `EXPLAIN VERBOSE`) | Inconsistent with the `EXPLAIN INCREMENTAL` namespace already established in the design. |
