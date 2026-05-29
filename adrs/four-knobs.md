# ADR: The Four Knobs — User-Facing Intent Declaration

**Status**: Accepted  
**Date**: 2026-05-29

---

## Context

DESIGN.md v3.3 commits to being "SLO-driven, not knob-driven" and states:

> "Operators set intent (SLOs, quotas, priorities); the control plane decides mechanism."
> Manual knobs remain as overrides, not primary controls.

And later: "four knobs."

However, the four knobs are never enumerated anywhere in the design. The auto-tuner,
the workload system, and `CREATE MATERIALIZED VIEW` all reference them obliquely, but a
user reading the documentation cannot find a single canonical list of what they are
allowed to configure and what the system manages on their behalf.

This ADR names the four knobs, specifies their syntax, and draws a clear line between
what users declare and what the system decides.

---

## Decision

### The Four Knobs

Users declare **intent**. The system decides **mechanism**. The four intent knobs are:

| Knob | What you're saying | What the system does |
|---|---|---|
| `FRESHNESS_SLO` | "Keep this view within N seconds of real time" | Tunes epoch size, parallelism, scheduling priority automatically |
| `STATE_BUDGET` | "Don't use more than N GB of storage for this view's state" | Controls operator state compaction, arrangement eviction, spill policy |
| `WORKLOAD` | "Run this view under these resource constraints" | Schedules within workload limits; shares workers across views in the workload |
| `PRIORITY` | "This view matters more / less than others" | Influences scheduler ordering when workload workers are contended |

These four knobs — and only these four — are what a user needs to specify. Everything
else (worker count, epoch timing, buffer sizes, compaction thresholds, merge-law
selection) is decided by the system and reflected back to the user via `EXPLAIN
INCREMENTAL` and diagnostics.

### Syntax

All four knobs are valid in `CREATE MATERIALIZED VIEW`:

```sql
CREATE MATERIALIZED VIEW reporting.daily_summary
  WITH (
    FRESHNESS_SLO = '5 seconds',   -- how fresh must this view be?
    STATE_BUDGET  = '20 GB',       -- how much storage is allowed?
    WORKLOAD      = 'realtime',    -- which resource policy applies?
    PRIORITY      = 'high'         -- relative importance under contention
  )
AS SELECT region, SUM(amount) FROM orders GROUP BY region;
```

All four are optional with sensible defaults:

| Knob | Default | Notes |
|---|---|---|
| `FRESHNESS_SLO` | `'30 seconds'` | Tunable system-wide default via `ALTER SYSTEM` |
| `STATE_BUDGET` | `'unlimited'` | Bounded by workload `MEMORY_LIMIT` at runtime |
| `WORKLOAD` | schema default, then system default | See workloads ADR |
| `PRIORITY` | `'normal'` | Overrides workload priority for this view specifically |

They can be updated after creation:

```sql
ALTER MATERIALIZED VIEW reporting.daily_summary
  SET FRESHNESS_SLO = '10 seconds',
      STATE_BUDGET  = '30 GB';
```

### SLO semantics

`FRESHNESS_SLO` is a **p99 target**, not a hard guarantee. The system will tune itself
to meet it under normal load. Under overload or after a failure/recovery, the SLO may
be breached temporarily. When it is:

- A named diagnostic reason is surfaced (`RS-4023`: SLO breach, with cause)
- The auto-tuner increases parallelism or epoch frequency to recover
- The breach duration is tracked in metrics (`view_freshness_slo_breach_seconds`)
- The user is not paged unless they have set up an alert; the system recovers
  automatically where possible

`FRESHNESS_SLO = '0'` is not valid. Synchronous / IMMEDIATE mode is an explicit
non-goal (DESIGN.md §1.1).

### STATE_BUDGET semantics

`STATE_BUDGET` is a **soft limit**. Approaching the budget:
- Triggers more aggressive compaction and arrangement eviction
- Surfaces a warning: `RS-5020: STATE_BUDGET_WARNING: 85% consumed`

Exceeding the budget:
- The view enters `DEGRADED` state
- Incoming updates are buffered up to a maximum buffer window, then dropped with `RS-5021`
- The user must either raise the budget or reduce the view's state requirements

`STATE_BUDGET = 'unlimited'` opts out of enforcement (subject to workload
`MEMORY_LIMIT`).

### What is NOT a knob

To avoid knob proliferation, the following are explicitly **not user-configurable**:
- Epoch size (auto-tuned to meet FRESHNESS_SLO)
- Worker thread count (auto-tuned within workload MAX_PARALLELISM)
- Compaction thresholds (auto-tuned within STATE_BUDGET)
- Merge law selection (determined by the planner, not the user)
- Shard placement (determined by the control plane)

Manual overrides exist for some of these (DESIGN.md §14) but are operator-facing
emergency levers, not part of the primary user API surface.

---

## Consequences

### What gets better

- Users have a single, canonical answer to "what can I configure on a view?" — four
  things, clearly named with clear semantics.
- The design's "SLO-driven" promise has a concrete, testable form.
- The auto-tuner has a clear optimization target: drive freshness lag below
  `FRESHNESS_SLO` while staying within `STATE_BUDGET` and `WORKLOAD` limits.

### What we accept

- FRESHNESS_SLO as a p99 target means users cannot demand hard latency guarantees.
  The documentation must be explicit about this. Users who need hard guarantees need
  `DEDICATED CLUSTER` (see Future Work in workloads ADR).
- Fixing four knobs means future knobs must clear a high bar to be added. That is a
  feature, not a bug.

---

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| Leave knobs unspecified, document per-feature | Already tried (DESIGN.md v3.1 through v3.22). The knobs are scattered. Users cannot find them. |
| Five knobs (add an explicit PARALLELISM knob) | Parallelism is mechanism, not intent. The system should own it. Users who truly need to override it can use the workload's MAX_PARALLELISM. |
| Make FRESHNESS_SLO a hard guarantee | Requires synchronous IVM — an explicit non-goal. p99 with named breach states is the correct model for an async streaming system. |
