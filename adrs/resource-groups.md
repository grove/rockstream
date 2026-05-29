# ADR: Drop `CREATE PIPELINE` as a User-Facing Construct; Introduce Resource Groups

**Status**: Accepted  
**Date**: 2026-05-29

---

## Context

The original design exposed `CREATE PIPELINE` as a top-level DDL construct. A pipeline
was a user-declared container that grouped related materialized views, owned their SLO
targets and resource quotas, and provided a lifecycle handle (pause, resume, replace).

During design review, we asked whether this construct carries its weight. Specifically:

- Every peer system (RisingWave, Feldera, Materialize) achieves the same goals without
  exposing pipelines as DDL.
- The optimizer can fuse shared operator DAGs transparently — users don't need to
  declare the grouping.
- Resource quotas can be attached to a schema or resource group, not a pipeline
  container.
- Source ownership and lifecycle (pause/resume) can operate on individual views or on
  a schema.

The core problem with `CREATE PIPELINE` is that users must answer *"which pipeline does
this view belong to?"* before they can create anything. That's friction before the
system has demonstrated any value.

---

## Decision

### 1. Pipelines are an internal concept only

`CREATE PIPELINE` is removed from the user-facing SQL surface. Users never name or
declare pipelines. Internally, the engine still compiles a set of views into a dataflow
program (a "pipeline unit"), but this is an optimizer concern — invisible to the user,
just as query plans are invisible in a traditional database.

The primary DDL construct is:

```sql
CREATE MATERIALIZED VIEW my_view AS
  SELECT ...;
```

Sources are created separately:

```sql
CREATE SOURCE kafka_orders FROM KAFKA ...;
```

### 2. Schemas are the grouping unit

Users organise related views into schemas. A schema is the natural grouping boundary for
naming, access control, and operational defaults.

```sql
CREATE SCHEMA reporting;
CREATE MATERIALIZED VIEW reporting.daily_summary AS ...;
CREATE MATERIALIZED VIEW reporting.weekly_rollup AS ...;
```

Schemas can carry a default resource group (see below), so all views inside inherit the
same resource policy by default.

### 3. Resource Groups are first-class objects

A Resource Group defines a named set of runtime constraints. Multiple schemas (and
individual views) can share the same resource group.

```sql
CREATE RESOURCE GROUP batch_analytics
  WITH
    MAX_PARALLELISM = 8,
    MEMORY_LIMIT    = '100GB',
    PRIORITY        = 'low';
```

#### What can be configured on a Resource Group

Start small. The initial surface covers the knobs that matter for the vast majority of
workloads:

| Property | Description |
|---|---|
| `MAX_PARALLELISM` | Maximum number of worker threads assigned to this group |
| `MEMORY_LIMIT` | Soft memory cap before spilling; hard cap triggers backpressure |
| `PRIORITY` | Scheduling priority: `high`, `normal` (default), or `low` |

Further properties (I/O rate limits, checkpoint intervals, idle auto-pause, etc.) can be
added later when real demand justifies them.

### 4. Resolution order for resource assignment

A view's effective resource group is resolved in this order:

1. **View-level override** — highest priority
2. **Schema default** — fallback when no view-level override is set
3. **System default resource group** — used when neither is set

```sql
-- Schema sets the default for all its views
CREATE SCHEMA reporting
  WITH DEFAULT_RESOURCE_GROUP = 'batch_analytics';

-- This view inherits 'batch_analytics'
CREATE MATERIALIZED VIEW reporting.daily_summary AS ...;

-- This view opts into a different group
CREATE MATERIALIZED VIEW reporting.critical_metric AS ...
  WITH RESOURCE_GROUP = 'realtime';
```

### 5. Cardinality rules

| Relationship | Cardinality |
|---|---|
| One view → one resource group | Exactly 1 (required) |
| One schema → one default resource group | 0 or 1 (optional) |
| Multiple schemas → one resource group | N schemas : 1 group (allowed) |

A view belongs to exactly one resource group at any moment. This avoids ambiguity when
limit conflicts arise. Reassignment is done via `ALTER MATERIALIZED VIEW ... WITH
RESOURCE_GROUP = '...'`.

---

## Consequences

### What gets simpler

- **Onboarding**: Users create sources and views. No ceremony around pipeline
  declarations.
- **Mental model**: Schemas and views map directly to familiar SQL concepts. Resource
  Groups are an optional operational concern that beginners can ignore entirely.
- **Lifecycle operations**: Pause, resume, and replace work on individual views or on an
  entire schema. No need for a pipeline wrapper.

### What stays the same internally

- The engine still compiles co-located views into a shared operator DAG. This optimisation
  is unchanged; it is just no longer surfaced as user syntax.
- SLO targets, freshness tokens, and quota enforcement remain first-class runtime
  properties — they are now expressed through Resource Groups and view-level annotations
  rather than pipeline DDL.

### Tradeoffs accepted

- A user who wants to reason about "all the compute doing my analytics work" must do so
  via a Resource Group name, not a pipeline name. This is a deliberate trade: less
  ceremony up front, slightly more indirection for operational queries. We judge this a
  net improvement.
- Schema-level lifecycle (e.g., `ALTER SCHEMA reporting PAUSE`) requires that all views
  in the schema are paused atomically. The control plane must implement this as a
  multi-view transaction; this is a bounded implementation cost.

---

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| Keep `CREATE PIPELINE` as optional sugar | Still forces the concept on users who read docs or examples. Optional constructs have a way of becoming required in practice. |
| Hierarchical resource groups (parent/child) | Adds complexity without clear benefit at the current scale of the system. Can be revisited. |
| Tags / labels instead of schemas for grouping | Too loose. Schemas provide namespace isolation, access control, and default inheritance in one concept. Tags are complementary, not a replacement. |
