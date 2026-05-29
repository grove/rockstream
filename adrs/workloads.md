# ADR: Drop `CREATE PIPELINE` as a User-Facing Construct; Introduce Workloads

**Status**: Proposed  
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
- Resource quotas can be attached to a schema or workload, not a pipeline container.
- Source ownership and lifecycle (pause/resume) can operate on individual views or on
  a schema.

The core problem with `CREATE PIPELINE` is that users must answer *"which pipeline does
this view belong to?"* before they can create anything. That's friction before the
system has demonstrated any value.

A survey of peer systems informed the naming choice:

| System | Construct | Model |
|---|---|---|
| Materialize | `CREATE CLUSTER` | Allocate fixed capacity per cluster |
| RisingWave | Resource Group (node flag) | Assign physical nodes to groups |
| Snowflake | `CREATE WAREHOUSE` | Allocate fixed capacity per warehouse |
| RockStream | `CREATE WORKLOAD` | Constrain max capacity via policy |

The term **Workload** was chosen over "Resource Group" (already used by RisingWave to
mean a pool of physical nodes, which is different) and "Cluster" (too closely associated
with allocation-based models). A workload is a *named policy* defining resource
constraints and scheduling priority — not a running operation and not a provisioned
compute pool.

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

Schemas can carry a default workload (see below), so all views inside inherit the
same resource policy by default.

### 3. Workloads are first-class objects

A Workload defines a named set of runtime constraints. Multiple schemas (and individual
views) can share the same workload.

```sql
CREATE WORKLOAD batch_analytics
  WITH
    MAX_PARALLELISM = 8,
    MEMORY_LIMIT    = '100GB',
    PRIORITY        = 'low';
```

#### What can be configured on a Workload

Start small. The initial surface covers the knobs that matter for the vast majority of
workloads:

| Property | Description |
|---|---|
| `MAX_PARALLELISM` | Maximum number of worker threads assigned to this workload |
| `MEMORY_LIMIT` | Soft memory cap before spilling; hard cap triggers backpressure |
| `PRIORITY` | Scheduling priority: `high`, `normal` (default), or `low` |

Further properties (I/O rate limits, checkpoint intervals, idle auto-pause, etc.) can be
added later when real demand justifies them.

### 4. Resolution order for workload assignment

A view's effective workload is resolved in this order:

1. **View-level override** — highest priority
2. **Schema default** — fallback when no view-level override is set
3. **System default workload** — used when neither is set

```sql
-- Schema sets the default for all its views
CREATE SCHEMA reporting
  WITH DEFAULT_WORKLOAD = 'batch_analytics';

-- This view inherits 'batch_analytics'
CREATE MATERIALIZED VIEW reporting.daily_summary AS ...;

-- This view opts into a different workload
CREATE MATERIALIZED VIEW reporting.critical_metric AS ...
  WITH WORKLOAD = 'realtime';
```

Schema-level defaults are always surfaced to the user. `SHOW CREATE MATERIALIZED VIEW`
will display the effective workload — including inherited ones — so silent inheritance
is never invisible. `EXPLAIN INCREMENTAL` will include a line: `executing in workload
'batch_analytics'`.

### 5. Cardinality rules

| Relationship | Cardinality |
|---|---|
| One view → one workload | Exactly 1 (required) |
| One schema → one default workload | 0 or 1 (optional) |
| Multiple schemas → one workload | N schemas : 1 workload (allowed) |

A view belongs to exactly one workload at any moment. This avoids ambiguity when limit
conflicts arise. Reassignment is done via `ALTER MATERIALIZED VIEW ... WITH WORKLOAD =
'...'`.

### 6. Per-view lifecycle control

Lifecycle operations (pause, resume) work at both the view level and the schema level,
independently of workload membership:

```sql
-- Pause one view
ALTER MATERIALIZED VIEW reporting.daily_summary PAUSE;

-- Pause all views in a schema
ALTER SCHEMA reporting PAUSE;
```

This means a user does not need to reason about workloads to stop or resume individual
views. Workload membership is about resource policy, not operational lifecycle.

### 7. Observability

The following system catalog queries give users full visibility into workload
assignments:

```sql
-- List all defined workloads and their limits
SELECT * FROM rockstream_catalog.workloads;

-- Show which views are using a specific workload
SELECT schema_name, view_name, workload, workload_source
FROM rockstream_catalog.materialized_views
WHERE workload = 'batch_analytics';
-- workload_source: 'view' | 'schema_default' | 'system_default'

-- Show effective workload for every view, including how it was assigned
SELECT schema_name, view_name, workload, workload_source
FROM rockstream_catalog.materialized_views
ORDER BY schema_name, view_name;
```

The `workload_source` column makes inheritance explicit, preventing the confusion of
"why is this view using the batch workload?".

---

## Consequences

### What gets simpler

- **Onboarding**: Users create sources and views. No ceremony around pipeline
  declarations.
- **Mental model**: Schemas and views map directly to familiar SQL concepts. Workloads
  are an optional operational concern that beginners can ignore entirely.
- **Lifecycle operations**: Pause, resume, and replace work on individual views or on an
  entire schema, independently of workload membership. No need for a pipeline wrapper.

### What stays the same internally

- The engine still compiles co-located views into a shared operator DAG. This optimisation
  is unchanged; it is just no longer surfaced as user syntax.
- SLO targets, freshness tokens, and quota enforcement remain first-class runtime
  properties — they are now expressed through Workloads and view-level annotations
  rather than pipeline DDL.

### Tradeoffs accepted

- A user who wants to reason about "all the compute doing my analytics work" must do so
  via a Workload name, not a pipeline name. This is a deliberate trade: less ceremony up
  front, slightly more indirection for operational queries. We judge this a net
  improvement.
- Schema-level lifecycle (e.g., `ALTER SCHEMA reporting PAUSE`) requires that all views
  in the schema are paused atomically. The control plane must implement this as a
  multi-view transaction; this is a bounded implementation cost.
- The constraint model (max capacity) provides soft isolation, not hard fault isolation.
  Two views in different workloads still share the same underlying worker pool. Users
  with critical-path workloads must be aware of this limitation (see Future Work below).

---

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| Keep `CREATE PIPELINE` as optional sugar | Still forces the concept on users who read docs or examples. Optional constructs have a way of becoming required in practice. |
| Hierarchical workloads (parent/child) | Adds complexity without clear benefit at the current scale of the system. Can be revisited. |
| Tags / labels instead of schemas for grouping | Too loose. Schemas provide namespace isolation, access control, and default inheritance in one concept. Tags are complementary, not a replacement. |
| Name it `RESOURCE GROUP` | Already used by RisingWave to mean a pool of physical nodes — a fundamentally different concept. Would confuse users migrating from RisingWave. |
| Name it `CLUSTER` | Too closely associated with Materialize's allocation-based model, where a cluster is a provisioned, always-on compute pool with a per-second cost. |

---

## Future Work

### Dedicated compute for critical workloads

The constraint model (a Workload is a policy, not a pool) means all views share the
same underlying worker infrastructure. This is by design for the common case, but it
does not satisfy users who need hard fault isolation — for example, a payments
processor that cannot tolerate noisy-neighbour interference from a batch analytics job.

If demand justifies it, a `DEDICATED CLUSTER` construct can be layered on top:

```sql
CREATE DEDICATED CLUSTER payments_cluster
  WITH SIZE = 'medium', REPLICATION_FACTOR = 2;

CREATE WORKLOAD payments
  WITH CLUSTER = 'payments_cluster', PRIORITY = 'high';
```

This would provide Materialize-style hard isolation for workloads that require it,
while keeping the default path (shared worker pool + soft constraints) simple and
cost-efficient. This is not part of the current design; it is noted here to ensure
the schema evolution path is not accidentally closed off.

### Cross-schema view tagging and bulk operations

The current design supports per-view and per-schema lifecycle operations (pause, resume).
However, users may want to group related views across multiple schemas for bulk operations
without moving them into a shared schema.

A tagging system would enable this:

```sql
CREATE MATERIALIZED VIEW reporting.daily_summary AS
  SELECT ...
TAGS ('critical', 'sla_sensitive', 'pii');

CREATE MATERIALIZED VIEW data_science.model_input AS
  SELECT ...
TAGS ('sla_sensitive', 'batch_only');

-- Later, bulk operations across schema boundaries:
ALTER VIEWS WITH TAG 'critical' SET PRIORITY = 'high';
ALTER VIEWS WITH TAG 'critical' PAUSE;
ALTER VIEWS WITH TAG 'pii' REQUIRES ENCRYPTION;
```

This would be orthogonal to workloads and schemas, allowing more flexible operational
groupings. Tags would be persisted as first-class metadata and queryable via catalog
views. This is deferred to a follow-on design to keep the current scope focused on
workloads as the primary resource management construct.
