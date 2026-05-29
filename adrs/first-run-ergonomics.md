# ADR: First-Run Ergonomics — Zero Friction to First Live View

**Status**: Proposed  
**Date**: 2026-05-29

---

## Context

RockStream's architecture is sound, but getting to a working materialized view requires
several non-obvious steps: stand up a Kafka source, create a schema, optionally create
a workload, then create the view. None of those steps are hard, but their combined
weight is real friction before the user has seen the system do anything useful.

Peer systems have solved this in different ways. RisingWave ships a built-in row
generator. Materialize has a quickstart with a synthetic data source. Both let a user
see incremental results within five minutes of installing the binary.

The goal of this ADR is to make RockStream's "first five minutes" require zero external
dependencies and zero prior configuration.

---

## Decision

### 1. Built-in row generator source

RockStream ships a `GENERATE ROWS` source that emits synthetic rows at a configurable
rate. It requires no external Kafka broker, no S3 bucket, and no schema setup. It
exists purely to let users explore IVM locally.

```sql
-- Emit 100 rows per second with auto-incrementing id, random amount, and current time
CREATE SOURCE demo.orders FROM GENERATE ROWS AS (
  id     SERIAL,
  amount FLOAT   DEFAULT random() * 100,
  region TEXT    DEFAULT pick('us-east', 'eu-west', 'ap-south'),
  ts     TIMESTAMP DEFAULT now()
) RATE = 100 PER SECOND;

-- Immediately usable:
CREATE MATERIALIZED VIEW demo.regional_totals AS
  SELECT region, SUM(amount) AS total
  FROM demo.orders
  GROUP BY region;
```

The generator is a first-class source connector (implements the `Source` trait), not a
special-cased hack. This means it exercises the full IVM pipeline in tests and demos.

**What it does not do**: it is not suitable for production use. The docs should say so
clearly. Its only purpose is local development and exploration.

### 2. Every required step produces a useful default when omitted

A user should be able to run:

```sql
CREATE MATERIALIZED VIEW my_view AS SELECT 1 AS n;
```

With no prior schema, no workload, and no source — and have it succeed. Specifically:

- If no schema is specified, the view goes into `public` (matching Postgres convention).
- If no workload is specified, the view inherits the system default workload.
- If the `public` schema does not exist yet, it is created automatically on first use.

This means the minimum viable RockStream session is:

```sql
CREATE SOURCE orders FROM KAFKA ...;
CREATE MATERIALIZED VIEW order_summary AS SELECT ...;
```

No `CREATE SCHEMA`, no `CREATE WORKLOAD`, no `SET` commands required.

### 3. Cost preview before long DDL

When `CREATE MATERIALIZED VIEW` is issued against a large existing dataset, the system
estimates the backfill cost and prompts before proceeding:

```
rockstream=> CREATE MATERIALIZED VIEW reporting.daily_summary AS SELECT ...;

⚠  Backfill estimate: 2.4 billion rows across 3 sources.
   Estimated time:  ~4 hours at current parallelism.
   Estimated state: ~18 GB.
   Run EXPLAIN INCREMENTAL ESTIMATE for details.
   Proceed? [y/N]
```

In non-interactive mode (piped SQL scripts, CI), the prompt is suppressed and execution
proceeds. Users can also suppress it explicitly:

```sql
CREATE MATERIALIZED VIEW reporting.daily_summary
  WITHOUT CONFIRMATION AS SELECT ...;
```

The `EXPLAIN INCREMENTAL ESTIMATE` command always works without side effects:

```sql
EXPLAIN INCREMENTAL ESTIMATE
  CREATE MATERIALIZED VIEW reporting.daily_summary AS SELECT ...;
```

This produces a cost breakdown without creating anything.

---

## Consequences

### What gets better

- A new user can have a working materialized view in under two minutes with zero
  external dependencies.
- The generator source doubles as a reproducible test fixture for operators writing
  integration tests.
- Large-scale DDL no longer silently kicks off hours of backfill work.
- Onboarding documentation becomes dramatically simpler: "run this one SQL block."

### What we accept

- The generator source adds maintenance surface. It must evolve alongside the connector
  contract.
- The cost prompt adds a round-trip in interactive sessions. The `WITHOUT CONFIRMATION`
  escape hatch handles automation.
- The cost estimate is inherently approximate — source statistics may be stale or
  unavailable. The estimate should say so, with a confidence indicator.

---

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| Skip the generator; document Kafka local setup | Requires Docker, a running broker, and topic creation. This is a 30-minute setup before IVM is visible. |
| Only add the generator, skip cost preview | Users will still kick off accidental 4-hour backfills. The preview is cheap insurance. |
| Make the prompt opt-in via a session flag | Opt-in safety features have near-zero adoption. Default-on with an escape hatch is the right posture. |
