# RockStream

**Keep your reports and dashboards up-to-date automatically as your workload grows.**

---

## The Problem

Imagine you run a business and you have a dashboard that shows today's sales, inventory
levels, and customer activity. Every time someone asks "what's the current total?", your
system has to go away, dig through all the historical data, add everything up from scratch,
and come back with an answer. If you have millions of records, that can take seconds — or
minutes. And the moment it finishes, the data is already slightly out of date.

This is how most traditional databases work. It's like tearing down and rebuilding a
scoreboard from scratch every time a player scores a point.

## The Solution

RockStream keeps a *live*, pre-computed answer for every question you care about. When
new data arrives — a new sale, a sensor reading, a customer action — RockStream figures
out *only what changed* and updates the answer in the blink of an eye. The scoreboard
stays up, and only the relevant numbers tick.

This technique is called **Incremental View Maintenance** (IVM). Instead of recomputing
everything, RockStream processes only the *difference* — the new or deleted records —
and applies it to the existing result. Think of it as keeping a running total rather than
re-adding every number on every receipt every time.

## How It Works (Simply)

```
New data arrives
      │
      ▼
RockStream figures out what changed (the "delta")
      │
      ▼
It updates only the affected parts of your pre-computed results
      │
      ▼
Your dashboards and reports quickly reflect the new reality
```

No routine full re-scans. Less waiting. Fresher data.

## What Makes RockStream Different

### Built on Cloud Storage

RockStream stores everything in object storage — the same kind of storage that powers
services like Amazon S3 or Google Cloud Storage. This means:

- **Bottomless capacity**: your data can grow without limits.
- **High durability**: your data is replicated automatically and won't disappear.
- **Low cost**: object storage is far cheaper than running dedicated database servers.

The underlying storage engine is [SlateDB](https://slatedb.io/), a modern database built
from the ground up for the cloud era.

### Scales With Your Workload — Without Touching Knobs

RockStream splits the work across as many workers as you need. Each worker is responsible
for a slice of the data. When traffic grows, the system adds parallelism to the slow
operators automatically. When it quiets down, it reduces it. Workers coordinate
automatically — no manual reconfiguration required.

You tell RockStream *what* you want — "keep this view fresh within 1 second; do not
exceed 200 GB of state" — and the system figures out *how*: how many shards each
operator needs, how often to commit to object storage, when to scale up or back. The
mechanism knobs are still there if you need them, but you should not need to reach for
them first.

The design is intentionally honest about the limits of scale: object-storage request
rates, skewed keys, network shuffle, and source/sink throughput still matter. RockStream's
goal is to remove the single database writer as the central bottleneck by splitting state
across many independent SlateDB-backed shards, and to give you a clear, named reason
when any of those underlying limits become the binding constraint.

### Never Loses Work

If a worker crashes, another one picks up exactly where it left off. Every step is
carefully saved in a way that allows the system to restart without re-processing anything
it already handled, and without skipping anything.

### Speaks SQL

You define what you want to track using ordinary SQL queries — the same language used in
spreadsheets and most databases. Write a query like:

```sql
SELECT product_id, SUM(quantity) AS total_sold
FROM orders
GROUP BY product_id
```

…and RockStream will maintain the answer to that query continuously, as new orders
arrive.

## Inspiration

RockStream is inspired by production systems and implementation research:

| System | What it does |
|---|---|
| **[Feldera](https://feldera.com/)** | Uses mathematical theory (DBSP) to guarantee that incremental results are always identical to what a full re-computation would produce |
| **pg_trickle** | Shows how to turn SQL views into practical per-operator delta rules, with many hard correctness cases worked through in PostgreSQL |
| **[SlateDB](https://slatedb.io/)** | Provides the cloud-native object-storage-backed LSM that RockStream uses as its durable shard and arrangement store |
| **[RisingWave](https://risingwave.com/)** | A streaming database that maintains materialized views in real time |
| **[Snowflake Dynamic Tables](https://docs.snowflake.com/en/user-guide/dynamic-tables-intro)** | Automatically refreshed tables inside the Snowflake cloud data warehouse |

RockStream brings these ideas to an open, cloud-native storage foundation.

## Key Concepts (Jargon-Free)

| Term | Plain-English Meaning |
|---|---|
| **Materialized view** | A saved, pre-computed answer to a query |
| **Delta / change** | Only the new or removed records since the last update |
| **Epoch** | A small batch of changes processed together, like a transaction |
| **Worker** | A process that handles one slice of the data |
| **Frontier** | A marker that says "no future change before this point is expected"; this is the distributed version of a watermark |
| **Checkpoint** | A saved snapshot of progress so the system can recover after a crash |
| **CDC** | Change Data Capture — streaming the changes out to other systems |

## Status

This project is in the **design phase**. Three documents describe the system
in progressively more detail:

| Document | Audience | What it covers |
|---|---|---|
| [DESIGN.md](DESIGN.md) | Engineers / architects | Full system architecture: storage layout, operator state, worker coordination, fault tolerance, scaling model, deployment ladder, operational guide |
| [IVM.md](IVM.md) | IVM specialists | How the incremental-view-maintenance engine itself works — DBSP-native operators, the differentiation pass, the circuit runtime, arrangements on SlateDB, and pg_trickle as a correctness oracle |
| [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) | Implementers | Phase-by-phase build plan from empty repo to GA, including corrected IVM milestones and validation gates |

## How Do I Know It’s Working?

The system exposes one primary health indicator per pipeline: **SLO compliance** —
the fraction of the recent window for which your declared freshness target was met.

- **SLO compliance = 1.0**: pipeline is healthy.
- **SLO compliance < 1.0**: a `degraded_reason` label says *why* in plain terms
  (`BACKFILLING`, `RECOVERING`, `OVER_BUDGET_RELAXED`, `RPS_THROTTLED`,
  `BLOCKED`, etc.) so you know whether to wait, add capacity, raise a quota,
  or fix a connector.
- **`rockstream explain <view>`**: shows the operator tree with per-operator
  diagnostics and what the auto-tuner is currently doing.
- **`rockstream explain <view> --estimate`**: previews cost, state size, and
  achievable freshness *before* you deploy a new view.
- **Freshness tokens**: query responses can include the source frontier they
  observed; clients that need read-your-writes behavior can ask the gateway to
  wait until that frontier is visible.
- **`rockstream support bundle --pipeline=foo`**: collects everything needed to
  debug an issue in one command.
- **`rockstream audit tail`**: shows every control-plane action — every scale
  decision, every degraded-state transition, every pipeline change — with the
  metric reading that triggered it. No silent changes.

A Grafana dashboard template ships with the project at
`deploy/dashboards/rockstream-overview.json`. The above-the-fold panel is a single
number per pipeline: SLO compliance over time.

## One Binary, One Config, Three Tiers

There is one `rockstream` binary. Node roles are flags, not separate executables.

- **Laptop / evaluation**: `rockstream start --storage=./data` — zero config; survives crashes.
- **Single host (small production)**: `rockstream start --role=all --storage=s3://bucket/...`.
- **Multi-host cluster**: `--role=control` on control nodes, `--role=worker` on workers.

Moving up the ladder is additive: the same data files produced at Tier 1 against MinIO
open at Tier 3 against S3. There is no data migration step because there is no
node-local state to migrate.

## Contributing

Contributions, feedback, and questions are welcome. Open an issue or start a discussion!

## License

Apache 2.0
