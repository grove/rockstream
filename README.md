# RockStream

**Keep your reports and dashboards up-to-date — automatically, instantly, and at any scale.**

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
Your dashboards and reports instantly reflect the new reality
```

No full re-scans. No waiting. No stale data.

## What Makes RockStream Different

### Built on Cloud Storage

RockStream stores everything in object storage — the same kind of storage that powers
services like Amazon S3 or Google Cloud Storage. This means:

- **Bottomless capacity**: your data can grow without limits.
- **High durability**: your data is replicated automatically and won't disappear.
- **Low cost**: object storage is far cheaper than running dedicated database servers.

The underlying storage engine is [SlateDB](https://slatedb.io/), a modern database built
from the ground up for the cloud era.

### Scales With Your Workload

RockStream splits the work across as many workers as you need. Each worker is responsible
for a slice of the data. When traffic grows, you add more workers. When it quiets down,
you remove them. Workers coordinate automatically — no manual reconfiguration required.

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

RockStream is inspired by three production systems:

| System | What it does |
|---|---|
| **[Feldera](https://feldera.com/)** | Uses mathematical theory (DBSP) to guarantee that incremental results are always identical to what a full re-computation would produce |
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
| **Watermark** | A marker that says "all changes up to this point have been processed" |
| **Checkpoint** | A saved snapshot of progress so the system can recover after a crash |
| **CDC** | Change Data Capture — streaming the changes out to other systems |

## Status

This project is in the **design phase**. The [DESIGN.md](DESIGN.md) document contains
the full technical architecture, including the storage layout, operator state design,
worker coordination protocol, and fault-tolerance strategy.

## Contributing

Contributions, feedback, and questions are welcome. Open an issue or start a discussion!

## License

Apache 2.0
