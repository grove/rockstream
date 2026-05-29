# RockStream Concepts Guide — Table of Contents

A friendly, in-depth tour of how RockStream thinks about data, time, change,
and consistency. Written for people who want to understand the system without
wading through implementation details.

---

## Part 1 — The Big Picture

1. **What RockStream Is, In Plain Words**
   The problem of stale dashboards. Why incremental view maintenance is
   different from rerunning queries. RockStream's place between Postgres,
   Materialize, Snowflake, and pg-trickle.

2. **The Mental Model**
   Two nouns and one verb: pipelines, views, deploy. What you, the user,
   are responsible for, and what the system handles for you.

3. **A Tour Through a Single View**
   Following one row from the moment it lands in a source table to the moment
   it shows up in a materialized view. Where it goes, what touches it, and
   why each step exists.

## Part 2 — How Data Moves

4. **Sources, Sinks, and Connectors**
   Where data comes from and where it ends up. The connector contract in
   everyday language. Kafka, Postgres, S3, internal writes — they all look
   the same to the engine.

5. **Deltas and Z-sets**
   Why RockStream thinks in *changes* rather than *snapshots*. The simple
   trick of attaching a +1 or -1 to every row. How this turns expensive
   recomputations into cheap arithmetic.

6. **Epochs: Batches of Work**
   What an epoch is. Why batching matters. How epoch size is chosen and what
   the trade-offs are. The atomic commit guarantee that makes epochs feel
   like transactions.

## Part 3 — How the System Stays in Sync

7. **Frontiers: Telling Other Operators You're Done**
   The simplest definition: a frontier is a promise. How operators read each
   other's frontiers to know when it's safe to proceed. Why this replaces
   locks, two-phase commit, and global clocks.

8. **The Frontier Protocol in Action**
   Three worked examples: a join across shards, a chain of views, and a
   diamond dependency. What you see at each step and why the result is
   always consistent.

9. **Causal Time, Not Wall-Clock Time**
   Why RockStream avoids "what time is it?" as a question. The difference
   between event time, processing time, and frontier-time. When you need to
   care, and when you really don't.

## Part 4 — Views and Pipelines

10. **Materialized Views: The Star of the Show**
    What a materialized view is in RockStream terms. How it differs from a
    Postgres materialized view. The storage it consumes, the queries it
    serves, the SLA it offers.

11. **Inline Views: Just Named Queries**
    The other kind of view. When you'd use one. The fact that they don't
    actually exist as stored data, and why that's often what you want.

12. **Pipelines: The Unit of Deployment**
    The container that holds your views, your sources, your sinks, your
    SLOs, your quotas. Why grouping things into a pipeline matters. When
    one pipeline is enough and when you need several.

13. **Composing Views Out of Views**
    Building bigger things out of smaller ones. Chains, fan-outs, joins,
    and the diamond pattern. How RockStream maintains consistency across
    these compositions without breaking a sweat.

## Part 5 — Freshness and Cost

14. **Cadences: How Often Things Update**
    The four options — deferred low-latency, periodic, calculated, and
    immediate — explained with examples. Picking the right one for your
    workload.

15. **SLOs: Telling the System What You Want**
    Freshness target, state budget, object-store budget, priority. The
    intent-based interface that lets you avoid tuning knobs. What happens
    when an SLO can't be met.

16. **Self-Tuning by Default**
    What the system tunes on your behalf — parallelism, epoch sizing,
    placement, throttling. When you should override, and what the
    overrides cost.

17. **The Trade-Offs Triangle**
    Freshness, throughput, and cost. You get to pick two. How RockStream
    helps you make the choice consciously rather than by accident.

## Part 6 — When Things Get Interesting

18. **Diamond Consistency Without 2PC**
    The classic problem: two views feed a third. How RockStream guarantees
    the third sees a consistent snapshot without a distributed transaction.
    Why this is the secret sauce.

19. **IMMEDIATE Mode and What It Means in a Distributed World**
    Why pg-trickle's IMMEDIATE doesn't generalize. The restricted form
    RockStream can support. What you should reach for instead when you
    want strong freshness.

20. **Bulk Loads and Source Gating**
    The everyday operational problem of loading a million rows without
    triggering a million refreshes. How RockStream handles it through
    credits and gates. The PAUSE / RESUME pattern.

21. **Reading Historical Data**
    Time travel: `AS OF EPOCH`, `AS OF TIMESTAMP`. The retention window.
    Cold-tier snapshots for ad-hoc analytics. When you'd reach for each.

22. **Recursive Queries and Graphs**
    Transitive closure, hierarchical aggregation, graph traversal. Why
    recursion is harder than ordinary joins. The incremental fixed-point
    machinery that makes it work.

## Part 7 — Operations Without Drama

23. **The One Signal: SLO Compliance**
    The single number that tells you whether things are healthy. What it
    looks like in Grafana. The named degradation reasons that tell you
    what to do when it dips.

24. **Failure and Recovery**
    What happens when a worker dies. How the system fences and re-leases
    shards. Recovery time bounds and what they mean for your dashboards.

25. **Scaling Up, Scaling Down, Scaling Out**
    The same binary, three tiers: laptop, single host, cluster. Moving
    between them is additive, not a migration. The deployment ladder.

26. **Multi-Tenancy: Namespaces and Quotas**
    Running many independent tenants on one cluster. How namespaces isolate
    them. Quotas as guardrails.

## Part 8 — Reference Material

27. **Glossary**
    Every term in one place: epoch, frontier, shard, arrangement, pipeline,
    cadence, SLO, quota, watermark, source, sink, connector, namespace,
    coordination group.

28. **Decision Trees**
    "Should I create a separate pipeline?" "Which cadence should I pick?"
    "Do I need IMMEDIATE mode?" Quick visual answers to common questions.

29. **Further Reading**
    Pointers into the design document, the IVM document, the implementation
    plan, and the underlying papers for readers who want to go deeper.

---

## How to Read This Guide

You don't have to read it linearly. Three suggested paths:

- **The 30-minute tour:** chapters 1, 2, 3, 6, 7, 10, 12, 14, 23.
- **The "I need to deploy something" path:** chapters 10, 12, 14, 15, 16, 23.
- **The "I'm debugging weirdness" path:** chapters 7, 8, 11, 18, 20, 23, 24.

Every chapter is self-contained. Cross-references are provided when a concept
builds on something covered elsewhere.
