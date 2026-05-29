# RockStream Concepts Guide

A friendly tour of how RockStream thinks about data, time, change, and
consistency — written for people who want to understand the system without
wading through implementation details.

---

## Part 1 — The Big Picture

### 1. What RockStream Is, In Plain Words

Imagine you have a dashboard that shows the total revenue per region for an
online store. Every time someone places an order, that dashboard ought to
update. The simplest way to make this happen is to ask the database the same
question over and over again: *"What is the total revenue per region right
now?"* The database obediently scans every order, groups them by region, sums
the amounts, and hands you back the answer. This works fine when you have a
thousand orders. It becomes painful when you have ten million. It becomes
impossible when you have a billion and the dashboard needs to refresh every
second.

There is a better way, and it has a name: *incremental view maintenance*. The
insight is simple. When a single new order arrives, the total for that order's
region goes up by the order's amount. Nothing else changes. So instead of
recomputing the entire answer from scratch, you can take the previous answer
and adjust it by the difference. The work you do is proportional to the
change, not to the size of the underlying data. A million orders coming in
costs you a million tiny adjustments rather than a single enormous scan.

RockStream is a system that does this for you, automatically, at scale. You
write SQL that describes what answer you want. RockStream figures out how to
maintain that answer as new data arrives, in a way that is fast, correct, and
runs across many machines without you having to think about which machine
does what. You get continuously fresh materialized views. The system keeps
them fresh by computing only what changed.

What makes RockStream different from the other systems in this space is the
combination it picks. PostgreSQL has materialized views, but you have to
refresh them manually or pay the cost of triggers; it doesn't scale beyond
one machine. Materialize and RisingWave are streaming systems built around
this idea, but they hold a lot of state in memory and have their own runtime
to manage. Snowflake dynamic tables get you a similar feel but live inside a
warehouse with its own pricing model. Pg-trickle adds incremental views to
PostgreSQL itself, which is wonderful for a single-database deployment but
inherits PostgreSQL's single-machine ceiling. RockStream sits at a particular
intersection: it speaks the PostgreSQL wire protocol so familiar tools work,
it stores all its data in cheap object storage like S3, and it shards work
across many machines so it can scale horizontally as your data grows.

The promise, in one sentence: SQL in, fresh views out, on a cluster that
grows with you, paying object-storage prices.

### 2. The Mental Model

When you use RockStream, you only ever interact with three kinds of things and
one verb. The things are **workloads**, **views**, and **sources**. The verb is
**create**. Everything else — sharding, shuffling, recovery, compaction, garbage
collection, rebalancing — happens for you, behind a curtain, and you don't
need to think about it unless something is wrong.

A **view** is a SQL query that has a name. It might be a tiny one-liner that
just selects a subset of a table, or it might be a complicated join with
window functions and recursion. From the outside, a view looks like a table:
you can run `SELECT * FROM my_view` against it just like you would against
`orders` or `customers`. The difference is that the view's contents are
computed from other data, and RockStream keeps those contents fresh as the
underlying data changes.

A **workload** is a named resource policy. It groups together a set of related
views under a shared freshness SLO, memory budget, and priority. It is the
unit that gets a freshness target ("my views should be at most one second
stale"), a resource limit ("don't use more than two hundred gigabytes of
state"), and a priority ("this workload matters more than that one when the
cluster is busy"). When you tell RockStream what you want, you tell it at the
workload level. The system then makes all the small decisions — how many
machines to use, how big to make each batch, when to flush to disk — to honor
what you asked for.

The verb is **create**. You write `CREATE MATERIALIZED VIEW`, you submit it,
and the system goes to work. If you want to change something, you replace it
or use the blue/green replacement path. The mental model deliberately stops
there. You never have to ask "which shard does this row live on?" or "is
operator instance number 47 healthy?" Those questions exist, but they exist
inside diagnostic tools that you only reach for when something has gone wrong.
In normal life, you have workloads and views, and that's it.

### 3. A Tour Through a Single View

Let's follow one row from the moment it lands in a source table to the
moment it shows up in a materialized view. This is the most useful single
exercise for building intuition, so we'll do it slowly and look at each step.

Suppose you have a view defined like this:

```sql
CREATE MATERIALIZED VIEW revenue_by_region AS
  SELECT region, SUM(amount) AS revenue
  FROM   orders
  GROUP  BY region;
```

You also have a connector that brings rows from a Kafka topic called
`orders` into a RockStream-managed table. A new order arrives on that Kafka
topic: region `"NORTH"`, amount `42.00`. What happens next?

First, the **connector** for the orders source reads the new message off
Kafka. The connector knows it's part of an in-flight batch — what RockStream
calls an **epoch**. Maybe the epoch has been open for fifty milliseconds.
The connector decides whether to keep accumulating messages or to close the
current epoch and start a new one. This decision is driven by a cadence: in
this case, suppose the workload runs at a hundred-millisecond cadence, so
the connector waits another fifty milliseconds and then closes the epoch.
The result is a small batch of rows representing all the changes that
happened to the orders source during that hundred-millisecond window.

Next, the batch flows into the **operator graph**. For our view, the graph
is straightforward: scan, group, aggregate, store. The interesting trick is
that the operators don't process the entire orders table. They process only
the changes. The new order in the `"NORTH"` region produces a single delta:
"add 42.00 to the running total for `NORTH`." The aggregate operator reads
its existing state for `NORTH`, applies the delta, and writes the new total
back. If `NORTH` previously had revenue of 1058.00, it now has 1100.00. One
arithmetic operation, no scan of historical orders.

The new total gets written into the view's storage. RockStream stores
materialized view outputs in something called SlateDB, which is a key-value
store designed to live on object storage like S3. The write doesn't go to
S3 immediately; it accumulates in a local write-ahead log, and the entire
epoch's worth of writes — for this view, for other views, for the operator
state, for everything — gets bundled into a single atomic batch and
committed together. This is important: either everything from epoch 42
landed safely, or none of it did. There is no "halfway through epoch 42"
state visible to anyone.

Once the batch commits, the operator publishes a small piece of metadata
called a **frontier**: a promise that says "I have finished epoch 42 and
I will not send you any older updates." Downstream consumers — other
operators, other views, query gateways — read this frontier and know they
can safely process or query data at epoch 42 or earlier.

A user opens a dashboard and asks for `SELECT region, revenue FROM
revenue_by_region`. The query gateway picks up the request, looks at the
latest committed frontier, and reads the view's storage at that frontier.
The `NORTH` row shows revenue of 1100.00. The user sees the updated number.
From the moment the order arrived on Kafka to the moment the dashboard
updated, perhaps two hundred milliseconds passed. No full scans were
performed. Only one row was actually computed.

That, in slow motion, is what RockStream does. The remaining chapters take
each of these steps and dig into why they work the way they do.

---

## Part 2 — How Data Moves

### 4. Sources, Sinks, and Connectors

Data has to come from somewhere and go somewhere. In RockStream, *somewhere*
is always behind an interface called a **connector**. A connector is a small
piece of code that knows how to talk to one specific external system — Kafka,
PostgreSQL via logical replication, S3 as a stream of Parquet files, Iceberg
tables, a simple HTTP webhook, or RockStream's own internal write API for
when clients want to push rows directly. From the engine's point of view,
they all look the same: a connector produces deltas (insertions, deletions)
on the source side, or accepts deltas on the sink side. The engine doesn't
care whether those bytes started life in Kafka or in S3.

This uniformity is more important than it sounds. It means the same view
definition works no matter where the data comes from. If you start with
data in PostgreSQL and later move it to Kafka, your views don't need to
change. If you eventually want to write your view's output back to an
Iceberg table for downstream analytics, you add a sink connector and the
engine handles it.

There are two contracts on top of this idea that are worth knowing about.
The first is the **offset token**. When a connector reads from a source,
it remembers where it is by handing the engine an opaque blob of bytes —
its position. For Kafka, that might be a per-partition offset. For
PostgreSQL, it's a log sequence number. For Iceberg, it's a snapshot ID.
The engine never inspects this blob; it just stores it durably and hands it
back when the connector restarts, so the connector can resume from exactly
where it left off. This is how exactly-once semantics work: replay a
connector from its last committed offset and the engine deduplicates the
re-delivered messages using the same idempotency keys it always uses.

The second contract is **backpressure**. A connector that's reading faster
than the engine can process will eventually run out of buffer space. Rather
than dropping data or running out of memory, the engine asks the connector
to slow down. The connector exposes a "credits available" signal: when
credits are at zero, the connector stops fetching. When the downstream
clears its backlog, credits replenish and the connector resumes. This same
mechanism is what makes features like source gating (covered in chapter 20)
possible.

The takeaway is that you don't usually think about connectors much. You
declare a source, point it at the system you want data from, and the engine
takes care of the rest. For getting started quickly, RockStream includes a
built-in data generator (`CREATE SOURCE ... FROM GENERATE ROWS`) that
produces synthetic rows without any external dependencies — you can have a
working materialized view in under two minutes.

### 5. Deltas and Z-sets

To understand how RockStream maintains views efficiently, you need to
internalize one small idea: data is represented as a series of *changes*,
not as a series of *snapshots*. The change might be "this row was inserted"
or "this row was deleted" or, for an update, "this row was deleted and this
new row was inserted." Every change carries a weight: typically +1 for an
insertion or -1 for a deletion, although weights can be any integer when
the same row needs to be added multiple times.

This representation has a fancy name — a **Z-set**, short for "set with
integer weights" — but the idea is humble. Think of it as a shopping list
where each item has a quantity, and quantities can be negative. The
quantity `+1` next to "milk" means you bought one carton of milk. The
quantity `-1` next to "bread" means you returned one loaf of bread. Add up
the list and you get your net change in groceries.

The reason this is powerful is that almost every relational operation
commutes with addition of Z-sets in a clean way. If your view is
`SELECT region, SUM(amount) FROM orders GROUP BY region`, and you receive
a delta that says "one new order in NORTH with amount 42," you can compute
the change to the view by running the same SQL against just the delta:
group the delta by region, sum the amounts. The result is itself a delta —
"NORTH's revenue went up by 42" — that you can add to the view's current
state. You never touch the orders table itself. You never recompute the
whole sum. You take the previous answer and add the change.

There are edge cases that complicate this picture. Some operations (like
`SELECT DISTINCT`) are sensitive to the difference between a count of two
and a count of three in a way that makes the math trickier. Some
aggregates (like `MIN` and `MAX`) cannot be incrementally updated when a
row disappears — you need to know what the second-smallest value was, and
you have to keep that around. Some queries (recursive CTEs, certain
outer joins) need clever fixed-point machinery. RockStream handles all of
these, but the underlying idea remains the same: maintain a view by
processing the *changes* to its inputs rather than by re-running the query.

For most workloads, the practical consequence is that view maintenance is
fast and cheap. A view fed by a high-volume stream stays current with
modest hardware because the work per incoming row is small and bounded.

### 6. Epochs: Batches of Work

You might wonder why RockStream doesn't process each delta one at a time.
After all, that's the cleanest mental model. The answer is efficiency.
Every time you commit work to durable storage, you pay a fixed cost — a
write to a log, a flush to S3, a metadata update. If you commit after
every row, you spend most of your time on overhead. If you commit after
every hundred thousand rows, you waste no time on overhead but your data
might be a long time stale. The middle ground is the **epoch**: a small
batch of work that gets processed and committed together.

An epoch is bounded by two things. There is a floor — the system won't
close an epoch earlier than `min_epoch_ms` (typically ten milliseconds) or
before it has accumulated `min_epoch_bytes` of data — because closing
epochs too often is wasteful. There is a ceiling — the system won't keep
an epoch open longer than `max_epoch_ms` (typically a second or two) —
because keeping epochs open too long makes the data stale. Inside that
range, the system chooses dynamically based on what it sees: a bursty
source closes epochs more often, a quiet source less often. Operators
generally don't tune these numbers themselves; they tell the system what
freshness target they want, and the system picks epoch sizes that meet it.

The magic of an epoch is what happens when it commits. Every change that
belongs to that epoch — every state update, every view output, every
shuffle buffer entry that needs to flow to the next operator, every piece
of metadata — gets bundled into a single atomic batch and written to
SlateDB in one shot. If the batch succeeds, all of it succeeds. If the
batch fails (a crash, a network blip), none of it took effect, and the
system simply replays the epoch from its last good frontier. There is no
"partially committed epoch" state. Either you see epoch 42 fully, or you
see the world as of the end of epoch 41.

This is what makes materialized views in RockStream feel coherent. When a
client queries the view at epoch 42, every row reflects the same set of
input changes. There's no anomaly where one row updated and another
didn't. The atomic epoch commit is the foundation that makes the rest of
the consistency story work.

---

## Part 3 — How the System Stays in Sync

### 7. Frontiers: Telling Other Operators You're Done

In a distributed system that runs without a global clock and without
distributed transactions, how do operators know when it's safe to do
something? How does a join operator know that it has seen all the inputs
for a particular epoch and can produce the join's output? How does a
query gateway know that the view it's about to read is consistent? The
answer, in RockStream, is the **frontier**.

A frontier is metadata. It is a tiny piece of information that says: "I
have processed everything up to epoch *N* and I promise never to send you
updates at any timestamp earlier than that." That's the whole concept.
Operators publish frontiers. Operators read other operators' frontiers.
When the frontier on an input advances, the consuming operator knows it
can act on data at that frontier without worrying that something earlier
will arrive later. Frontiers are how the system tells itself that work is
done.

This may sound abstract, so consider a concrete analogy. Imagine three
people in three offices, each one assembling a part of a larger product.
Each person has a whiteboard outside their door that they update with a
number: "I have finished part 1, 2, ... up to and including part *N*."
The fourth person, who assembles the final product, walks past all three
whiteboards before starting their work. If all three whiteboards say
"finished through part 42," the assembler can confidently start on part
42's final assembly. If one whiteboard says "finished through 41," the
assembler waits. Nobody is on the phone with anyone. Nobody is holding a
lock. The whiteboards do all the coordination.

That is exactly what frontiers do in RockStream. They replace the locks
and the two-phase commits and the global clocks that a more traditional
system would use. Because frontiers are just metadata, they propagate
cheaply. Because they are monotonic — they only ever advance, never
retreat — operators can act on them without fear of contradiction. And
because the protocol allows for inputs to be at different frontiers at
the same time, the system never has to stop the world to take a snapshot.

The thing to internalize is that frontiers are the system's way of
substituting metadata for coordination. The cost of checking a frontier
is roughly the cost of reading a number. The cost of holding a
distributed lock is the cost of a global handshake. The difference is
enormous, and it is the reason RockStream can scale to many shards
without the coordination overhead crushing the throughput.

### 8. The Frontier Protocol in Action

It is one thing to describe frontiers and another to see them at work.
Let's walk through three examples that cover the most important cases.

**The first example is a simple chain.** Suppose you have a source
called `orders`, a view called `revenue_by_region` that aggregates from
`orders`, and another view called `top_regions` that selects the five
highest-revenue regions from `revenue_by_region`. The data flows in one
direction. The `orders` source publishes a frontier as it closes epochs:
"finished through epoch 42." The `revenue_by_region` operator reads that
frontier, sees that input epoch 42 is complete, processes its own work
for epoch 42, and publishes its own frontier saying it's now done through
42. The `top_regions` operator reads that frontier and does the same. The
chain propagates progress forward one link at a time. Nothing is forced
to wait longer than the time it takes its inputs to finish.

**The second example is a join across two shards.** Suppose you join
`orders` (sharded by `region`) with `customers` (sharded by `customer_id`).
Because the join keys are different, the join operator has to receive
data from both shards and combine them. The join's input frontier is
literally the minimum of the two source frontiers. If `orders` is at
epoch 42 and `customers` is at epoch 41, the join can only safely
process up to epoch 41. As soon as `customers` advances to 42, the join
can advance too. There is no global lock; the join simply takes the
minimum and acts on it.

**The third example is the diamond.** Suppose you have a base table
called `events`, two views `events_by_user` and `events_by_session` that
both aggregate from `events`, and a third view `user_session_summary`
that joins the two. Now consider what happens when a single batch of
events arrives. Both `events_by_user` and `events_by_session` will
process the same epoch in parallel. They may not finish at the same
instant — one might take longer due to different work shapes — but they
both eventually publish a frontier for the epoch. The downstream join
reads both frontiers and waits for both to reach the same epoch before
processing. The result is that `user_session_summary` sees a snapshot in
which the two intermediate views are perfectly aligned with each other.
This is the *diamond consistency* guarantee. The frontier protocol
delivers it without the user having to ask for it and without any
coordination overhead.

### 9. Causal Time, Not Wall-Clock Time

Distributed systems and wall-clock time get along badly. Different
machines have slightly different clocks. Network delays mean that even if
the clocks agreed, the order in which events arrive at different
observers can disagree. Anyone who has chased a bug caused by clock skew
knows that "what time is it?" is a much harder question than it sounds.

RockStream sidesteps this problem by tracking progress as a *causal*
quantity rather than a *temporal* one. The epochs we have been discussing
aren't tied to wall-clock seconds. They are tied to causal positions in
the stream of input changes. Epoch 42 means "the state of the world after
the first forty-two batches of input." Two observers can disagree about
what time it is, but they cannot disagree about which input batches have
been processed. That is the basis on which the entire consistency story
is built.

There is a separate, parallel notion of time called the **watermark**,
which connectors emit alongside data to tell time-window operators when
they can close a window. A watermark says, "I believe no further events
with timestamps earlier than *T* will arrive." If you have a query like
"give me the count of events in each one-minute window," you need
watermarks to know when to finalize each window's count. Watermarks are
about *event time* — when did the event actually happen out there in the
world — while frontiers are about *processing progress* — how much input
has the system absorbed so far. They serve different jobs and they live
side by side.

For most everyday work, you don't need to think about either explicitly.
You write SQL, the engine maintains your views, the result is consistent
and reasonably fresh. The time-aware machinery comes out when you do
things like windowed aggregations or temporal joins. Even then, the
operator chooses what watermark policy to use, and the engine handles
the rest.

---

## Part 4 — Views and Workloads

### 10. Materialized Views: The Star of the Show

A materialized view in RockStream is a SQL query whose result is
continuously kept up to date. You define it once, the system computes it
once, and from then on the system maintains it incrementally as the
underlying data changes. Querying it is the same as querying a regular
table: `SELECT * FROM revenue_by_region` returns the current contents,
which reflect every input change that has been committed at the time of
the query.

This is similar in spirit to a PostgreSQL materialized view, but with
important differences. In Postgres, materialized views are refreshed by
running the defining query from scratch — either manually with `REFRESH
MATERIALIZED VIEW` or via triggers that fire on every write. The first
approach is slow; the second is expensive and serializes writes. In
RockStream, the view is maintained continuously and incrementally. There
is no refresh step. There is no per-write trigger. The view simply stays
fresh as a side effect of the engine processing input epochs.

Storage-wise, a materialized view occupies space in the system. The
view's contents are stored in a shard of SlateDB (or across many shards
for large views), keyed by the view's primary key. Querying the view
reads from this storage. Updating the view writes to this storage. The
size of the view on disk is roughly the size of its result set, plus a
small overhead for indexing.

The view also occupies *state*. State is the intermediate data that the
operators maintain to support incremental computation. An aggregate
needs to remember the running sum and count for each group. A join needs
to remember every row on each side, indexed by the join key, so it can
look up matches when new rows arrive. A windowed aggregate needs to
remember rows that haven't yet rolled out of the window. This state is
the price you pay for incremental maintenance. It is bounded by your
workload's memory limit (chapter 15), and the system reports how much
state each view consumes so you can plan.

You can query a materialized view at the latest committed epoch or, in
some cases, at a historical epoch using `AS OF EPOCH` syntax (covered in
chapter 21). You can also subscribe to a view's output to receive a
stream of changes as they happen, which is useful when you want to
forward updates to external systems.

### 11. Inline Views: Just Named Queries

Not every view needs to be materialized. Sometimes you just want a named
query — a piece of SQL you can reuse — without paying for continuous
maintenance and storage. RockStream calls these **inline views**, and
they're defined with the standard `CREATE VIEW` syntax (no `MATERIALIZED`
keyword).

An inline view is essentially a macro. When the planner encounters a
reference to one, it substitutes the view's definition in place and
proceeds. There is no storage. There is no operator state. The view
contributes nothing to the cluster's state budget. It exists purely as a
textual convenience and a way to share query fragments across
materialized view definitions.

Use inline views when you want to:

- Compose a complex query out of smaller, named pieces without paying for
  intermediate materialization
- Present a stable interface to clients that abstracts over the details
  of the underlying tables ("`customer_profile` always projects these
  columns regardless of how we restructure the underlying tables")
- Build reusable filter or transformation fragments that get inlined into
  multiple materialized views

The trade-off is that the work is done at query time, not maintained
ahead of time. If many materialized views all reference the same inline
view, each of them does the work independently. If you find yourself
writing the same expensive subquery into multiple views, it might be
worth promoting it to a materialized view of its own so the computation
is shared.

The key thing to remember is that `CREATE VIEW` and `CREATE MATERIALIZED
VIEW` are very different. One creates a piece of stored, continuously
maintained data. The other creates a named SQL fragment. Both are useful;
they solve different problems.

### 12. Workloads: The Unit of Resource Policy

A **workload** is a named resource policy that groups related views under
a shared freshness SLO, memory budget, and priority. Workloads are
declared separately from the views that use them. A view references a
workload at creation time; the workload is not a container you deploy —
it is a constraint policy the system enforces.

A typical setup looks like this:

```sql
-- Declare the resource policy once.
CREATE WORKLOAD sales_analytics WITH (
    FRESHNESS_SLO  = '1s',
    MEMORY_LIMIT   = '100GB',
    PRIORITY       = normal
);

-- Create sources and views; assign them to the workload.
CREATE SOURCE orders FROM kafka (...);

CREATE MATERIALIZED VIEW revenue_by_region
WITH (WORKLOAD = sales_analytics)
AS
    SELECT region, SUM(amount) FROM orders GROUP BY region;

CREATE MATERIALIZED VIEW revenue_by_product
WITH (WORKLOAD = sales_analytics)
AS
    SELECT product_id, SUM(amount) FROM orders GROUP BY product_id;
```

Both views are assigned to the `sales_analytics` workload. They share
the same freshness target and memory budget. The system builds a single
shared operator graph that fans out to both views, which is more
efficient than maintaining two independent computation paths.

The workload's freshness SLO — one second, in this example — applies
to all views in the workload. The system tunes its epoch sizing and
scheduling so that each view's output is at most one second behind the
input. The memory limit of one hundred gigabytes is shared across all
views' operator state. If one view is small and the other is large,
that's fine, as long as the total stays under the limit.

You create a new workload when:

- You want **independent SLOs**: one set of views needs sub-second
  freshness, another can tolerate five-minute lag, and you don't want
  the faster one to starve the slower one or vice versa.
- You want **independent resource budgets**: you don't want a single
  rogue view to consume all the cluster's memory at the expense of
  other views.
- You want **independent priorities**: different tenants or different
  applications should have different priorities under contention.
- You want **multi-tenant isolation**: different tenants get different
  workloads, possibly in different namespaces, with constraints enforced
  independently.

Views in the same workload share fate with respect to resource
constraints. They share the SLO, the budget, and the priority. If the
workload's memory limit is exceeded, all views in the workload are
affected. This encourages you to group related views with similar
operational requirements, which usually matches how teams actually
operate them.

If you omit `WITH WORKLOAD` when creating a view, the view inherits the
schema's default workload (set with `ALTER SCHEMA ... SET DEFAULT WORKLOAD
= name`). If no schema default is set, the view uses the system default
workload, which has generous limits and normal priority.

### 13. Composing Views Out of Views

Materialized views can read from other materialized views, not just from
base tables. This is what lets you build sophisticated derived datasets
out of small, focused pieces. A common pattern looks like this:

```sql
CREATE MATERIALIZED VIEW recent_orders AS
  SELECT * FROM orders WHERE created_at > NOW() - INTERVAL '7 days';

CREATE MATERIALIZED VIEW recent_revenue_by_region AS
  SELECT region, SUM(amount) FROM recent_orders GROUP BY region;

CREATE MATERIALIZED VIEW top_recent_regions AS
  SELECT region FROM recent_revenue_by_region
  ORDER BY revenue DESC LIMIT 5;
```

Each view does one thing. The first filters; the second aggregates; the
third ranks. From the user's perspective, each view stands on its own:
you can query `recent_orders` directly, or `recent_revenue_by_region`,
or `top_recent_regions`. From the system's perspective, the three views
form a chain. When new orders arrive, only deltas flow through the
chain. The filter view forwards only the orders that match its
predicate. The aggregate view applies the deltas to its running totals.
The ranking view updates its top-5 list when totals change.

The frontier protocol coordinates the chain. Each view publishes its
frontier as it commits each epoch. Downstream views consume frontiers
from upstream views the same way they consume frontiers from base
tables. The fact that a view depends on a view rather than a table is
invisible at the protocol level.

There are two patterns worth highlighting because they come up often.
The **chain** pattern is the linear case above: view A → view B → view C.
Latency adds up across the chain — each step takes some time — but the
total is bounded by the cadence and the per-step processing time, so a
three-step chain at a 100 ms cadence still updates in roughly half a
second end to end. The **fan-out** pattern is when multiple downstream
views read from the same upstream view. Here the upstream view's
maintenance is shared, which is more efficient than maintaining the
same computation in multiple places.

The harder pattern is the **diamond**: two views share a common
upstream, and a third view joins those two. We covered the consistency
implications in chapter 8. The mechanical detail to understand is that
when a downstream view depends on multiple upstream views, the system
automatically tracks the dependency graph and ensures the downstream
sees a consistent snapshot. You write the SQL; the engine handles the
consistency.

---

## Part 5 — Freshness and Cost

### 14. Cadences: How Often Things Update

The **cadence** is how often a workload closes epochs and propagates
updates. It is the knob that controls how fresh your views are. Faster
cadences mean fresher data and more overhead. Slower cadences mean
staler data and less overhead. RockStream offers four cadence modes,
each suited to a different workload pattern.

**Deferred low-latency** is the default. Source connectors close epochs
frequently — typically every ten to a hundred milliseconds — and the
engine drains them as fast as the SLO requires. This is what you want
for dashboards, real-time analytics, and any workload where freshness in
the sub-second range matters but you don't need synchronous, in-line
updates. Most workloads run in this mode.

**Periodic** mode closes epochs at a fixed wall-clock interval — every
five seconds, every minute, every hour. This is what you reach for when
you want predictable batching, when the upstream produces data in
predictable bursts, or when you want to amortize cost across larger
batches. A periodic cadence trades freshness for throughput and is
appropriate for workloads where "fresh within a minute" is good enough
and you'd rather not pay for sub-second freshness.

**Calculated** mode lets the downstream cadence be inherited from the
demands of the consumers. If three downstream views all read from an
upstream view and the strictest of them needs one-second freshness, the
upstream runs at one-second cadence. If the strictest only needs ten
seconds, the upstream runs at ten seconds. This avoids the situation
where you over-maintain an upstream view "just in case" — the system
figures out what's needed based on actual consumer requirements.

**Immediate** mode is the strictest: the view updates synchronously
within the writing transaction. This is what pg-trickle calls IMMEDIATE
mode. RockStream supports it in a restricted form for views that fit on
a single shard and have simple defining queries (scans, filters,
projections — no joins or aggregates that span shards). The reason it's
restricted is that true cluster-wide synchronous maintenance would
require distributed locking, which would conflict with the scaling
properties that make RockStream useful in the first place. For most
workloads where you think you want IMMEDIATE, what you actually want is
the diamond consistency guarantee that the default cadence already
provides — covered in chapter 19.

You usually don't set cadence directly. You set a freshness SLO and the
system picks a cadence that meets it. The cadence shows up in
diagnostics and `EXPLAIN INCREMENTAL` output, so you can see what the
system chose, but you only override it when you have a specific reason.

### 15. SLOs: Telling the System What You Want

The key idea behind RockStream's operational interface is **intent-based
configuration**. You don't tell the system how to do its job. You tell
it what outcome you want, and the system figures out the how. The
configuration mechanism for this is the **SLO** (service-level
objective), which lives on the workload.

A workload declares its SLO in the `CREATE WORKLOAD` statement:

```sql
CREATE WORKLOAD my_workload WITH (
    FRESHNESS_SLO  = '1s',
    MEMORY_LIMIT   = '200GB',
    PRIORITY       = normal
);
```

The **freshness SLO** is the headline number: how stale your views are
allowed to be. A target of one second means the system commits to
keeping the views' output within one second of the most recent input,
under normal conditions. The system tunes its epoch sizing, its
parallelism, and its scheduling to meet this number. If it can't meet
the target (because the input rate is too high, or the cluster is too
small, or there's a transient problem), the view transitions to a named
degraded state and reports the reason. You always know whether your SLO
is being honored, and if not, why.

The **memory limit** is a soft cap on how much arrangement state the
workload's views are collectively allowed to consume. This is the
storage for the intermediate state operators maintain (the indexed join
keys, the aggregated subtotals, the windowed buffers) — not the output
rows themselves. If a view's plan requires more state than the budget
allows, it is rejected at deploy time with a clear error. This protects
you from accidentally deploying a query that would consume a terabyte of
state when you only meant to maintain a small running total.

The **priority** decides which workload's views win when the cluster is
contended. A high-priority workload gets preferential access to cluster
resources; a low-priority workload yields. Most workloads are normal;
the priority knob is there for the cases where you really do need to
guarantee that one workload comes first.

The SLO is enforced; it's not a hope. The system reports a single
**SLO compliance** metric per view — a number between 0.0 and 1.0
representing the fraction of time the SLO was met over a rolling window.
This is the one number you put on a dashboard to answer "is my
view healthy?" Drill-down metrics tell you what to look at if it
dips, but the headline is always the same shape.

### 16. Self-Tuning by Default

Inside the SLO envelope, RockStream tunes itself. The mechanisms it
adjusts include:

- **Operator parallelism**: how many parallel instances of each operator
  to run. More parallelism means higher throughput but more overhead;
  the system raises parallelism when latency budgets start to slip and
  lowers it when there's slack.
- **Epoch sizing**: the floor and ceiling on epoch duration. The system
  shortens epochs to chase tight freshness targets and lengthens them
  to amortize overhead when there's no pressure.
- **Source throttling**: the rate at which connectors poll their
  upstream. If the engine starts to fall behind, it throttles the
  connector so the input rate stops outpacing the processing rate.
- **Placement and locality**: where each operator instance runs. The
  system prefers to co-locate operators that share data to avoid
  network shuffles, but it spreads them when locality conflicts with
  meeting the SLO.

The tuner runs continuously. It observes metrics — frontier lag, epoch
duration, queue depths, write rates — and adjusts the knobs in small
steps. You normally don't see it work. You see the SLO compliance number
stay near 1.0 and the system silently does whatever it takes.

When you want to override the tuner, you can. Every adaptive knob has a
manual setting. You might pin parallelism to a specific value if you're
benchmarking, or set an epoch ceiling lower than the SLO loop would
choose if you have a strict freshness requirement that's tighter than
the budget. Overrides survive across restarts and show up in `SHOW
WORKLOAD` output so you remember they're there.

The philosophy is that tuning is the system's job, not yours, and
overrides are escape hatches for the cases where you know something the
system doesn't. In ordinary operation, you set an SLO and walk away.

### 17. The Trade-Offs Triangle

There are three things you can ask for in a streaming system: low
latency, high throughput, and low cost. You can have any two, but not
all three. This is a hard physical truth and RockStream cannot make it
go away. What it can do is make the trade-off visible and tunable.

If you want **low latency and high throughput**, you pay for it with
hardware and object-store bandwidth: many parallel workers, frequent
small commits, lots of network traffic. Set a tight freshness SLO and a
generous state budget, and the system delivers.

If you want **low latency and low cost**, you accept that throughput
will be limited: a tight freshness SLO with a constrained state budget
and a modest cluster size means you can only handle workloads up to a
certain input rate. The system tells you when that ceiling is reached
by reporting an SLO violation with a named reason like
`INPUT_RATE_EXCEEDS_CAPACITY`.

If you want **high throughput and low cost**, you accept higher latency:
a loose freshness SLO (say, one minute instead of one second) lets the
system batch much more aggressively, amortize commits across larger
units, and skip work that would otherwise be wasted. This is what
periodic cadence (chapter 14) is for.

RockStream's intent-based interface helps you make this trade-off
consciously. You write down what you want — freshness target, state
budget, cost cap — and the system tells you if those are achievable. If
they're not, you adjust your expectations, not the system's internals.

---

## Part 6 — When Things Get Interesting

### 18. Diamond Consistency Without 2PC

The diamond pattern came up briefly in chapter 8 and chapter 13. It
deserves its own chapter because it is one of the cases where RockStream
quietly does something hard.

Recall the setup: you have a base table, two views that both depend on
it, and a third view that joins those two. The classic concern is that
when an update lands in the base table, the two intermediate views
update at slightly different times. If the third view runs its join in
the gap between those two updates, it sees an inconsistent snapshot —
one view reflects the update, the other doesn't. The result is
gibberish.

PostgreSQL would solve this with a transaction that wraps all three
view refreshes together. Pg-trickle does something similar: it groups
the views into a `DiamondConsistency::Atomic` group and refreshes them
inside a single savepoint, so external observers see all three updates
or none. This works because Postgres has transactions and locks
internally; the system can pause writes while it refreshes.

In a distributed system, you can't afford to pause writes across many
shards while a downstream view refreshes. The latency would be terrible.
The throughput would collapse. The cluster's whole reason for being
would be defeated. So RockStream solves the diamond problem differently:
it uses the frontier protocol.

The third view's join operator reads frontiers from both intermediate
views. It only processes epoch *N* when both intermediates have
published a frontier of *N* or higher. Once that happens, the engine
guarantees the two intermediates are *both* at epoch *N* — by the
frontier promise — and so the join can safely combine their state. The
result is an output for epoch *N* that reflects a consistent snapshot
of the underlying base table at epoch *N*.

There is no lock. There is no two-phase commit. There is no pause in
the write path. The two intermediates may have committed at different
wall-clock times; that's fine. What matters is that their frontiers
align at the moment the downstream consumes them. The frontier protocol
substitutes metadata coordination for synchronous coordination, and the
result is the same correctness with vastly better scaling properties.

This is the secret sauce. Anywhere in your view graph that you have a
diamond — wherever two paths from a common source rejoin — RockStream
quietly enforces consistency through frontiers. You don't write
anything special. You just write the SQL and the engine handles it.

### 19. IMMEDIATE Mode and What It Means in a Distributed World

Pg-trickle has an IMMEDIATE refresh mode that maintains a view
synchronously within the same transaction as the source DML. When you
`INSERT` into a base table, the view updates before the `INSERT`
returns. This is wonderful for sub-millisecond freshness on a single
PostgreSQL machine, and it is the right answer for many OLTP-style
workloads where you want read-your-writes consistency.

The reason it works in pg-trickle is that PostgreSQL has transactions
that span all the relevant work and triggers that fire within those
transactions. The system can hold a lock on the view, copy the
transition tables into temp storage, run the delta computation, apply
the result to the view's storage, and release the lock — all inside the
same transaction. No other process sees a partially updated state. The
writer waits for the whole thing.

This does not generalize cleanly to a distributed cluster. To do the
same thing across many shards, you would need a distributed transaction
spanning the write and all the downstream views — and any shards they
touch. That means distributed locking, two-phase commit, and global
coordination on every write. RockStream is built specifically to avoid
those things, because they are what makes systems hard to scale beyond
a single machine.

RockStream therefore supports IMMEDIATE in a restricted form: views
that fit on a single shard, with simple defining queries (no
multi-shard joins, no global aggregates, nothing that requires shuffle).
For these, the engine can offer synchronous freshness with single-shard
locking. The query analyzer detects whether a view is eligible at
`CREATE` time and refuses to mark it IMMEDIATE if it isn't.

For everything else, the right answer is not IMMEDIATE but the
combination of a tight freshness SLO and the diamond consistency
guarantee. If you set your SLO to a hundred milliseconds, the system
delivers updates within a hundred milliseconds. If you use coordination
groups (the diamond pattern), you get cross-view consistency. The
practical difference between "synchronous immediate" and "asynchronous
within 100 ms with cross-view consistency" is small for most workloads,
and the scalability cost of the former is large. RockStream picks the
trade-off that lets you scale; pg-trickle picks the trade-off that
gives you the strongest single-machine semantics. Both are right for
their use case.

### 20. Bulk Loads and Source Gating

A common operational problem: you need to load ten million historical
rows into a base table. If your view is being maintained at a
hundred-millisecond cadence, that load is going to trigger thousands of
intermediate refreshes, each one applying a tiny delta to your view's
state. You'll end up doing a lot of work that gets immediately undone by
the next batch. You'd much rather pause maintenance, do the bulk load,
and resume — letting the system catch up with one large coalesced
refresh at the end.

Pg-trickle has a feature for exactly this called **source gating**. You
gate the source table; the scheduler skips downstream view refreshes
that depend on the gated source; you bulk-load; you ungate the source;
maintenance resumes. RockStream supports the same pattern, built on top
of the credit-based backpressure system you met in chapter 4.

The mechanism is: a gated source has its credits set to zero. With zero
credits, the connector cannot emit new epochs into the engine. The
in-flight epoch (if any) is allowed to complete, but no new ones start.
Downstream views' frontiers stop advancing because their inputs aren't
producing new data. The bulk load proceeds against the underlying
source storage. When you ungate, credits restore, the connector picks
up where it left off, and the accumulated changes flow through as
either one large epoch or a small number of large epochs — much more
efficient than processing them in small bites.

In the SQL interface, this looks something like:

```sql
PAUSE SOURCE my_source;
-- bulk load proceeds here, perhaps via COPY or direct ingest
RESUME SOURCE my_source;
```

There is also a more sophisticated variant called **watermark gating**
that pauses maintenance until multiple sources' event-time watermarks
align within a tolerance window. This is useful when you have a view
that joins data from multiple ETL-fed sources and you want to wait for
them all to catch up to roughly the same event time before producing
output. The mechanism is the same — credits suppress epoch emission —
but the trigger is alignment rather than an explicit pause.

The takeaway is that the engine already has the machinery to support
operational features like gating. The user-facing API is small and the
underlying mechanism is the same one that handles backpressure in
ordinary operation.

### 21. Reading Historical Data

A materialized view is a moving target. Most of the time you want the
latest committed state, and that's what `SELECT * FROM my_view`
returns. But sometimes you want to read the view as it was at some
earlier point — to reproduce a result, to debug a regression, to
generate a snapshot for downstream comparison. RockStream supports
this through the `AS OF` clause:

```sql
SELECT * FROM my_view AS OF EPOCH 12345;
SELECT * FROM my_view AS OF TIMESTAMP '2026-05-28 14:00:00';
```

The first form reads at an explicit epoch number. The second form
translates the timestamp to the nearest committed epoch and reads
there. Both forms are bounded by the view's retention window: if you
ask for data older than the retention horizon, you get an error
explaining what happened.

Retention is a property of the materialized view. By default, the
system retains enough history to cover the last seven days or one
hundred and twenty-eight committed checkpoints, whichever is longer.
You can override this per view:

```sql
CREATE MATERIALIZED VIEW my_view WITH (retention = '30d') AS ...;
```

Retention costs storage. A view with thirty days of retention may have
significantly more data on disk than a view with the default seven
days, depending on how much the view changes. The system reports
per-view storage usage so you can tune this consciously.

For ad-hoc analytics over a longer history, the **cold tier** is the
right tool. The cold tier writes periodic snapshots of view contents as
Iceberg or Delta Lake tables in object storage. Tools like DuckDB,
Trino, and Spark can read these directly without going through
RockStream. The cold tier is enabled per view and runs on a slower
cadence — typically every few minutes to every hour — producing
columnar snapshots that are cheap to scan but lag behind the live
view's state.

The distinction matters. The live view (in SlateDB) is optimized for
incremental maintenance and key-based lookups; it serves point queries
and dashboards with low latency. The cold tier (in Iceberg) is
optimized for analytical scans; it serves ad-hoc SQL over weeks or
months of data. You usually want both: live for dashboards, cold for
exploration.

### 22. Recursive Queries and Graphs

Some queries don't fit the straightforward "scan, filter, join,
aggregate" mold. Transitive closure of a graph. Hierarchical roll-ups
through a tree of categories. Reachability in a network. These all
require **recursion**: the answer depends on the answer applied to
itself.

In SQL, you express recursion with `WITH RECURSIVE`:

```sql
WITH RECURSIVE descendants AS (
  SELECT id, parent_id FROM categories WHERE id = 42
  UNION
  SELECT c.id, c.parent_id
  FROM   categories c
  JOIN   descendants d ON c.parent_id = d.id
)
SELECT * FROM descendants;
```

This says: start with category 42, then keep adding its children, and
its children's children, until no new rows come in. Doing this
incrementally — keeping the answer fresh as the underlying tree
changes — is genuinely harder than maintaining a flat aggregate. New
edges might create new reachability paths. Deleted edges might remove
paths. The system has to figure out which paths in the answer are
still valid after every change.

RockStream supports this through a technique called **semi-naive
evaluation** for insert-only changes and **Delete-and-Rederive (DRed)**
for mixed changes. Semi-naive works on the principle that any new path
must involve at least one new edge, so you only need to consider
extensions of recently-added edges rather than re-running the whole
recursion. DRed is more involved: it tentatively removes paths that
depended on deleted edges, then re-derives any paths that survive
through alternate routes. Both techniques are bounded by a recursion
depth limit to prevent runaway iteration.

For most users, the only thing to know is that `WITH RECURSIVE` works
and that the system maintains it incrementally. You write the SQL and
the engine handles the rest. For very large or deeply recursive
workloads, there are diagnostic tools that let you see what strategy
the engine chose and how many iterations it took.

---

## Part 7 — Operations Without Drama

### 23. The One Signal: SLO Compliance

If you only look at one metric for your view, it should be **SLO
compliance**. This is a number between 0.0 and 1.0 that represents the
fraction of time the view met its freshness target over a rolling
window (by default five minutes). A value of 1.0 means the SLO has
been met for the entire window. A value of 0.8 means it was missed 20%
of the time. A value of 0.0 means it was never met.

You put this single number on your dashboard, one per view. If
they're all at 1.0, everything is fine and you don't need to look at
anything else. If one dips, you click into it and the system shows you
the **degradation reason**: a short, named code like
`INPUT_RATE_EXCEEDS_CAPACITY`, `SHARD_REBALANCING`, `OBJECT_STORE_SLOW`,
`STATE_BUDGET_EXHAUSTED`. Each reason has a known meaning and a known
mitigation. You don't have to chase mysterious symptoms; the system
tells you what's wrong in operator terms.

This is a deliberate operational stance. Most monitoring systems give
you a thousand metrics and let you figure out which ones matter.
RockStream gives you one metric per view that summarizes whether
your intent is being honored, and then drill-downs that tell you what
to do if it's not. The aim is that an on-call engineer with no specific
RockStream training should be able to answer "is my workload healthy?"
within ten seconds and "what's wrong?" within a minute.

### 24. Failure and Recovery

Things break. Workers crash. Networks partition. Object storage has
brownouts. RockStream is designed to recover from all of these without
manual intervention and within bounded time.

When a **worker dies**, its shards are re-leased to other workers. The
SlateDB single-writer mechanism prevents split-brain: the old writer's
manifest is fenced, so even if it comes back online it cannot commit.
The new writer opens the shard, reads its last committed frontier,
replays any WAL entries beyond the last checkpoint, and resumes
processing from there. The view transitions to a `RECOVERING`
state during this process and back to `ACTIVE` when the frontier
catches up. The recovery time is bounded by the cluster's recovery
SLO, typically under sixty seconds.

When **object storage is slow**, the system backs off. Operators that
can't flush their writes accumulate them in local buffers. Connectors
throttle their input rate so the buffers don't grow unbounded. The
view reports a degraded state with the reason `OBJECT_STORE_SLOW`.
When storage recovers, the buffers drain and the view returns to
normal. No data is lost, but freshness lags during the brownout.

When a **network partition** isolates a worker, the partitioned worker
detects its isolation through missing heartbeats and self-fences,
giving up its shard leases voluntarily. The control plane reassigns
the shards to a still-connected worker. This is critical: a
partitioned-but-alive worker that didn't self-fence could race a new
owner and cause divergent commits. The self-fencing rule prevents this
class of bug entirely.

For the operator, the visible effect of all these failures is the same:
SLO compliance dips during recovery and rises again afterward. The
degradation reason tells you what kind of failure happened. You usually
don't have to do anything; the system handles it. When you do need to
intervene — say, to add capacity if a workload has outgrown the cluster
— the documentation tells you exactly what action each reason calls for.

### 25. Scaling Up, Scaling Down, Scaling Out

The same RockStream binary serves three deployment tiers, and you can
move between them additively without rewriting your configuration or
migrating your data.

**Tier 1** is a single process on your laptop or a CI runner. You point
it at a local filesystem directory and you're up and running. The
control plane, the worker, the frontier aggregator, and the gateway are
all in one process. Latency is low because everything is in-memory or
on local disk. This is what you use for development, evaluation, and
small test workloads.

**Tier 2** is a single host configured to use shared object storage.
You launch one process with `--role=all --storage=s3://...` and now your
data survives across restarts and is durable beyond any single machine.
Within this single host, you can run as many worker threads and shards
as the hardware supports. This tier is suitable for small production
workloads where one machine is enough.

**Tier 3** is the full multi-host cluster. Control plane processes run
on a few nodes (typically three to five for high availability); worker
processes run on as many nodes as you need for your workload. You add
nodes online, the control plane discovers them and admits them, and
workloads start using the new capacity. You remove nodes by draining
them gracefully and the shards they own get migrated elsewhere.

The same data files written in Tier 1 against MinIO can be opened in
Tier 3 against S3. There is no migration step because there is no
node-local state. The deployment ladder is purely additive: you add
machines, you add roles, you don't reconfigure.

This matters because it means you can start small. You don't have to
design a cluster on day one. You build your workloads against a laptop
deployment, you deploy them to a single production host when you're
ready, and you scale out to a cluster when the load demands it. The
mental model and the SQL never change.

### 26. Multi-Tenancy: Namespaces and Quotas

If multiple teams share a cluster, you'll want each team's workloads to
be isolated from the others. RockStream supports this through
**namespaces**, which are roughly analogous to PostgreSQL databases:
each namespace has its own catalog of tables, views, and workloads, and
its own set of quotas.

A workload lives in a namespace. The workload's resource usage counts
against the namespace's quotas, not against any global cluster budget.
This means one team's runaway workload cannot starve another team's
views: if team A's namespace exceeds its budget, team A's views
degrade, but team B's views continue normally.

Access control is also per-namespace. A user with `pipeline_owner`
rights on namespace A can deploy, alter, and drop views in
namespace A, but cannot even see what exists in namespace B. The pgwire
gateway routes connections to the right namespace based on the
connection string, exactly like a database in PostgreSQL.

For most single-team deployments, you don't need to think about
namespaces; there's a default namespace and everything goes there. For
multi-tenant deployments, namespaces are the boundary you administer.

---

## Part 8 — Reference Material

### 27. Glossary

**Arrangement** — Indexed operator state, typically a key-value map of
the data an operator needs to support incremental updates. The state of
a join, for example, is an arrangement of each side's rows indexed by
the join key.

**Backpressure** — A mechanism by which downstream operators (or sinks)
signal upstream that they cannot accept more data, causing upstream to
slow down. In RockStream, implemented via the credit system on
connectors.

**Cadence** — How often a workload closes epochs. The four modes are
deferred low-latency, periodic, calculated, and immediate.

**Connector** — A piece of code that bridges between an external system
(Kafka, PostgreSQL, S3, etc.) and the RockStream engine, exposing the
external data as a source or accepting the engine's output as a sink.

**Coordination group** — A set of views that the system treats as a
single consistency unit. Diamonds (two views from a common source, a
third view joining them) are the most common example.

**Delta** — A change, typically expressed as a Z-set. A delta might
contain inserts (+1 weight) and deletes (-1 weight). Operators process
deltas; they don't reprocess whole tables.

**Epoch** — A batch of input changes processed and committed together
as an atomic unit. Epoch durations are bounded by the cadence and the
SLO.

**Frontier** — A piece of metadata published by an operator that says
"I have processed everything up to epoch *N* and I will not send you
older updates." Frontiers are the protocol RockStream uses to
coordinate progress across operators and shards.

**Inline view** — A view defined with `CREATE VIEW` (not `CREATE
MATERIALIZED VIEW`). Stored as a SQL fragment; expanded inline when
referenced. Consumes no state.

**Materialized view** — A view defined with `CREATE MATERIALIZED VIEW`.
Continuously maintained by the engine; queryable like a table. Consumes
storage proportional to its result size plus operator state.

**Namespace** — An isolation boundary roughly analogous to a PostgreSQL
database. Catalog objects, workloads, quotas, and access control are
scoped to a namespace.

**Workload** — A named resource policy. Groups related views under a
shared freshness SLO, memory limit, and priority. The unit of
operational intent.

**Quota** — A resource cap (state bytes, object-store rate, etc.)
applied at the workload or namespace level.

**Shard** — A partition of the cluster's state. Each shard is owned by
one worker and corresponds to one SlateDB instance.

**Sink** — A destination for view output, typically an external system
like Kafka, Iceberg, or another database. Optional; views can be
queried directly without a sink.

**SLO (service-level objective)** — A workload-level promise about
freshness, memory limit, or priority. The system tunes itself to meet
the SLO.

**Source** — An input that feeds views. Defined via a connector pointing
at an external system or via RockStream's internal write API.

**Watermark** — A piece of event-time metadata emitted by connectors
that tells time-window operators when they can close a window.
Separate from frontiers, which track processing progress.

**Z-set** — A multiset with integer weights. The data type that flows
between operators in RockStream.

### 28. Decision Trees

**"Should I create a new workload?"** Ask yourself: do these views need
the same freshness target? Should they share a memory budget? Should
they have the same priority? If yes to all three, one workload. If no
to any, separate workloads.

**"Which cadence should I pick?"** Start with the default (deferred
low-latency). Switch to periodic if you want predictable batching and
can tolerate fixed-interval latency. Use calculated if your view feeds
downstream views with widely varying freshness needs. Use immediate
only when you have read-your-writes requirements and your view fits
the eligibility criteria.

**"Do I need IMMEDIATE mode?"** Probably not. If your reason is
"I want fast updates," set a tight freshness SLO instead. If your
reason is "I want cross-view consistency," coordination groups give
you that automatically. The narrow case where you actually need
IMMEDIATE is read-your-writes consistency on a single-shard view that
must reflect a write before the write transaction returns.

**"Should I use a materialized view or an inline view?"** Materialized
if the view is queried often, joined with other tables, or feeds
downstream maintained data. Inline if it's just a reusable SQL fragment
or an abstraction layer.

**"Should I enable the cold tier?"** Yes if you have ad-hoc analytical
workloads over long time horizons. No if all your reads are point
queries or dashboards against recent data.

**"How tight should my freshness SLO be?"** Tighter SLOs cost more.
Pick the loosest SLO that meets your business requirement. "Dashboard
that refreshes every 5 seconds" doesn't need a 100-ms SLO; one or two
seconds is plenty.

### 29. Further Reading

For the architectural deep dive, see [DESIGN.md](../DESIGN.md) — the
authoritative description of the system, its principles, and the
trade-offs behind each design decision.

For the incremental view maintenance specifics, see
[IVM.md](../IVM.md) — how RockStream borrows from Feldera/DBSP, what it
takes from pg-trickle as a correctness oracle, and the per-operator
differentiation rules.

For the phased build-out, see
[IMPLEMENTATION_PLAN.md](../IMPLEMENTATION_PLAN.md) — the roadmap from
the current state to a production-grade system, with milestones and
deliverables.

For the underlying ideas, the foundational papers are:

- **DBSP** (Budiu et al.): the mathematical framework for incremental
  computation that RockStream's runtime is modeled on.
- **Differential Dataflow** (McSherry et al.): the original work on
  frontier-based progress tracking that inspired RockStream's
  coordination model.
- **FoundationDB**: the source of the deterministic-simulation testing
  discipline that RockStream adopts for correctness.
- **The CALM theorem** (Hellerstein, Alvaro): the formal basis for the
  monotone-frontier commit invariant.

The other systems worth reading for comparison are **Materialize**
(streaming SQL, single-node memory-resident state), **RisingWave**
(streaming SQL, distributed, similar problem space to RockStream),
**Feldera** (DBSP-native, single-node), **pg-trickle** (incremental
maintenance inside PostgreSQL), and **Snowflake dynamic tables**
(declarative incremental maintenance in a warehouse).

---

## A Final Note

The concepts in this guide are deliberately presented from the user's
side of the curtain. The implementation is much more elaborate than the
mental model: there are dozens of subsystems, hundreds of metrics, and
thousands of lines of carefully reasoned code that all exist so that
the user-facing experience can be simple. This is intentional. The
test of a good system is not how clever its insides are; it's how easy
it is to use its outside.

When you write a SQL view in RockStream and it stays fresh while your
data grows from a thousand rows to a billion, you're seeing the payoff
of every architectural decision in this guide. The frontiers, the
epochs, the shards, the coordination groups, the SLO loop, the
self-tuning, the namespaces — all of it exists so that the SQL you
wrote keeps working as your world gets bigger.

That is the promise. The rest of the system is the machinery that
delivers it.
