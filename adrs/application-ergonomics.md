# ADR: Application Ergonomics — Freshness, Lifecycle, and CLI Surface

**Status**: Proposed  
**Date**: 2026-05-29

---

## Context

Three ergonomic gaps remain after the workload ADR that affect application developers
and operators doing day-to-day work:

1. **Read-after-write is opt-in.** The design (v3.6) exposed freshness tokens that
   applications had to thread manually. This is a significant burden on application
   developers who just want "read what I just wrote."

2. **Blue-green view replacement has no user-facing solution.** Replacing a view
   definition without downtime requires careful manual sequencing (create new view,
   wait for hydration, drop old view, rename). The design acknowledges this is a
   coordination problem but provides no DDL to solve it.

3. **The CLI surface references "pipelines and views."** After dropping `CREATE PIPELINE`
   as a user construct (see workloads ADR), this description is stale and will confuse
   users who read both the CLI help and the design docs.

---

## Decision

### 1. Read-after-write is the default session behaviour

Applications should never need to thread freshness tokens manually for the common case
of "I just wrote something, now I want to read it back."

**Default behaviour**: After any `INSERT`, `UPDATE`, or `DELETE` in a session, the
next `SELECT` in that session waits until the materialized views it reads have caught
up to include that write.

```sql
INSERT INTO orders VALUES (42, 'widget', 99.99);
-- No token required. The next SELECT automatically sees the inserted row.
SELECT * FROM order_summary WHERE order_id = 42;  -- always consistent
```

This is `READ AFTER WRITE` consistency, scoped to the session. It does not require
global synchronisation — the session tracks its own write frontier and the gateway
enforces it locally.

**For applications that need to relax this** (high-throughput write paths where
occasional stale reads are acceptable):

```sql
SET session_read_after_write = OFF;
-- or per-query:
SELECT /*+ ALLOW_STALE */ * FROM order_summary WHERE order_id = 42;
```

**For applications that need stricter guarantees** (cross-session consistency):

```sql
-- Returns a token representing the current write frontier
SELECT rockstream.write_fence() AS fence;
-- Pass the token to another session or service, which can wait for it:
SELECT * FROM order_summary WHERE rockstream.after_fence(:fence);
```

The manual token API remains available for cross-session and cross-service cases. It
is just no longer the *default* experience.

### 2. Blue-green view replacement via `CREATE REPLACEMENT MATERIALIZED VIEW`

Replacing a view definition without downtime is a first-class operation. The pattern
mirrors what Materialize ships:

**Step 1**: Create a replacement (hydrates in the background, does not affect the
original):

```sql
CREATE REPLACEMENT MATERIALIZED VIEW reporting.daily_summary_v2
  FOR reporting.daily_summary AS
  SELECT region, SUM(amount) AS total, COUNT(*) AS order_count
  FROM orders
  GROUP BY region;
```

The replacement view is marked as a staging object. It is not queryable by end users.
It hydrates against historical data while the original continues serving live traffic.

**Step 2**: When ready, apply the replacement atomically:

```sql
-- Check readiness first:
SHOW REPLACEMENT STATUS FOR MATERIALIZED VIEW reporting.daily_summary;
-- Status: HYDRATED (ready to apply)

-- Apply with no downtime:
ALTER MATERIALIZED VIEW reporting.daily_summary APPLY REPLACEMENT daily_summary_v2;
```

The swap is atomic from the user's perspective: queries either see the old results or
the new results, never a partial state. The old view's state is cleaned up
asynchronously after the swap.

**Step 3** (optional): Roll back if something is wrong:

```sql
ALTER MATERIALIZED VIEW reporting.daily_summary DISCARD REPLACEMENT daily_summary_v2;
```

**Restrictions** (consistent with Materialize's learnings):
- The replacement must produce the same output schema (column names, types, order) as
  the original.
- You cannot create dependent objects (indexes, other views) on a replacement view
  before it is applied.
- Only one pending replacement per view is allowed at a time.

### 3. CLI surface updated to reflect current design

The `rockstream` CLI help text and documentation must reflect the current DDL surface.
The phrase "pipelines and views" is replaced throughout:

| Before | After |
|---|---|
| "The CLI surface is pipelines and views" | "The CLI surface is workloads, schemas, views, and sources" |
| `rockstream pipeline list` | `rockstream view list` |
| `rockstream pipeline pause <name>` | `rockstream view pause <schema>.<name>` |
| `rockstream pipeline resume <name>` | `rockstream view resume <schema>.<name>` |

Schema-level operations:

```
rockstream schema pause <schema_name>    # pause all views in a schema
rockstream schema resume <schema_name>   # resume all views in a schema
rockstream workload list                 # show all workloads and their limits
rockstream workload show <name>          # show views using a workload
```

The binary itself remains a single `rockstream` binary with role flags — that design
decision is unchanged. Only the sub-command names are updated.

---

## Consequences

### What gets better

- Application developers get sensible read-after-write behaviour without learning about
  freshness tokens. Tokens remain available for advanced cross-session use cases.
- View replacement goes from a manual multi-step process (prone to partial-failure
  states) to a two-step atomic operation.
- Users reading CLI help will no longer encounter references to "pipeline" as a
  user-facing concept.

### What we accept

- Session-scoped read-after-write adds a frontier check on each `SELECT` that follows a
  write. This is a bounded per-query overhead (a single frontier comparison) and is
  negligible for all but the most latency-sensitive applications. The `ALLOW_STALE`
  escape hatch covers those cases.
- `CREATE REPLACEMENT MATERIALIZED VIEW` requires the control plane to manage staging
  view state and apply the swap atomically. This is a non-trivial implementation cost,
  but it is bounded and well-understood (Materialize has shipped it).
- CLI sub-command renames are a breaking change for any scripts or automation written
  against the old names. Since we are pre-1.0, this is acceptable. Deprecation aliases
  can be added if the old names are in widespread use by beta users.

---

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| Keep freshness tokens as the only API | Too burdensome for application developers. Every read after every write requires manual token threading. RisingWave's default `READ COMMITTED` model shows users expect this to "just work." |
| Manual view rename (create, wait, drop, rename) | Fragile under failure. A crash between drop and rename leaves the system in a broken state. The replacement model is transactional. |
| Keep "pipeline" in CLI for backward compat | The concept is gone from the DDL. Keeping it in CLI creates a split mental model. Clean break is better pre-1.0. |
