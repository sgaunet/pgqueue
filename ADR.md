# Architecture Decision Records

This document captures key architectural decisions made in `pgqueue` and the rationale behind them. It serves as a reference for contributors to understand design choices and maintain consistency.

---

## ADR-001: Direct SQL Instead of ORM or Query Builder

**Status**: Accepted

**Decision**: Use `database/sql` with parameterized queries instead of an ORM (GORM, ent) or query builder (sqlc, squirrel).

**Context**: pgqueue relies heavily on PostgreSQL-specific features (advisory locks, `FOR UPDATE SKIP LOCKED`, `uuidv7()`, partial indexes). These features are difficult to express through ORMs or may require escape hatches that negate their benefits.

**Rationale**:
- **Transparency**: Every query is visible in source code; performance is predictable with no hidden N+1 or implicit JOIN overhead.
- **PostgreSQL-native**: Direct access to features like `FOR UPDATE SKIP LOCKED`, partial indexes (`WHERE status = 'pending'`), and `uuidv7()` without abstraction leaks.
- **No build step**: No code generation required (unlike sqlc), keeping the build simple.
- **Driver-agnostic**: Works with both `pgx` and `lib/pq` since it only depends on `database/sql` interfaces.

**Trade-offs**: More boilerplate for scanning rows, and the query surface is spread across the package rather than centralised — `queries.go` holds the shared metadata/DLQ/replay-log helpers, but each feature owns its own SQL (`channel.go`, `pubsub.go`, `consume.go`, `batch.go`, `publish.go`, `gc.go`, `replay.go`, `stats.go`, `migrations.go`), and the per-queue DDL lives in `pgqueue.go`. Locating every statement therefore means grepping the package, not opening one file.

---

## ADR-002: Table-Per-Queue Architecture

**Status**: Accepted

**Decision**: Each queue gets dedicated tables (`pgqueue_msg_{name}`, `pgqueue_dlq_{name}`, and `pgqueue_sub_{name}` for pub/sub) created dynamically by `CreateChannel()`/`CreateTopic()`.

**Context**: The alternative is a single shared messages table with a `queue_name` column. This is simpler to set up but creates operational problems at scale.

**Rationale**:
- **Isolation**: Queue operations don't interfere with each other. A slow consumer on one queue doesn't lock rows needed by another.
- **Independent scaling**: Hot queues can be moved to separate tablespaces or partitioned independently.
- **Index efficiency**: Per-queue partial indexes (e.g., `WHERE status = 'pending'`) stay small and fast. A shared table would have a larger index spanning all queues.
- **VACUUM performance**: Dead tuples are isolated per queue. A high-throughput queue won't bloat the shared table's VACUUM workload.
- **Schema flexibility**: Channel and pub/sub message tables have different schemas optimized for their access patterns.

**Trade-offs**: More tables to manage, and dynamic DDL requires careful name validation (`queueNameRegex`). Queue names are sanitized via `sanitizeTableName()` to prevent SQL injection.

**Scalability ceiling**: Each queue multiplies the number of PostgreSQL relations in the database. A **channel** is 2 tables (`pgqueue_msg_*`, `pgqueue_dlq_*`) and roughly 8 indexes; a **topic** is 3 tables (adds `pgqueue_sub_*`) and roughly 12 indexes (counting primary keys). So N queues add on the order of `N × (2–3)` tables and `N × (8–12)` indexes to `pg_class` / `pg_index`. Several admin operations also scale linearly with queue count: `GarbageCollector.Collect` iterates every queue, `ListChannels` / `ListTopics` walk the metadata table, and `UnhealthySubscribers` issues one query per topic (N+1). The design is comfortable for **tens to low hundreds of queues per database**. At the scale of thousands of queues, PostgreSQL catalog bloat, query-planning latency, and autovacuum-of-catalog overhead become the dominant cost. The table-per-queue design is therefore **not** appropriate for patterns that mint a queue per tenant or per user — for those workloads, multiplex tenants onto a fixed pool of queues (demultiplex via message metadata or payload), or shard tenants across separate databases. Pass `WithMaxQueues(n)` to `New` to enforce a hard cap: once it is reached, `CreateChannel` / `CreateTopic` return `ErrMaxQueuesReached` rather than silently growing the catalog, so the ceiling is hit deliberately, not by surprise.

---

## ADR-003: UUIDv7 as Primary Key

**Status**: Accepted

**Decision**: Require PostgreSQL 18+ for native `uuidv7()` and use it as the primary key for all tables.

**Context**: Common alternatives include `BIGSERIAL`, `UUIDv4` (`gen_random_uuid()`), or application-generated IDs.

**Rationale**:
- **Time-ordered**: UUIDv7 embeds a millisecond timestamp, enabling `ORDER BY id` for chronological message ordering without a separate index on `created_at`.
- **K-sortable**: Sequential inserts avoid the B-tree page splits caused by random UUIDv4, resulting in better write and index performance.
- **Native function**: PostgreSQL 18 provides `uuidv7()` natively, so no extension or Go library dependency is needed for ID generation.
- **Distributed-safe**: Unlike `BIGSERIAL`, UUIDs don't require coordination between nodes, making future horizontal scaling possible.

**Trade-offs**: Requires PostgreSQL 18+, which limits compatibility. This is an intentional choice to leverage the latest PostgreSQL capabilities without workarounds.

---

## ADR-004: FOR UPDATE SKIP LOCKED for Message Consumption

**Status**: Accepted

**Decision**: Use `SELECT ... FOR UPDATE SKIP LOCKED` to dequeue messages from channels.

**Context**: Message consumption requires that concurrent consumers never claim the same message at the same time. Common approaches include advisory locks, `DELETE ... RETURNING`, or `UPDATE ... RETURNING` with subqueries.

**Rationale**:
- **Non-blocking**: `SKIP LOCKED` allows concurrent consumers to each grab a different message without waiting. Consumers never block each other.
- **At most one concurrent consumer per message**: The row lock ensures no two consumers hold the same message in flight. This is mutual exclusion, **not** exactly-once delivery — pgqueue is **at-least-once** by design. When a claim's visibility timeout lapses without an ack, the message is deliberately redelivered (see the visibility-timeout bullet below), so the same message can be handed out more than once over its lifetime. Handlers must be idempotent.
- **PostgreSQL-native**: This is a first-class PostgreSQL feature designed for queue workloads, well-optimized in the query planner.
- **Visibility timeout integration**: Pairs naturally with the visibility timeout pattern: lock the row, set `status='processing'` and `visibility_timeout`, then commit.

**Trade-offs**: Requires the consuming operation to run within a transaction. This is already the natural pattern for pgqueue's consume-then-ack workflow.

---

## ADR-005: Separate Channel and Pub/Sub Schemas

**Status**: Accepted

**Decision**: Channel and pub/sub message tables have fundamentally different schemas rather than a unified table with optional columns.

**Context**: Channels (point-to-point) and pub/sub (fan-out) have different state tracking needs.

**Rationale**:
- **Channels** need per-message state: `status`, `retry_count`, `max_retries`, `visibility_timeout`, `available_at`, `claim_id`, `processed_at`, `error_message`. Messages transition through `pending -> processing -> completed/DLQ`.
- **Pub/Sub** messages are immutable once published (`id`, `payload`, `created_at`, `metadata`). Per-subscriber state lives in the `pgqueue_sub_{name}` table, allowing each subscriber to track progress independently.
- **No wasted storage**: Channel tables don't carry subscriber tracking columns; pub/sub tables don't carry retry/status columns.
- **Cleaner queries**: Each pattern's queries are optimized for its specific schema without conditional logic.

**Trade-offs**: Two code paths for table creation and querying, but this reflects a genuine domain difference, not accidental complexity.

---

## ADR-006: Destructive Operations Are Explicit at the Call Site (No Confirmation Flag)

**Status**: Accepted — updated 2026-07, superseding the original `confirm=true` design (see note below).

**Decision**: Destructive operations do not take a confirmation parameter. Instead:
- **Replay** (`ReplayFrom`, `ReplayMessage`, `ReplayDLQ`) **executes by default**. To preview how many messages *would* be replayed (and skipped) without mutating anything, pass `ReplayOptions{DryRun: true}` — the returned `Replayed` / `Skipped` counts report what a real run would do.
- **Purge and delete** (`GarbageCollector.PurgeQueue`, `DeleteChannel`, `DeleteTopic`) act **immediately and irreversibly** and take no confirmation flag. Callers gate them at the call site.

**Context**: An earlier iteration required an explicit `confirm=true` / `Confirm: true` boolean on every destructive call. In practice a mandatory `true` literal is boilerplate that callers and reviewers rubber-stamp: it does not actually prevent copy-paste accidents (the copied call carries `true` too), it clutters the API, and it conflates two distinct needs — *previewing* a replay versus *authorizing* a purge.

**Rationale**:
- **A preview beats a confirmation for replay.** `DryRun: true` returns the would-be `Replayed` / `Skipped` counts with no side effects — the real information an operator wants before a large replay, rather than a ceremonial boolean.
- **Purge/delete intent belongs at the call site.** These have no meaningful preview, so the guard that matters is one the caller owns: a feature flag, an operator prompt, or an environment check. A library-level boolean gives false assurance without adding real safety.
- **Cleaner, idiomatic API.** Executing by default, with an opt-in dry run, matches Go norms and removes a required argument that carried no information.

**Trade-offs**: Destructive calls are no longer syntactically self-flagging, so the docs foreground them (see the "Destructive operations" sections in the README and CLAUDE.md) and callers must add their own guards where the blast radius warrants.

> **Superseded/updated (2026-07).** The original ADR-006 mandated a `confirm=true` / `Confirm: true` parameter on these operations. That parameter was removed (commit `731b4fa`) in favour of the DryRun-preview / call-site-gated design above. This entry is updated in place to reconcile the ADR with the shipped API, the project constitution (v2.0.0), the README, and CLAUDE.md, which were already aligned to it.
