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

**Trade-offs**: More boilerplate for scanning rows, but the query surface is small and well-contained in `queries.go`.

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

**Scalability ceiling**: Each queue is 2-3 tables plus 6-7 indexes, so queue count multiplies the number of PostgreSQL relations in the database. Several admin operations also scale linearly with queue count: `GarbageCollector.Collect` iterates every queue, `ListChannels` / `ListTopics` walk the metadata table, and `GetUnhealthySubscribers` issues one query per topic (N+1). The design is comfortable for **tens to low hundreds of queues per database**. At the scale of thousands of queues, PostgreSQL catalog bloat, query-planning latency, and autovacuum overhead become the dominant cost. The table-per-queue design is therefore **not** appropriate for patterns that mint a queue per tenant or per user — for those workloads, multiplex tenants onto a fixed pool of queues (demultiplex via message metadata or payload), or shard tenants across separate databases. `WithMaxQueues(n)` lets you enforce a cap so the ceiling is reached deliberately, not by surprise.

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

**Context**: Message consumption requires exactly-once delivery under concurrent consumers. Common approaches include advisory locks, `DELETE ... RETURNING`, or `UPDATE ... RETURNING` with subqueries.

**Rationale**:
- **Non-blocking**: `SKIP LOCKED` allows concurrent consumers to each grab a different message without waiting. Consumers never block each other.
- **Exactly-once delivery**: The row lock ensures only one consumer processes each message within a transaction.
- **PostgreSQL-native**: This is a first-class PostgreSQL feature designed for queue workloads, well-optimized in the query planner.
- **Visibility timeout integration**: Pairs naturally with the visibility timeout pattern: lock the row, set `status='processing'` and `visibility_timeout`, then commit.

**Trade-offs**: Requires the consuming operation to run within a transaction. This is already the natural pattern for pgqueue's consume-then-ack workflow.

---

## ADR-005: Separate Channel and Pub/Sub Schemas

**Status**: Accepted

**Decision**: Channel and pub/sub message tables have fundamentally different schemas rather than a unified table with optional columns.

**Context**: Channels (point-to-point) and pub/sub (fan-out) have different state tracking needs.

**Rationale**:
- **Channels** need per-message state: `status`, `retry_count`, `max_retries`, `visibility_timeout`, `ack_deadline`, `processed_at`, `error_message`. Messages transition through `pending -> processing -> completed/DLQ`.
- **Pub/Sub** messages are immutable once published (`id`, `payload`, `created_at`, `metadata`). Per-subscriber state lives in the `pgqueue_sub_{name}` table, allowing each subscriber to track progress independently.
- **No wasted storage**: Channel tables don't carry subscriber tracking columns; pub/sub tables don't carry retry/status columns.
- **Cleaner queries**: Each pattern's queries are optimized for its specific schema without conditional logic.

**Trade-offs**: Two code paths for table creation and querying, but this reflects a genuine domain difference, not accidental complexity.

---

## ADR-006: Explicit Safety Confirmations for Destructive Operations

**Status**: Accepted

**Decision**: Destructive operations (`ReplayFrom`, `ReplayMessage`, `ReplayDLQ`, `PurgeQueue`, `DeleteChannel`, `DeleteTopic`) require an explicit `confirm=true` or `Confirm: true` parameter.

**Context**: Accidental replays or purges can cause data loss or duplicate processing. A simple boolean guard prevents programmatic accidents while keeping the API ergonomic.

**Rationale**:
- **Defense against copy-paste errors**: Prevents accidental destructive calls in scripts or REPL sessions.
- **Self-documenting**: The `Confirm` field in the call site makes the intent explicit to code reviewers.
- **No external dependency**: Unlike interactive prompts, this works in automated pipelines where stdin is unavailable.

**Trade-offs**: Slightly more verbose call sites, but the cost of accidental data loss far outweighs the minor ergonomic overhead.
