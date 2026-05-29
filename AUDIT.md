# pgqueue Pre-Release Design & Correctness Audit

**Date:** 2026-05-28
**Branch:** `002-post-hardening-remediation`
**Scope:** Whole-codebase scan for future problems — API design, concurrency/correctness, SQL/schema/scaling — with an emphasis on issues that would be expensive to fix *after* a 1.0 release.

> **Method.** Three parallel audits were run (API design, concurrency/correctness, SQL/schema/scaling). Every high-severity claim was then **verified against the actual source** before inclusion here. The raw audits over-reported; this report keeps only verified-real issues.

---

## 1. Executive summary

The library is in good shape. The hardest correctness paths (retry/DLQ boundary, visibility-timeout reclaim, OR-query indexing, table-name collision avoidance) are already correct and, in several cases, defended by explanatory comments. The remaining real issues are mostly **API-shape decisions** that are cheap to make now and expensive to change after release, plus a few **robustness/observability** hardening items and **operability docs**.

| Tier | Theme | Items |
|------|-------|-------|
| 1 | Breaking API — lock in before 1.0 | ~~#A1 DB interface~~ ✅, ~~#A2 batch results~~ ✅, ~~#A3 Listener contract~~ ✅ |
| 2 | Robustness — non-breaking | ~~#B4 BIGINT counters~~ ✅, ~~#B5 invalid-index handling~~ ✅, #B6 unlock logging |
| 3 | Operability / docs | #C7 queue ceiling, #C8 polling cost, #C9 DLQ/TTL FK hazard |

User-flagged priorities for the eventual remediation pass: **#A2 (batch results)** and **#A3 (Listener contract)** — both now resolved (#133, #134).

---

## 2. Findings

### Tier 1 — API shape (cheap now, breaking later)

#### A1. `*sql.DB` is hard-wired into the public constructors — ✅ RESOLVED (#132)
- **Where:** `InitSchema(ctx, db *sql.DB, ...)` (pgqueue.go:144); `New(ctx, db *sql.DB, ...)` (pgqueue.go:324).
- **Problem:** Concrete `*sql.DB` in the public signature locks consumers in forever. Any future need to accept a pool wrapper, a transaction-scoped handle, or a test double becomes a breaking change post-1.0.
- **Recommendation:** Accept a minimal interface that `*sql.DB` already satisfies (the context-aware `ExecContext`/`QueryContext`/`QueryRowContext`/`BeginTx` set). `*sql.DB` passes unchanged, so it is a no-op for existing callers but unblocks future flexibility.
- **Cost to defer:** High. This is the single most expensive-to-change API decision.
- **Resolution:** Added an exported `DB` interface (`ExecContext`/`QueryContext`/`QueryRowContext`/`BeginTx`/`PingContext`/`Conn`) used by `InitSchema`, `New`, `checkDBReady`, the `Queue.db` field, and `runMigrations`. `*sql.DB` satisfies it unchanged (asserted with `var _ DB = (*sql.DB)(nil)`), so existing callers compile as-is. `PingContext`/`Conn` are included because the readiness check and the session-level advisory-lock migration runner require them; `Conn`/`BeginTx` still yield concrete `*sql.Conn`/`*sql.Tx`.

#### A2. Batch ack/nack collapse partial success into a single error *(user priority)* — ✅ RESOLVED (#133)
- **Where:** `AckBatch` / `NackBatch` and their `ackChannelBatch`/`ackTopicBatch` helpers (`batch.go`). The multi-row `UPDATE ... FROM unnest($1::text::uuid[], $2::text::uuid[])` returns one aggregate error; `rows == 0` is used as total-failure signal.
- **Problem:** When acking N receipts where some claims expired (or never matched), the caller cannot learn *which* succeeded and which need retry. A partial success can even surface as `ErrMessageAlreadyAcked`, implying everything failed.
- **Recommendation:** Return a structured result, e.g.
  ```go
  type BatchResult struct {
      Succeeded int
      Failed    []FailedReceipt // receipt + reason (ErrClaimExpired, ErrMessageNotFound, ...)
  }
  ```
  This is a breaking signature change best made before release.
- **Resolution:** `AckBatch`/`NackBatch` now return `(BatchResult, error)` where `BatchResult{Succeeded []Receipt; Failed []FailedReceipt}` and `FailedReceipt{Receipt; Reason error}`. Each failed receipt carries `ErrClaimExpired` / `ErrMessageAlreadyAcked` / `ErrMessageNotFound`, classified identically to the single-receipt path (`classifyClaimState`). Partial success is no longer an error; `error` is reserved for operational failures and aborts the call.

#### A3. `Listener.Listen(ctx, channel) error` looks synchronous but is fire-and-forget *(user priority)* — ✅ RESOLVED (#134)
- **Where:** `Listener` interface in `notify.go`; implementation in `pglisten/listener.go` (the `ctx` parameter is ignored — `_ context.Context` — and the actual `LISTEN` is issued asynchronously in the background `run()` goroutine).
- **Problem:** A `nil` return does **not** mean the subscription succeeded. If the background goroutine fails to issue `LISTEN` (e.g. a dropped connection), delivery silently degrades to the safety-net poll, which may be seconds/minutes away. The `ctx` parameter is a red herring suggesting blocking semantics.
- **Recommendation:** Either (a) make `Listen` synchronously confirm the `LISTEN` succeeded before returning, or (b) rename / document it explicitly as fire-and-forget and drop the misleading `ctx`. Decide before the interface is public.
- **Resolution:** Took option (a). `pglisten`'s `Listen` now blocks until the `LISTEN` is confirmed on the backend session (or `ctx` fires), honoring `ctx` and returning a meaningful error. The notifier confirms off the consume hot path via a per-channel `confirmListen` goroutine, which surfaces real `LISTEN` failures through the existing `#68` WARN/ERROR escalation.

---

### Tier 2 — robustness hardening (non-breaking)

#### B4. `retry_count` / `max_retries` are 32-bit `INT` — ✅ RESOLVED (#135)
- **Where:** channel message table DDL (pgqueue.go ~:1086, `max_retries INT CHECK (... >= 0)`); increment in Go `int` at channel.go:383.
- **Problem:** The column only checks `>= 0`. A pathological crash-loop could overflow the counter; once `retry_count` wraps negative the exhaustion test `retryCount > channelMaxRetries(...)` becomes unreliable, so a message could dodge the DLQ.
- **Recommendation:** Use `BIGINT` for the counters and/or add an explicit overflow guard before incrementing. Trivial as a forward-only migration now; annoying later.
- **Resolution:** All four per-queue retry counters (`pgqueue_msg_*.retry_count` / `.max_retries`, `pgqueue_sub_*.retry_count`, `pgqueue_dlq_*.retry_count`) are now `BIGINT`, raising the overflow ceiling from ~2.1×10⁹ to ~9.2×10¹⁸. Fresh queues are born `BIGINT`; the forward-only v4 migration (`migrateBigintRetryCounts`) widens pre-existing per-queue tables, skipping columns already `BIGINT` for idempotency. Internal `max_retries` scan targets moved `sql.NullInt32`→`sql.NullInt64`; no public API change (`RetryCount`/`MaxRetries` were already `int`). No explicit Go guard added — `BIGINT` makes the wrap unreachable.

#### B5. A failed `CREATE INDEX` leaves an invalid index that `IF NOT EXISTS` skips forever
- **Where:** per-queue index creation (pgqueue.go:1113-1157) and the v2 migration index (migrations.go). All use `CREATE INDEX IF NOT EXISTS`.
- **Problem:** If an index creation is interrupted (statement timeout, crash, or a future `CREATE INDEX CONCURRENTLY`), PostgreSQL leaves an **invalid** index (`pg_index.indisvalid = false`). A later `IF NOT EXISTS` silently sees the name and skips it forever, so GC/consume queries quietly degrade to scans with no signal.
- **Recommendation:** Detect invalid leftovers (query `pg_index.indisvalid`) and drop+recreate, or add a startup validity check that logs/repairs.
- **Resolution:** Added a catalog-driven repair pass (`index_repair.go`). A single query finds every invalid `idx_pgqueue_*` index in the schema and captures its exact recreate DDL via `pg_get_indexdef` — so no DDL registry is needed and it covers global, channel, sub, DLQ, and any future indexes; the `idx_pgqueue_` prefix filter deliberately excludes `*_pkey` constraint indexes. Each invalid index is dropped and recreated. The pass runs **automatically on every `InitSchema`** (not just when a migration is pending), reusing the advisory-locked migration connection so concurrent startups serialize — this is the "startup validity check that logs/repairs". A new public `Queue.RepairIndexes(ctx) (RepairResult, error)` exposes the same pass for long-running processes that self-heal without restarting. `InitSchema` now also accepts `WithLogger` (in addition to `WithSchema`) so the repair pass can report what it fixed. Repair is best-effort: an index that cannot be recreated is reported in `RepairResult.Failed` and logged rather than bricking startup; only a detection-query failure aborts. The common case (no invalid index) costs one catalog query and issues no DDL.

#### B6. Migration advisory-unlock failure is swallowed
- **Where:** migrations.go:397 — `_, _ = conn.ExecContext(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", ...)`.
- **Problem:** Safe in practice (the lock is session-scoped and released when the pooled connection's session ends), but a genuine unlock failure is invisible, making a stuck-lock scenario undiagnosable.
- **Recommendation:** Log the unlock error at WARN. Optional: explicitly close/discard the migration connection rather than returning it to the pool.

---

### Tier 3 — operability / documentation (no code risk, real surprises)

#### C7. Table-per-queue has no documented ceiling
- **Context:** Each queue creates 2–3 tables plus 5–7 indexes (`createIndexes`, `createDLQTable`, sub-table DDL). Thousands of queues means thousands of tables and tens of thousands of indexes → `pg_class`/`pg_index` bloat, autovacuum-of-catalog pressure, planner overhead.
- **Recommendation:** Document a recommended practical limit and surface guidance near the optional `maxQueues` cap / `ErrMaxQueuesReached`.

#### C8. Polling is the default consume path; cost scales with consumers × queues
- **Context:** Default poll loop (consume.go) issues a query per poll per consumer even when empty. The `pglisten` LISTEN/NOTIFY adapter exists but is opt-in.
- **Recommendation:** Document the polling cost trade-off prominently and point high-fan-in users at `pglisten`.

#### C9. Pub/Sub DLQ foreign-key hazard when `CompletedMessageTTL < DLQRetention`
- **Where:** sub table `FOREIGN KEY (message_id) REFERENCES pgqueue_msg_<queue>(id) ON DELETE CASCADE` (pgqueue.go ~:998); warning doc comment in replay.go.
- **Problem:** If the message TTL is shorter than the DLQ retention, the parent message row is purged before the DLQ entry, cascading away DLQ rows / breaking replay. Currently only a doc comment guards this.
- **Recommendation:** Validate `DLQRetention <= CompletedMessageTTL` for pub/sub at config time (or document as a hard constraint).

---

## 3. Suggested remediation order (for a later pass)

1. **Tier 1 (breaking, before 1.0):** ✅ DONE — A2 batch results (#133) → A3 Listener contract (#134) → A1 DB interface (#132). *(A2/A3 first per user.)*
2. **Tier 2 (non-breaking):** B4 BIGINT counters → B5 invalid-index handling → B6 unlock logging.
3. **Tier 3 (docs/validation):** C7 → C8 → C9.

### Verification plan for those changes
- `task tests` (root, no Docker) and `task tests-integration` (testcontainers/Docker) must stay green.
- **A1:** compile-check that `*sql.DB` satisfies the new interface; `go build ./...` across all six workspace modules (pglisten/otel/prom/examples).
- **A2:** integration test acking a batch mixing valid + expired/foreign receipts, asserting the per-receipt breakdown.
- **B4/B5:** add a forward-only migration; run against fresh `postgres:18-alpine`; re-run `InitSchema` for idempotency; artificially invalidate an index and confirm repair.
- `task lint` (golangci-lint, config `.golangci.yml`) clean on all touched files.

---

## 4. Appendix — files inspected

`pgqueue.go`, `channel.go`, `pubsub.go`, `consume.go`, `publish.go`, `batch.go`, `queries.go`, `pgparam.go`, `gc.go`, `replay.go`, `migrations.go`, `errors.go`, `config.go`, `options.go`, `types.go`, `notify.go`, `pglisten/listener.go`.
