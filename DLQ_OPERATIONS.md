# DLQ Operations Runbook

Operator guide for inspecting and replaying the dead-letter queue.

This is the page to read when messages are failing in production and you need to
find out what broke, decide what to put back, and leave an audit trail. It
assumes you already have a `*pgqueue.Queue`; see the [README](README.md) for
setup.

**The one thing to know before you start:** replay **executes by default**.
There is no confirmation flag ([ADR-006](ADR.md#adr-006-destructive-operations-are-explicit-at-the-call-site-no-confirmation-flag)) —
`ReplayOptions{}` means "replay everything, unlimited, unattributed". Set
`DryRun: true` to preview. Gate destructive calls at your own call site.

---

## The loop

| Step | Call | Purpose |
|---|---|---|
| 1. Triage | `DLQStats` | How bad is it? |
| 2. Inspect | `ListDLQMessages` | What actually failed, and why? |
| 3. Preview | `ReplayOptions{DryRun: true}` | What *would* a replay do? |
| 4. Replay | `ReplayDLQ` / `ReplayFrom` / `ReplayMessage` | Put messages back |
| 5. Audit | `ReplayHistory` | Who replayed what, when? |

---

## 1. Triage — how bad is it?

```go
stats, err := pq.DLQStats(ctx, "orders", pgqueue.QueueTypeChannel)
if err != nil {
    return err
}
log.Printf("dlq=%s count=%d oldest=%v avg_retries=%.1f",
    stats.QueueName, stats.TotalCount, stats.OldestMovedAt, stats.AvgRetryCount)
```

`DLQStats` gives you the shape of the problem without reading any payloads:

| Field | Read it as |
|---|---|
| `TotalCount` | Blast radius. |
| `OldestMovedAt` / `NewestMovedAt` | Both `*time.Time`, nil on an empty DLQ. A wide spread means a slow bleed; a tight cluster means one incident. |
| `AvgRetryCount` | Near `max_retries` means messages fought and lost (likely a real bug). Near zero means they failed instantly (likely a poison payload or a dependency that was hard-down). |

Check `OldestMovedAt` against your retention window before anything else — see
[The replay window is bounded](#8-the-replay-window-is-bounded).

---

## 2. Inspect — walk the DLQ

`ListDLQMessages` is keyset-paginated on the UUIDv7 `id` column, so it is
index-friendly and stable under concurrent inserts and deletes. You do not get a
`LIMIT/OFFSET` skew when rows are being added or purged underneath you.

```go
page := pgqueue.DLQPage{Limit: 50}
for {
    msgs, next, err := pq.ListDLQMessages(ctx, "orders", pgqueue.QueueTypeChannel, page)
    if err != nil {
        return err
    }
    if len(msgs) == 0 {
        break // exhausted
    }
    for _, m := range msgs {
        log.Printf("id=%s orig=%s retries=%d moved=%s reason=%s",
            m.ID, m.OriginalMessageID, m.RetryCount, m.MovedAt, m.FailureReason)
    }
    page = next
}
```

`DLQMessage` carries `ID`, `OriginalMessageID`, `Payload`, `FailureReason`,
`RetryCount`, `MovedAt`, and `Metadata`. Triage on `FailureReason` — it is the
error string from the final failed attempt.

**`ID` vs `OriginalMessageID`.** `ID` identifies the *DLQ row*.
`OriginalMessageID` identifies the message on the live queue that failed — that
is the one you pass to `ReplayMessage`. Mixing them up is the most common
mistake here.

### Paging rules

- **`Limit`** — non-positive selects `DefaultDLQPageSize` (100); anything above
  `MaxDLQPageSize` (1000) is **clamped**, not rejected. A huge limit cannot make
  the library pre-allocate an unbounded slice, but it also will not give you
  more than 1000 rows in one call.
- **The cursor is safe on an empty page.** An empty page returns a cursor that
  carries your position forward rather than rewinding to the start of the queue,
  and its `Limit` is the *effective* limit that was applied. So a poller can
  unconditionally reassign `page = next` on every tick:

```go
page := pgqueue.DLQPage{Limit: 50}
for range ticker.C {
    msgs, next, err := pq.ListDLQMessages(ctx, "orders", pgqueue.QueueTypeChannel, page)
    if err != nil {
        continue
    }
    handleNewFailures(msgs)
    page = next // safe even when msgs is empty — the cursor is preserved
}
```

---

## 3. Preview — dry-run semantics differ per method

**This is the most error-prone part of the API.** `DryRun: true` never mutates
anything and never writes a `pgqueue_replay_log` row, but what it *returns*
depends on which call you made.

### `ReplayFrom` + DryRun → a count

Returns how many messages would be replayed, already capped by `Limit`, so the
number matches what a real run would do.

```go
n, err := pq.ReplayFrom(ctx, "orders", pgqueue.QueueTypeChannel, since,
    pgqueue.ReplayOptions{DryRun: true})
// n = messages that would be reinstated
```

### `ReplayMessage` + DryRun → an eligibility check, *not* a count

Returns `nil` if the message could be replayed, or the reason it could not. It
does not tell you "1" — it tells you yes or no.

```go
err := pq.ReplayMessage(ctx, "orders", pgqueue.QueueTypeChannel, origID,
    pgqueue.ReplayOptions{DryRun: true})
switch {
case err == nil:
    // replayable
case errors.Is(err, pgqueue.ErrMessageInProcessing):
    // in flight right now — try later
case errors.Is(err, pgqueue.ErrMessageNotFound):
    // no such message (ErrReplayMessageNotFound wraps this)
}
```

### `ReplayDLQ` + DryRun → real counts, with one caveat

It walks the real paginated path and rolls each page back, so `Replayed` and
`Skipped` match a real run — **except** for duplicates. Because each page is
rolled back, inserts from earlier pages are invisible to later ones. If two DLQ
entries share the same `original_message_id` and land in different pages, a real
run replays the first and skips the second (`ON CONFLICT`), but the dry run
counts **both** as `Replayed`.

So a dry run can **over-report `Replayed`** and under-report `Skipped` when
duplicates exist. A real run is always exact.

---

## 4. Replay — and read the result

Three entry points, and **only one of them reads the DLQ**:

```go
// THE DLQ TOOL. Everything in the DLQ, paginated internally.
res, err := pq.ReplayDLQ(ctx, "orders", pgqueue.QueueTypeChannel, opts)

// One specific message, by its OriginalMessageID. Channels only.
err := pq.ReplayMessage(ctx, "orders", pgqueue.QueueTypeChannel, origID, opts)

// NOT a DLQ tool — see the warning below.
n, err := pq.ReplayFrom(ctx, "orders", pgqueue.QueueTypeChannel, since, opts)
```

`ReplayDLQ` processes the DLQ in keyset-paginated pages, each in its own
transaction, so memory and lock footprint stay bounded no matter how large the
backlog is.

> **`ReplayFrom` will not recover dead-lettered messages.** It resets messages
> still on the *message* table — those created at or after `since` whose status
> is neither `pending` nor `processing`, i.e. already-**completed** ones — back
> to `pending`. But promoting a channel message to the DLQ **deletes its row
> from the message table**, so a dead-lettered message is not there for
> `ReplayFrom` to find.
>
> `ReplayFrom` is a *re-process history* tool ("run last Tuesday's traffic
> again"), not a failure-recovery tool. For failures, use `ReplayDLQ` or
> `ReplayMessage`.

### `Skipped` is not a failure

```go
res, err := pq.ReplayDLQ(ctx, "orders", pgqueue.QueueTypeChannel,
    pgqueue.ReplayOptions{PerformedBy: "sylvain@example.com"})
// res.Replayed — reinstated onto the live queue
// res.Skipped  — examined but not replayable; LEFT IN THE DLQ
```

A row is skipped when it cannot be safely reinstated — its original message is
still live on the queue, or (pub/sub) it has no active subscriber, or the
message row was removed by some other means (manual SQL, `PurgeQueue`). Skipped
rows **stay in the DLQ**; one stale entry can never abort the whole replay.

If `res.Replayed` is lower than the `TotalCount` you saw in triage, `Skipped`
is where the difference went. That is usually correct behavior, not a bug.

---

## 5. `Limit` — the footgun

For **`ReplayDLQ`**, `Limit` caps the number of DLQ rows **examined**
(`Replayed + Skipped`) — *not* the number replayed.

This matters: a DLQ full of un-replayable rows returns promptly with
`Replayed: 0` rather than scanning the whole table. That is deliberate (it
bounds the work), but it means **`Limit: 100` does not promise you 100 replayed
messages.** It promises at most 100 rows looked at.

```go
// Cautious first batch: look at 100 rows, see what happens.
res, err := pq.ReplayDLQ(ctx, "orders", pgqueue.QueueTypeChannel,
    pgqueue.ReplayOptions{Limit: 100, PerformedBy: "oncall"})

// Then go unbounded once you trust the outcome.
res, err = pq.ReplayDLQ(ctx, "orders", pgqueue.QueueTypeChannel,
    pgqueue.ReplayOptions{Limit: pgqueue.ReplayAll, PerformedBy: "oncall"})
```

`ReplayAll` (0, the zero value) means no cap. A negative `Limit` is rejected
with `ErrInvalidConfig`.

**Always run a limited batch before an unbounded one.** Replaying a large DLQ
puts every message back on the live queue at once; if the underlying dependency
is still broken, they will all fail again and march straight back to the DLQ,
burning their retries on the way.

---

## 6. Audit — label it, then read it back

Every real (non-dry-run) replay writes a row to `pgqueue_replay_log`. Set
`PerformedBy` so that row means something six months from now:

```go
res, err := pq.ReplayDLQ(ctx, "orders", pgqueue.QueueTypeChannel,
    pgqueue.ReplayOptions{PerformedBy: "sylvain@example.com"})
```

Use an operator email, a service name, or a JWT subject. Constraints: at most
`MaxPerformedByLen` (256) bytes, and **no control characters** — the value is
stored verbatim and read back through `psql` and log pipelines, so NUL, CR, LF,
and friends are rejected with `ErrInvalidPerformedBy`. It is bound as a query
parameter, so this is not an injection guard; it is there to keep the audit
table greppable.

Read the trail back:

```go
logs, err := pq.ReplayHistory(ctx, "orders", pgqueue.QueueTypeChannel, 20)
for _, l := range logs {
    who := "<unattributed>"
    if l.CreatedBy != nil {
        who = *l.CreatedBy
    }
    log.Printf("%s  %s  count=%d  by=%s", l.CreatedAt, l.ReplayType, l.MessageCount, who)
}
```

`ReplayHistory` returns most recent first. A `limit <= 0` selects
`DefaultReplayHistoryLimit` (100). `CreatedBy` is `*string` and is nil for
replays run without `PerformedBy` — which is exactly why you should always set
it.

**Dry runs write no audit row.** If you need a record that someone *looked*,
log it yourself.

---

## 7. Pub/sub restrictions

`ReplayMessage` is **channel-only**. On a topic it returns
`ErrReplayNotSupported` — use `ReplayFrom` or `ReplayDLQ` instead.

For pub/sub, replay re-creates the failed subscriber's subscription row, which
foreign-keys the original message row. That row is kept alive for you: the
garbage collector never purges a message while a DLQ entry still references it.
The DLQ entry is always reaped first, so `CompletedMessageTTL` may safely be
shorter than `DLQRetention` — which is the usual arrangement.

An in-flight message cannot be replayed at all, on either queue type:
`ErrMessageInProcessing`. Wait for its visibility timeout to lapse, or for the
consumer to ack/nack, and retry.

---

## 8. The replay window is bounded

**DLQ messages are not recoverable forever.** If you run a `GarbageCollector`,
it deletes DLQ rows older than `RetentionPolicy.DLQRetention` — **30 days by
default**.

That default is applied even when you do not configure a policy: an all-zero
`DefaultPolicy` is treated as unconfigured and replaced with the built-in
defaults (`CompletedMessageTTL` 24h, `DLQRetention` 30d) so that a GC bounds
table growth out of the box.

Two consequences:

- Size `DLQRetention` against your triage SLA. If nobody looks at the DLQ for
  six weeks, the evidence is gone.
- To keep DLQ entries indefinitely, set `DLQRetention: pgqueue.KeepForever`.
  Note that `0` also disables the purge — but an *all-zero* policy is what
  triggers the defaulting above, so `KeepForever` (`-1`) is the sentinel that
  reliably means "never purge" while leaving the struct non-zero.

Check `stats.OldestMovedAt` during triage. If it is close to your retention
horizon, replay or export before you investigate.

---

## 9. Replay is not purge

Do not confuse these.

- **Replay** puts messages *back on the queue* to be processed again.
- **`PurgeQueue` deletes them.** It is immediate, irreversible, takes no
  confirmation flag, and lives on the **`*GarbageCollector`**, not on `*Queue`:

```go
err := gc.PurgeQueue(ctx, "orders", pgqueue.QueueTypeChannel)
```

Same for `DeleteChannel` / `DeleteTopic`, which drop the tables outright. There
is no undo. Gate them at your call site.

---

## Error reference

| Error | Meaning | Operator action |
|---|---|---|
| `ErrReplayMessageNotFound` | No such message. Wraps `ErrMessageNotFound`. | Check you passed `OriginalMessageID`, not the DLQ row's `ID`. |
| `ErrMessageInProcessing` | The message is in flight, or raced out of `processing` mid-replay. | Wait for the visibility timeout or the ack/nack, then retry. |
| `ErrReplayNotSupported` | `ReplayMessage` called on a pub/sub topic. | Use `ReplayFrom` or `ReplayDLQ`. |
| `ErrInvalidPerformedBy` | `PerformedBy` exceeds 256 bytes or contains control characters. | Sanitize the label. |
| `ErrInvalidConfig` | Negative `ReplayOptions.Limit`. | Use `ReplayAll` (0) for unlimited. |
| `ErrQueueNotFound` / `ErrTopicNotFound` | Unknown queue name. | Check the name and queue type. |

Match with `errors.Is`, never on the error string.
