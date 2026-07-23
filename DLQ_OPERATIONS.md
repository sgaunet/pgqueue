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
| 4. Replay | `ReplayDLQ` | Put failed messages back — the only call that reads the DLQ |
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
| `AvgRetryCount` | **Not a failure signal.** A message only reaches the DLQ once `retry_count + 1 > max_retries`, and it is stored with that final count — so for a single retry cap this is always exactly `max_retries + 1`. It is a *config* discriminator: the cap is resolved at publish time and stamped on the row, so an average that is not `max_retries + 1` means the DLQ mixes rows published under different caps — someone changed `WithQueueMaxRetries` or `WithDefaultMaxRetries` between them. |

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
`RetryCount`, `MovedAt`, and `Metadata`. Triage on `FailureReason`, with two
caveats:

- It is **truncated to 1024 bytes** and sanitized to valid UTF-8 (invalid byte
  sequences become `�`). A stack trace or a marshalled upstream response is cut
  off; do not expect the whole thing to be there.
- It is the handler's error only when the message was **nacked**. A message that
  was dead-lettered because its visibility timeout lapsed once too often — a
  consumer that crashed, hung, or never acked — carries the fixed string
  `exceeded max retries: not acknowledged before visibility timeout`. The
  handler's own error is not recorded in that case, and every such row looks
  identical. Grepping `FailureReason` will not distinguish one crash-loop from
  another; correlate on `MovedAt` and your application logs instead.

**`ID` vs `OriginalMessageID`.** `ID` identifies the *DLQ row* — it is the
keyset cursor and the unit `ReplayDLQ` works on. `OriginalMessageID` is the id
the message *used to have* on the live queue: dead-lettering **deletes the
message row**, so nothing on the live queue carries that id any more. Treat it
as a correlation key for your logs, traces, and dedup records — not as a handle
you can pass to `ReplayMessage` (see [§4](#4-replay--and-read-the-result)).

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
does not tell you "1" — it tells you yes or no. Note the id is a **live**
message id, not a `DLQMessage.OriginalMessageID` (see
[§4](#4-replay--and-read-the-result)).

```go
err := pq.ReplayMessage(ctx, "orders", pgqueue.QueueTypeChannel, msgID,
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
// THE DLQ TOOL — the only one. Everything in the DLQ, paginated internally.
res, err := pq.ReplayDLQ(ctx, "orders", pgqueue.QueueTypeChannel, opts)

// NOT DLQ tools — they read the live message table. See the warning below.
n, err := pq.ReplayFrom(ctx, "orders", pgqueue.QueueTypeChannel, since, opts)
err := pq.ReplayMessage(ctx, "orders", pgqueue.QueueTypeChannel, msgID, opts)
```

`ReplayDLQ` processes the DLQ in keyset-paginated pages, each in its own
transaction, so memory and lock footprint stay bounded no matter how large the
backlog is.

> **Neither `ReplayFrom` nor `ReplayMessage` can recover a dead-lettered
> message.** Both operate on the *message* table: `ReplayFrom` resets rows
> created at or after `since` whose status is neither `pending` nor
> `processing` — i.e. already-**completed** ones — back to `pending`, and
> `ReplayMessage` does the same for one id. But promoting a channel message to
> the DLQ **deletes its row from the message table**, so a dead-lettered
> message is not there for either of them to find. `ReplayMessage` on a
> `DLQMessage.OriginalMessageID` therefore always returns
> `ErrReplayMessageNotFound`, however healthy the DLQ row is.
>
> They are *re-process history* tools — `ReplayFrom` for a time range ("run
> last Tuesday's traffic again"), `ReplayMessage` as its single-id sibling for
> one already-completed message. Neither is a failure-recovery tool.
> **For anything in the DLQ, use `ReplayDLQ`.** (`ReplayMessage` is also
> rejected outright on a topic: `ErrReplayNotSupported`.)

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

Every real (non-dry-run) replay writes to `pgqueue_replay_log` — **one row per
internal page of 100**, not one row per replay. `ReplayDLQ` and `ReplayFrom`
walk the backlog in pages of 100 and commit each page's audit row inside that
page's transaction, so the trail stays consistent with what was actually
reinstated even if the process dies mid-replay. A 1000-message replay leaves
~10 rows of `MessageCount: 100`, not one row of 1000, and a page that
reinstated nothing writes no row at all — so a replay that skipped everything
leaves **no trace**. `ReplayMessage` writes a single row with
`MessageCount: 1`. **Always SUM `MessageCount` across rows; never read one row
as the total.**

Set `PerformedBy` so those rows mean something six months from now:

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
logs, err := pq.ReplayHistory(ctx, "orders", pgqueue.QueueTypeChannel, 100)
var total int64
for _, l := range logs {
    who := "<unattributed>"
    if l.CreatedBy != nil {
        who = *l.CreatedBy
    }
    log.Printf("%s  %s  count=%d  by=%s", l.CreatedAt, l.ReplayType, l.MessageCount, who)
    total += l.MessageCount // pages, so sum — one row is not the whole replay
}
```

`ReplayHistory` returns most recent first. A `limit <= 0` selects
`DefaultReplayHistoryLimit` (100); size it against *pages*, not replays — a
single 5000-message replay already accounts for ~50 rows. `CreatedBy` is
`*string` and is nil for replays run without `PerformedBy` — which is exactly
why you should always set it.

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

In-flight (`processing`) rows are never replayed, but only `ReplayMessage`
*tells* you so — it returns `ErrMessageInProcessing` (channels only; it is the
sole source of that error). `ReplayFrom` silently **excludes** `pending` and
`processing` rows from its candidate set on both queue types: no error, they
simply do not appear in the count. If `ReplayFrom` reinstated fewer messages
than you expected, in-flight rows are one reason why.

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
- **`PurgeQueue` deletes them.** It is immediate, irreversible, and takes no
  confirmation flag. It exists on **both** `*Queue` and `*GarbageCollector`;
  the `*GarbageCollector` method just forwards, so call it on the `*Queue` —
  purging needs no garbage collector.

```go
err := pq.PurgeQueue(ctx, "orders", pgqueue.QueueTypeChannel)
```

> **`PurgeQueue` wipes the DLQ too — including for channels.** It empties the
> queue *and* deletes every row of `pgqueue_dlq_{name}` in the same transaction,
> for a point-to-point channel exactly as much as for a topic (a topic
> additionally loses every subscription row). The tables survive; their contents
> do not.
>
> This is the single most dangerous call in this runbook. Purging a jammed live
> queue mid-incident destroys **all** the failure evidence the rest of this page
> exists to help you read: payloads, `FailureReason`, `MovedAt`, everything. If
> the DLQ rows still matter, `ListDLQMessages` and export them — or `ReplayDLQ`
> them — **before** you purge.

`DeleteChannel` / `DeleteTopic` go further. In one transaction they drop the
per-queue tables outright (message, DLQ, and for topics the subscription table),
remove the `pgqueue_metadata` row and — for topics — the `pgqueue_subscribers`
registrations, **and** delete every `pgqueue_replay_log` row for that queue. So
the audit trail from [§6](#6-audit--label-it-then-read-it-back) goes with it.
Export anything you need to keep first. There is no undo for any of this. Gate
it at your call site.

---

## Error reference

| Error | Meaning | Operator action |
|---|---|---|
| `ErrReplayMessageNotFound` | `ReplayMessage` found no such row **on the live message table**. Wraps `ErrMessageNotFound`. | If the message is in the DLQ this is expected and unavoidable — dead-lettering deleted the message row, so `ReplayMessage` can never reach it. Use `ReplayDLQ`. Otherwise the id is simply wrong or the message was purged/expired. |
| `ErrMessageInProcessing` | `ReplayMessage` only: the message is in flight, or raced out of `processing` mid-replay. | Wait for the visibility timeout or the ack/nack, then retry. (`ReplayFrom` skips in-flight rows silently instead.) |
| `ErrReplayNotSupported` | `ReplayMessage` called on a pub/sub topic. | Use `ReplayFrom` (live messages) or `ReplayDLQ` (dead-lettered ones). |
| `ErrInvalidPerformedBy` | `PerformedBy` exceeds 256 bytes or contains control characters. | Sanitize the label. |
| `ErrInvalidConfig` | Negative `ReplayOptions.Limit`. | Use `ReplayAll` (0) for unlimited. |
| `ErrQueueNotFound` / `ErrTopicNotFound` | Unknown queue name. | Check the name and queue type. |

Match with `errors.Is`, never on the error string.
