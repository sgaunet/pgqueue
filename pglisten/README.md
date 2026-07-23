# pglisten

A pgx-backed `pgqueue.Listener`: push-based delivery for
[pgqueue](../README.md) via PostgreSQL `LISTEN`/`NOTIFY`.

It ships as a separate Go module so the `pgx` dependency stays out of the core
pgqueue dependency graph — consumers who do not want push delivery never pull
it in.

## Why

Without a listener, a blocking consume loop polls. The built-in safety-net poll
interval defaults to **1s** (`pgqueue.WithSafetyNetPoll`, or per-consume
`pgqueue.WithPollInterval`), so an idle queue costs one query per consumer per
second and a message published just after a tick waits up to a full interval.

With a listener registered, publishing emits a `NOTIFY` inside the publishing
transaction and blocked consumers wake as soon as it arrives, instead of on the
next tick.

Push is an optimization, not a correctness requirement: the safety-net poll
always stays active and cannot be turned off. A `NOTIFY` emitted while the
listener is reconnecting is lost, and the poll is what recovers delivery in
that window.

## Requirements

- PostgreSQL 18+
- Go 1.25+

## Install

```bash
go get github.com/sgaunet/pgqueue/pglisten
```

## Wiring

```go
import (
    "github.com/sgaunet/pgqueue"
    "github.com/sgaunet/pgqueue/pglisten"
)

// connString is a standard PostgreSQL connection string (pgx.Connect form).
// The listener opens its own dedicated connection, separate from the *sql.DB.
l, err := pglisten.New(ctx, connString)
if err != nil {
    return err
}

q, err := pgqueue.New(ctx, db, pgqueue.WithListener(l))
if err != nil {
    return err
}
defer q.Close() // closes the listener too
```

**Ownership:** once `WithListener` has been applied, the `Queue` owns the
listener's lifecycle. `q.Close()` stops the GC and the consume loops, then
closes the listener. Do not close the listener separately — and note `Close`
does *not* close your `*sql.DB`, which stays yours.

## Behaviour and defaults

- **Reconnect backoff** (`ReconnectPolicy`, override with
  `WithReconnectPolicy`). The delay before attempt *n* is drawn uniformly from
  `[0, window)` where `window = min(BaseDelay * Multiplier^n, MaxDelay)` — full
  jitter, so many instances recovering at once do not reconnect in lockstep.
  Defaults: `BaseDelay` 1s, `MaxDelay` 30s, `Multiplier` 2. Zero or invalid
  fields fall back to their default individually.
- **Keepalive** (`WithKeepaliveInterval`, default **30s**). Bounds how long the
  listener blocks waiting for a notification before probing the connection with
  `Ping` (probe timeout 5s). This detects a silently-dead TCP connection — NAT
  or firewall idle drop, half-open socket with no RST, hung backend — within
  the interval rather than at the OS-level ~2h TCP timeout. A failed probe
  triggers the normal reconnect flow. The keepalive cannot be disabled.
- **Reconnect visibility.** `WithLogger` logs each failed reconnect attempt and
  each keepalive-probe failure at WARN. `WithOnReconnect(func(attempt int))`
  fires once per successful reconnect — count the invocations for a reconnect
  total; `attempt` is how many connect attempts that one recovery took, and
  resets on every drop.
- **Subscriptions survive reconnects.** Every channel passed to `Listen` is
  remembered and re-issued on a fresh connection. `Listen` blocks until the
  server confirms the `LISTEN`, so a nil return means notifications will be
  delivered.
- **Backpressure.** The notifications channel is buffered (64). When it fills,
  the receive loop blocks rather than dropping a wake, and logs the rising and
  falling edge. pgqueue's own notifier drains it promptly.

## Runnable example

See [`examples/pglisten`](../examples/pglisten) for a complete program that
measures consumer wake latency with and without the listener.
