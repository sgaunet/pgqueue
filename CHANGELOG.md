# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Changed

### Removed

### Fixed

## [1.0.0] - YYYY-MM-DD

First stable release. pgqueue is a PostgreSQL 18 message-queue library for
Go 1.25+ offering two delivery patterns — **Channels** (point-to-point) and
**Pub/Sub** (fan-out) — with at-least-once delivery. This release makes the
hot paths constant-time at scale, stands up a full CI pipeline, publishes
installable versioned modules, and freezes the public API and on-disk schema.
The entries below describe the library at first release and the deltas from the
pre-release development series.

### Added

- Core PostgreSQL 18 message queue for Go 1.25+ with at-least-once delivery and
  two patterns: **Channels** (point-to-point via `FOR UPDATE SKIP LOCKED`) and
  **Pub/Sub** (fan-out to every subscriber).
- Constant-time consume: a targeted, planner-selectable index serves the claim
  path, so `Receive`/`Consume` cost is bounded by the consumable backlog and does
  not grow with the number of completed or dead-lettered rows the table retains.
- Bounded default garbage collection: the completed-message purge is served by an
  index rather than a full sequential scan, and a GC that bounds table growth is
  enabled out of the box.
- Per-table autovacuum and storage tuning so dead-tuple and index bloat stay
  bounded under sustained claim/ack/nack churn.
- Plan-regression test that fails CI if a future change reintroduces a sequential
  scan or an unbounded historical filter on the consume path.
- Installable, independently versioned optional adapter modules — `pglisten`
  (LISTEN/NOTIFY push delivery), `otelpgqueue` (OpenTelemetry), and `prompgqueue`
  (Prometheus) — each installable with `go get …@v1.0.0`.
- Status-column `CHECK` integrity constraint on channel message tables.
- Bounded pub/sub subscriber fan-out (per-topic subscriber cap and paginated
  fan-out insert) so a single publish cannot materialize the full
  messages × subscribers cross-product in one transaction.
- Goroutine-leak detection (goleak) across Close/Consume/GC lifecycles in the
  concurrency suites.
- A published `CHANGELOG`, a SemVer policy, and a forward-only schema/migration
  compatibility guarantee.
- CI that runs the unit tests and the integration suite against a real
  PostgreSQL 18 container under `-race`, lints, records a coverage baseline, and
  (on release tags) smoke-tests that consumers can `go get` the modules.

### Changed

- `NewGarbageCollector` now returns `(*GarbageCollector, error)` and validates its
  configuration, consistent with the primary `New` constructor.
- The on-disk schema is a single clean baseline at `SchemaVersion = 1`; the
  pre-release migration chain — including its `ACCESS EXCLUSIVE`-lock and
  full-table-scan steps — is squashed away. The forward-only compatibility promise
  begins at v1.0.0.
- Concurrent batch ack/nack lock rows in a canonical order so overlapping
  operations no longer deadlock against each other.
- The timed-out-message reset runs in bounded pages with a bounded lock window,
  like every other bulk GC operation.
- Multi-queue batch ack/nack reflects results already committed for earlier
  queue-groups and surfaces any later-group error, instead of silently discarding
  committed work.
- Shutdown and cleanup paths that intentionally survive caller cancellation are
  bounded by a grace timeout, so a wedged connection cannot hang them forever.
- Single-receipt `Ack`/`Nack` failures carry queue/message context, consistent
  with the batch and other error paths.
- Sibling configuration options validate invalid input consistently, rather than
  some rejecting and some silently clamping.
- The fake/in-memory queue matches the real queue's not-found behavior when
  publishing to an unregistered queue.

### Removed

- Deprecated `PGQueue` type alias.
- `PauseQueue` / `ResumeQueue` stubs.
- Duplicate schema-version receiver method and dead exported types that leaked
  internal configuration shapes.
- Dead and duplicate indexes from the frozen schema.
- Stale "future feature" godoc note on already-shipped functionality.

### Fixed

- Consume no longer degrades to an O(history) index scan on large tables
  (previously ~24 ms/call at 500K rows and growing with history depth).
- The default GC completed-purge no longer performs a full sequential scan every
  cycle (previously ~21 ms/queue/cycle at 300K rows).
- Adapter modules no longer pin core `v0.0.0` behind a local `replace`;
  `go get …/<adapter>@v1.0.0` now resolves the real core version from a clean
  cache without workspace mode.
- The metadata cache stays bounded even when no garbage collector is running.
- Integration-suite flakiness under shared-CI timing: the container startup
  timeout was raised so a slow or loaded runner does not flake a passing run.

[Unreleased]: https://github.com/sgaunet/pgqueue/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/sgaunet/pgqueue/releases/tag/v1.0.0
