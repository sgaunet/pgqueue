<!--
SYNC IMPACT REPORT
==================
Version change: 1.0.0 → 2.0.0
Rationale: Amendment reconciling the constitution with the shipped, deliberately
  idiomatic API and the pre-release schema-baseline reset
  (feature 003-v1-release-readiness).

Modified principles:
  - IV. Safe-by-Default Destructive Operations — REDEFINED mechanism (MAJOR):
    caller-provided confirmation parameters replaced by execute-by-default plus a
    ReplayOptions.DryRun preview and call-site-gated Purge/Delete. Core intent
    preserved (no accidental data loss, at-least-once delivery, the
    retryCount+1 > maxRetry DLQ guard, keep-forever retention semantics).
  - V. Forward-Only Schema Evolution — EXPANDED (MINOR): added a one-time,
    pre-release-only baseline-reset exception that permits squashing migrations
    v1–v8 and resetting SchemaVersion before the first v1.0.0 tag. Forward-only
    binds permanently from v1.0.0 onward.
  - I. Library-First & Consumer-Dependency Minimalism — CLARIFIED (PATCH): public
    API path corrected from `pkg/pgqueue` to the repository root (package `pgqueue`).
Added sections: none
Removed sections: none

Templates requiring updates:
  ✅ .specify/templates/plan-template.md — generic "Constitution Check" gate; compatible
  ✅ .specify/templates/spec-template.md — no principle-driven mandatory sections changed
  ✅ .specify/templates/tasks-template.md — task categories compatible
  ✅ .specify/templates/checklist-template.md — generic; no change required
  ✅ README.md — no `confirm`/`pkg/pgqueue` references; already states Go 1.25+
  ✅ CLAUDE.md — "Destructive operations" section already documents the DryRun/no-confirm API

Follow-up TODOs: none (prior README "Go 1.21+" TODO resolved — README now states Go 1.25+).
-->

# pgqueue Constitution

## Core Principles

### I. Library-First & Consumer-Dependency Minimalism

pgqueue is a library, not an application. The public API lives at the repository
root (package `pgqueue`) and MUST remain importable via
`go get github.com/sgaunet/pgqueue` without pulling
in test-only or operational tooling. Test-only dependencies (e.g. testcontainers,
the Docker SDK) MUST stay isolated in the nested `internal/integration` module so
they never enter a consumer's dependency graph. Any new dependency added to the
root module MUST be justified as required at runtime by library consumers.

**Rationale**: Consumers embed pgqueue into their own services; a bloated or
Docker-coupled dependency tree degrades their build, audit, and supply-chain
posture.

### II. Test-First & Integration-Verified

Behavioral changes MUST be covered by tests before they are considered complete.
Logic that touches PostgreSQL MUST be verified by integration tests in
`internal/integration` running against `postgres:18-alpine` via testcontainers.
Integration tests MUST pass with the race detector and repeated runs
(`go test ./... -race -count=2`). Pure logic with no database interaction MAY be
covered by unit tests in the root module. A change that alters delivery semantics,
retry behavior, or schema MUST NOT merge without a corresponding integration test.

**Rationale**: Queue correctness (at-least-once delivery, visibility timeouts,
DLQ transitions) only manifests against a real PostgreSQL instance; race-detected,
repeated runs catch concurrency defects that single passes hide.

### III. Direct, Parameterized SQL — No ORM, No Codegen

All database access MUST use parameterized SQL via `database/sql`. ORMs and SQL
code-generation tools are prohibited. Query methods MUST accept an optional
`*sql.Tx` to support caller-managed transactions. Code MUST function with both the
`pgx` and `lib/pq` drivers. User-supplied identifiers MUST be validated against
`^[a-zA-Z0-9_-]+$` and passed through `sanitizeTableName()`; values MUST always be
bound as parameters, never string-interpolated.

**Rationale**: Direct SQL keeps behavior explicit and auditable, preserves
driver portability, and — combined with strict identifier validation — closes the
SQL-injection surface inherent to a table-per-queue design.

### IV. Safe-by-Default Destructive Operations

Destructive and data-rewriting operations MUST be explicit and hard to trigger by
accident, and their semantics MUST be documented where a caller first meets them.
The library expresses this idiomatically rather than through confirmation flags:

- **Replay** (`ReplayFrom()`, `ReplayMessage()`, `ReplayDLQ()`) executes by default
  and MUST offer a non-mutating preview via `ReplayOptions.DryRun`; a dry run MUST
  NOT modify any data.
- **Immediate, irreversible** operations (`PurgeQueue()`, `DeleteChannel()`,
  `DeleteTopic()`) take no confirmation parameter and are gated by the caller at the
  call site; their irreversibility MUST be documented in the README and godoc.

The at-least-once delivery contract MUST be upheld: messages are redelivered on
visibility-timeout expiry, so the library MUST NOT silently drop or
double-acknowledge messages. Retry-to-DLQ promotion MUST use the
`retryCount+1 > maxRetry` guard. In a `RetentionPolicy`, a zero field means "keep
forever" and MUST NOT trigger deletion; the exported `KeepForever` sentinel is the
only permitted negative duration.

**Rationale**: A queue is a system of record in transit; accidental purges, replays,
or off-by-one DLQ moves cause irreversible data loss for consumers. Confirmation
*flags* were removed in favor of an idiomatic API — execute-by-default with an
explicit `DryRun` preview for replay, call-site gating for deletes — that is clearer
and harder to misuse than a boolean parameter. The safety obligation is met through
documented semantics and the preserved delivery guarantees, not ceremony.

### V. Forward-Only Schema Evolution

Schema changes MUST be expressed as append-only entries in the ordered
`migrations` slice (`migrations.go`); the `SchemaVersion` constant MUST be bumped
in the same change. Migrations MUST be forward-only, idempotent where possible,
serialized via a PostgreSQL advisory lock, and each MUST run in its own
transaction. Existing migrations MUST NOT be edited or reordered once released.

**One-time pre-release exception**: while the library has never been tagged (no
released version has applied the chain), the migration set MAY be collapsed into a
single fresh baseline — including resetting the `SchemaVersion` constant — because
no consumer has run it. This exception is available ONLY before the first `v1.0.0`
tag and MUST NOT recur afterward; from `v1.0.0` onward the append-only,
bump-in-the-same-change, and no-edit-once-released rules bind permanently.

All primary keys and time-ordered identifiers MUST use UUIDv7 — `DEFAULT uuidv7()`
in SQL and `NewUUIDv7()` in Go; `gen_random_uuid()` is prohibited.

**Rationale**: Consumers run `InitSchema()` against live databases with no
external migration tooling; forward-only, immutable migrations make upgrades
deterministic, and UUIDv7 provides the chronological ordering the queue depends on.

## Technology Standards & Constraints

- **Runtime targets**: PostgreSQL 18+ and Go 1.25+. Features MUST NOT depend on
  newer versions without a constitution amendment.
- **Module layout**: Root module `github.com/sgaunet/pgqueue` for the library;
  nested `internal/integration` module for Docker-backed tests; `go.work` ties
  them locally and a `replace` directive keeps integration tests CI-standalone.
- **Linting**: `golangci-lint` MUST pass using the committed `.golangci.yml`
  (`default: all` with selective disables). New lint failures introduced by a
  change MUST be fixed before merge; pre-existing failures are out of scope.
- **Table-per-queue**: Each queue owns `pgqueue_msg_{name}`, `pgqueue_dlq_{name}`,
  and (pubsub only) `pgqueue_sub_{name}`; global tables are created by
  `InitSchema()`. Initialization order is `InitSchema()` → `Init()` →
  `CreateChannel()`/`CreateTopic()`.

## Development Workflow & Quality Gates

- **Spec-driven changes**: New capabilities, breaking changes, and significant
  performance or security work follow the Spec Kit / OpenSpec workflow
  (`openspec/AGENTS.md`) before implementation.
- **Gates before merge**: `go build ./...`, `golangci-lint run`, root-module unit
  tests, and the `internal/integration` suite (`-race -count=2`) MUST all pass.
- **Public API changes**: Any change to exported identifiers in the root `pgqueue`
  package MUST be reflected in doc comments and the README, and assessed for
  semantic-version impact.
- **Review**: Every PR MUST be reviewed for compliance with these principles;
  deviations MUST be recorded in the plan's Complexity Tracking with a justified
  rationale or the change MUST be revised.

## Governance

This constitution supersedes ad-hoc practices for the pgqueue repository. All
pull requests and reviews MUST verify compliance with the Core Principles; any
unavoidable deviation MUST be documented and justified in the implementation
plan's Complexity Tracking table.

Amendments MUST be proposed via pull request, MUST update this file, and MUST
re-verify dependent templates under `.specify/templates/`. Versioning of this
constitution follows semantic versioning:

- **MAJOR**: Backward-incompatible governance changes or removal/redefinition of
  a principle.
- **MINOR**: A new principle or section is added, or guidance is materially
  expanded.
- **PATCH**: Clarifications, wording, and non-semantic refinements.

Compliance is reviewed on every change. Runtime development guidance for AI
assistants and contributors lives in `CLAUDE.md` and `openspec/AGENTS.md`; those
files MUST stay consistent with this constitution.

**Version**: 2.0.0 | **Ratified**: 2026-05-21 | **Last Amended**: 2026-07-11
