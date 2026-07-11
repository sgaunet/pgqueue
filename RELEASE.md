# Release process

pgqueue is a Go multi-module workspace: the core module `github.com/sgaunet/pgqueue`
(at the repository root) plus optional adapter modules that version independently
but track the core major. Releasing is **ordered** — the core tag must exist
before the adapters can require it — so follow these steps in sequence.

This is the maintainer runbook for the human-gated release step. Nothing here is
applied ahead of time: the adapter `go.mod` edits in step 2 cannot be
`go mod tidy`'d until the core tag exists, so they are performed at release time,
not committed in advance.

## 0. Preconditions

- [ ] All P1 CI gates are green on `main`: unit tests, the integration suite
      under `-race`, the per-module build+lint matrix, and the toolchain matrix
      (see `.github/workflows/`).
- [ ] `CHANGELOG.md` has a `## [1.0.0]` entry with the real date filled in
      (replace the `YYYY-MM-DD` placeholder).
- [ ] The README "Versioning and compatibility" section is merged.
- [ ] `git tag -l` does not yet contain the version you are cutting.
- [ ] You are on `main`, at the commit you intend to release.

## 1. Tag the core module first

The core module lives at the repository root, so its tag is a bare version:

```bash
git tag v1.0.0
git push origin v1.0.0
```

Wait until `v1.0.0` is visible on the remote (and resolvable via your `GOPROXY`,
if you use one) before continuing — the adapter bumps below resolve it.

## 2. Bump each adapter to require the real core version

In each consumer-facing module — `pglisten/`, `otelpgqueue/`, `prompgqueue/`, and
`examples/` — edit `go.mod`:

1. Set the core requirement to the version you just pushed:

   ```
   require github.com/sgaunet/pgqueue v1.0.0
   ```

2. **Remove** the consumer-facing local replace directive:

   ```
   replace github.com/sgaunet/pgqueue => ../      # delete this line
   ```

3. Tidy each module against the now-published core:

   ```bash
   for m in pglisten otelpgqueue prompgqueue examples; do (cd "$m" && go mod tidy); done
   ```

> **Leave `internal/integration` alone.** It carries local replaces
> (`… => ../../` for core and `…/pglisten => ../../pglisten`) but is an
> `internal/`, never-published module used only by CI and local dev — its
> replaces stay. Only the four consumer-facing modules above drop theirs.

Commit the adapter `go.mod`/`go.sum` changes on `main` and confirm CI is green.

## 3. Tag the adapters

Go submodule tags are `<module-path>/vX.Y.Z`:

```bash
git tag pglisten/v1.0.0
git tag otelpgqueue/v1.0.0
git tag prompgqueue/v1.0.0
git push origin pglisten/v1.0.0 otelpgqueue/v1.0.0 prompgqueue/v1.0.0
```

`examples/` is neither published nor imported, so it is not tagged.

## 4. Verify from a clean consumer

`go.work` is not tracked, so a fresh clone resolves modules honestly. Run the
smoke test (also wired as the `goget-smoke` workflow) to prove no adapter drags
the core in at `v0.0.0`:

```bash
scripts/goget-smoke.sh v1.0.0
```

Or manually, from a throwaway module with no `go.work` in scope:

```bash
tmp=$(mktemp -d); cd "$tmp"; go mod init smoketest
GOWORK=off GOFLAGS=-mod=mod go get github.com/sgaunet/pgqueue@v1.0.0
GOWORK=off GOFLAGS=-mod=mod go get github.com/sgaunet/pgqueue/pglisten@v1.0.0
GOWORK=off GOFLAGS=-mod=mod go get github.com/sgaunet/pgqueue/otelpgqueue@v1.0.0
GOWORK=off GOFLAGS=-mod=mod go get github.com/sgaunet/pgqueue/prompgqueue@v1.0.0
go build ./...   # each adapter must pull core v1.0.0, not v0.0.0
```

## Versioning & compatibility policy

- **SemVer.** An exported-API breaking change is a major bump; additive changes
  are minor; fixes are patch. Optional adapters version independently but track
  the core major.
- **Schema.** `InitSchema` runs a forward-only migration chain from the `v1.0.0`
  baseline (`SchemaVersion = 1`). Upgrades append migrations and bump the version;
  released migrations are never edited or reordered; downgrades are unsupported; a
  database newer than the binary yields `ErrSchemaTooNew`. The pre-1.0 squash was
  a one-time reset and does not recur.

See `CHANGELOG.md` for per-release detail.
