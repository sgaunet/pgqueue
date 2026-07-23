#!/usr/bin/env bash
# goget-smoke.sh — prove that a downstream consumer can `go get` pgqueue and each
# optional adapter from a clean module cache with NO go.work in scope, and that
# no adapter drags the core module in at the placeholder v0.0.0 (the unresolved
# `replace` trap this release removes, H3/FR-013/SC-006).
#
# Usage:
#   scripts/goget-smoke.sh [version]
#     version defaults to $GOGET_SMOKE_VERSION, else "latest".
#
# Before v1.0.0 is tagged there is no release to resolve, so run it with
# "latest" (it will resolve a pseudo-version) — the v0.0.0 assertion is the real
# gate and holds regardless of the version argument. After tagging, run it with
# "v1.0.0" as a release gate.
set -euo pipefail

VERSION="${1:-${GOGET_SMOKE_VERSION:-latest}}"
CORE="github.com/sgaunet/pgqueue"
ADAPTERS=(pglisten otelpgqueue prompgqueue)

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
cd "$tmp"

# A throwaway module with no go.work in scope, so `go get` must resolve real
# published versions rather than the workspace's local `replace` directives.
export GOWORK=off
export GOFLAGS=-mod=mod

go mod init smoketest >/dev/null

echo ">> go get ${CORE}@${VERSION}"
go get "${CORE}@${VERSION}"
for a in "${ADAPTERS[@]}"; do
	echo ">> go get ${CORE}/${a}@${VERSION}"
	go get "${CORE}/${a}@${VERSION}"
done

# A tiny program importing each module so `go build` must actually compile them.
cat >main.go <<'EOF'
package main

import (
	_ "github.com/sgaunet/pgqueue"
	_ "github.com/sgaunet/pgqueue/otelpgqueue"
	_ "github.com/sgaunet/pgqueue/pglisten"
	_ "github.com/sgaunet/pgqueue/prompgqueue"
)

func main() {}
EOF

go build ./...

# The core module must NOT resolve to the placeholder v0.0.0 — which is what an
# adapter with an unresolved `replace` directive would fall back to.
core_version="$(go list -m -f '{{.Version}}' "${CORE}")"
echo ">> resolved ${CORE} ${core_version}"
if [ "${core_version}" = "v0.0.0" ] || [ -z "${core_version}" ]; then
	echo "FAIL: ${CORE} resolved to '${core_version}': adapters are not pulling a real published core version." >&2
	exit 1
fi

echo "OK: core and all adapters resolve and build (${CORE} ${core_version})."
