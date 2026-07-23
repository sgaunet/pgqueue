// Package integration holds the integration test suite for pgqueue.
//
// It lives in a separate Go module so that testcontainers-go and the Docker
// SDK it pulls in are not part of the dependency graph of library consumers.
// The package itself is intentionally empty; all code lives in _test.go files.
package integration
