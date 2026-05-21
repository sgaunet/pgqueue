// Package prompgqueue provides a Prometheus metrics adapter for pgqueue.
//
// It implements the pgqueue.MetricsRecorder hook interface so metrics can be
// enabled via pgqueue.WithMetrics, while keeping the Prometheus client
// dependency out of the core pgqueue package's dependency graph.
//
// The adapter implementation is added by tasks T050 and T051.
package prompgqueue
