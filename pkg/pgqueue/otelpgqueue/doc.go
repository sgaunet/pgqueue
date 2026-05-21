// Package otelpgqueue provides OpenTelemetry tracing and metrics adapters for
// pgqueue.
//
// It implements the pgqueue.Tracer and pgqueue.MetricsRecorder hook interfaces
// so observability can be enabled via pgqueue.WithTracer and
// pgqueue.WithMetrics, while keeping the OpenTelemetry dependency out of the
// core pgqueue package's dependency graph.
//
// The adapter implementations are added by tasks T049 and T051.
package otelpgqueue
