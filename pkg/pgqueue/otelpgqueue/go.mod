module github.com/sgaunet/pgqueue/pkg/pgqueue/otelpgqueue

go 1.25.0

require (
	github.com/sgaunet/pgqueue v0.0.0
	go.opentelemetry.io/otel v1.39.0
	go.opentelemetry.io/otel/metric v1.39.0
	go.opentelemetry.io/otel/trace v1.39.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
)

replace github.com/sgaunet/pgqueue => ../../../
