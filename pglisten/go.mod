module github.com/sgaunet/pgqueue/pglisten

go 1.25.0

require (
	github.com/jackc/pgx/v5 v5.10.0
	github.com/sgaunet/pgqueue v0.0.0
)

require (
	github.com/google/uuid v1.6.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	golang.org/x/text v0.36.0 // indirect
)

replace github.com/sgaunet/pgqueue => ../
