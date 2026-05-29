package pgqueue

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
)

// RepairResult reports the outcome of an index-validity repair pass — both the
// automatic pass InitSchema runs at startup and an explicit RepairIndexes call.
type RepairResult struct {
	// Repaired lists, by index name, the invalid indexes that were dropped and
	// successfully recreated.
	Repaired []string
	// Failed lists invalid indexes that were detected but could not be
	// recreated; the underlying error is logged. A non-empty Failed does not
	// fail the pass — the index is left for a later attempt rather than
	// blocking startup.
	Failed []string
}

// indexRepairer is the minimal database surface repairInvalidIndexes needs.
// Both the package DB interface (so the public RepairIndexes can use pq.db) and
// *sql.Conn (so InitSchema can repair on the advisory-locked migration
// connection) satisfy it.
type indexRepairer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// invalidIndex is a pgqueue index whose build never completed
// (pg_index.indisvalid = false), captured with the DDL needed to recreate it.
type invalidIndex struct {
	schema string // namespace the index lives in
	name   string // index relname
	def    string // pg_get_indexdef output: the full CREATE INDEX statement
}

// findInvalidIndexesSQL detects pgqueue's own invalid secondary indexes in a
// single catalog query. pg_get_indexdef returns the exact CREATE INDEX
// statement even for an invalid index, so the recreate DDL needs no registry
// and automatically covers every index shape (global, channel, sub, DLQ, and
// any added later). The LIKE is anchored to the idx_pgqueue_ prefix (with $
// escaping the literal underscores) so it never matches primary-key or unique
// constraint indexes, which are named <table>_pkey and must not be dropped and
// recreated this way.
const findInvalidIndexesSQL = `
	SELECT n.nspname, c.relname, pg_get_indexdef(i.indexrelid)
	FROM pg_index i
	JOIN pg_class c ON c.oid = i.indexrelid
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE NOT i.indisvalid
	  AND n.nspname = $1
	  AND c.relname LIKE 'idx$_pgqueue$_%' ESCAPE '$'`

// RepairIndexes scans the configured schema for pgqueue indexes left invalid by
// an interrupted build (a crash mid-CREATE INDEX, or a future CREATE INDEX
// CONCURRENTLY) and repairs each by dropping and recreating it. An invalid index
// is one PostgreSQL recorded but never finished building
// (pg_index.indisvalid = false); because every pgqueue index is created with
// CREATE INDEX IF NOT EXISTS, such a leftover is skipped forever and consume/GC
// queries silently fall back to sequential scans until it is rebuilt.
//
// InitSchema runs this same pass automatically at startup, so most callers never
// need it. Call RepairIndexes directly from a long-running process that wants to
// self-heal without restarting. The common case — no invalid indexes — costs a
// single catalog query and issues no DDL.
//
// Repair is best-effort: an index that cannot be recreated is reported in
// RepairResult.Failed and logged, but does not fail the call. A non-nil error
// means the detection query itself failed.
//
// Note that recreating an index takes the same locks as the original CREATE
// INDEX, briefly blocking writes to that one table while it rebuilds.
func (pq *Queue) RepairIndexes(ctx context.Context) (RepairResult, error) {
	if err := pq.checkClosed(); err != nil {
		return RepairResult{}, err
	}

	return repairInvalidIndexes(ctx, pq.db, pq.cfg.schemaName, pq.logger)
}

// repairIndexesAfterMigrations runs the index-repair pass on the advisory-locked
// migration connection and logs a one-line summary. A detection-query failure is
// returned (it signals a real DB problem); a single index that cannot be
// recreated is reported in Failed and logged inside repairInvalidIndexes rather
// than bricking InitSchema, so this returns nil in that case.
func repairIndexesAfterMigrations(
	ctx context.Context, ir indexRepairer, schema string, logger *slog.Logger,
) error {
	result, err := repairInvalidIndexes(ctx, ir, schema, logger)
	if err != nil {
		return fmt.Errorf("failed to repair invalid indexes: %w", err)
	}
	if logger != nil && (len(result.Repaired) > 0 || len(result.Failed) > 0) {
		logger.Warn("pgqueue: index repair pass complete",
			"repaired", len(result.Repaired), "failed", len(result.Failed))
	}

	return nil
}

// repairInvalidIndexes finds and rebuilds pgqueue's invalid indexes in schema.
// It runs on any indexRepairer: the public RepairIndexes passes the Queue's DB
// handle, while runMigrations passes its advisory-locked migration connection so
// the repair serializes with concurrent startups. logger may be nil.
func repairInvalidIndexes(
	ctx context.Context, ir indexRepairer, schema string, logger *slog.Logger,
) (RepairResult, error) {
	if schema == "" {
		schema = defaultSchemaName
	}

	invalid, err := findInvalidIndexes(ctx, ir, schema)
	if err != nil {
		return RepairResult{}, err
	}

	var result RepairResult
	for _, idx := range invalid {
		if err := recreateIndex(ctx, ir, idx); err != nil {
			result.Failed = append(result.Failed, idx.name)
			if logger != nil {
				logger.Error("pgqueue: failed to repair invalid index",
					"index", idx.name, "schema", idx.schema, "err", err)
			}

			continue
		}
		result.Repaired = append(result.Repaired, idx.name)
		if logger != nil {
			logger.Warn("pgqueue: repaired invalid index",
				"index", idx.name, "schema", idx.schema)
		}
	}

	return result, nil
}

// findInvalidIndexes runs the detection query and fully drains the result set
// before any DDL is issued: the migration connection processes one statement at
// a time, so the cursor must close before recreateIndex runs on the same conn.
func findInvalidIndexes(
	ctx context.Context, ir indexRepairer, schema string,
) ([]invalidIndex, error) {
	rows, err := ir.QueryContext(ctx, findInvalidIndexesSQL, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to scan for invalid indexes: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []invalidIndex
	for rows.Next() {
		var idx invalidIndex
		if err := rows.Scan(&idx.schema, &idx.name, &idx.def); err != nil {
			return nil, fmt.Errorf("failed to read invalid index row: %w", err)
		}
		out = append(out, idx)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate invalid indexes: %w", err)
	}

	return out, nil
}

// recreateIndex drops the invalid leftover and recreates it from the captured
// definition. DROP is schema-qualified so it resolves regardless of the
// connection's search_path; the pg_get_indexdef text is already schema-qualified
// as needed, so the rebuilt index lands in the same table and schema as before.
func recreateIndex(ctx context.Context, ir indexRepairer, idx invalidIndex) error {
	// The drop target is quoted catalog identifiers for a pgqueue-owned index
	// (idx_pgqueue_*), and the definition is PostgreSQL's own pg_get_indexdef
	// output for that same index — neither is user-supplied SQL.
	drop := fmt.Sprintf("DROP INDEX IF EXISTS %s.%s",
		quoteIdent(idx.schema), quoteIdent(idx.name))
	if _, err := ir.ExecContext(ctx, drop); err != nil {
		return fmt.Errorf("drop %q: %w", idx.name, err)
	}
	if _, err := ir.ExecContext(ctx, idx.def); err != nil {
		return fmt.Errorf("recreate %q: %w", idx.name, err)
	}

	return nil
}

// quoteIdent renders s as a double-quoted SQL identifier, doubling any embedded
// double quote, so a catalog-sourced schema or index name interpolates safely.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
