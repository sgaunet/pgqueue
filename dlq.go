package pgqueue

// dlq.go holds the dead-letter-queue listing API: keyset-paginated listing of
// dead-letter messages (FR-022). Aggregate DLQ statistics are in stats.go
// (DLQStats).

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
)

// DefaultDLQPageSize is the page size used by ListDLQMessages when
// DLQPage.Limit is not set to a positive value.
const DefaultDLQPageSize = 100

// MaxDLQPageSize caps DLQPage.Limit. A larger value is clamped to it rather
// than rejected, so a mistaken huge limit cannot pre-allocate an unbounded
// result slice (mirrors the maxConcurrency clamp in consume.go).
const MaxDLQPageSize = 1000

// DLQPage is a keyset-pagination cursor for ListDLQMessages. Start with the zero
// value; on each call pass back the DLQPage returned by the previous call to
// fetch the next page — including when the returned page was empty: the
// cursor returned alongside an empty page still carries the incoming AfterID
// forward (rather than resetting to the beginning), and its Limit is filled
// in with the effective limit that was actually applied, so it is always
// safe and directly reusable for the next call. Keyset pagination on the
// UUIDv7 id column is index-friendly and stable under concurrent inserts and
// deletes (R8).
type DLQPage struct {
	// AfterID returns only rows with a greater id. The zero value starts at the
	// beginning.
	AfterID uuid.UUID
	// Limit caps the rows per page. A non-positive value falls back to
	// DefaultDLQPageSize; a value greater than MaxDLQPageSize is clamped to it.
	Limit int
}

// resolveDLQLimit resolves the DLQPage.Limit a caller supplied into the
// effective per-page row limit: non-positive values fall back to
// DefaultDLQPageSize, and values above MaxDLQPageSize are clamped down to it.
// It is a pure function so the limit-resolution rules can be unit-tested
// without a database.
func resolveDLQLimit(n int) int {
	if n <= 0 {
		return DefaultDLQPageSize
	}
	if n > MaxDLQPageSize {
		return MaxDLQPageSize
	}
	return n
}

// ListDLQMessages returns one keyset-paginated page of dead-letter messages,
// ordered by id, together with the cursor for the next page. When the returned
// page has fewer rows than the requested limit, the dead-letter queue is
// exhausted. An empty page still returns a usable cursor: AfterID carries the
// incoming cursor forward instead of rewinding to the beginning, so polling
// callers can unconditionally reassign page = next after every call.
func (pq *Queue) ListDLQMessages(
	ctx context.Context,
	name string,
	queueType QueueType,
	page DLQPage,
) ([]DLQMessage, DLQPage, error) {
	if err := pq.checkClosed(); err != nil {
		return nil, DLQPage{}, err
	}

	limit := resolveDLQLimit(page.Limit)

	tableName, err := pq.cachedTableName(ctx, string(queueType), name)
	if err != nil {
		return nil, DLQPage{}, fmt.Errorf("%s/%s: %w", queueType, name, err)
	}

	// A NULL after-id (the zero UUID) starts at the beginning; otherwise only
	// rows strictly after the cursor are returned.
	var afterID any
	if page.AfterID != (uuid.UUID{}) {
		afterID = page.AfterID
	}

	// The table name comes from a queueNameRegex-validated queue name, so this
	// interpolation is injection-safe.
	query := fmt.Sprintf(`
		SELECT id, original_message_id, payload, failure_reason,
		       retry_count, moved_at, metadata
		FROM %s
		WHERE ($1::uuid IS NULL OR id > $1)
		ORDER BY id
		LIMIT $2
	`, pq.dlqTable(tableName))

	rows, err := pq.db.QueryContext(ctx, query, afterID, limit)
	if err != nil {
		return nil, DLQPage{}, fmt.Errorf("failed to query DLQ: %w", err)
	}
	defer func() { _ = rows.Close() }()

	messages := make([]DLQMessage, 0, limit)
	for rows.Next() {
		msg, scanErr := pq.scanDLQMessage(ctx, name, rows)
		if scanErr != nil {
			return nil, DLQPage{}, scanErr
		}
		messages = append(messages, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, DLQPage{}, fmt.Errorf("error iterating DLQ messages: %w", err)
	}

	// Seed next with the incoming cursor so an empty page preserves it rather
	// than rewinding to the beginning (uuid.Nil means "start from the
	// beginning" — see the afterID handling above). Limit carries forward the
	// effective limit, not the possibly-zero page.Limit the caller passed in,
	// so a caller that started from the zero value gets back a Limit it can
	// compare against len(msgs) on the very next call.
	next := DLQPage{Limit: limit, AfterID: page.AfterID}
	if len(messages) > 0 {
		next.AfterID = messages[len(messages)-1].ID
	}
	return messages, next, nil
}

// scanDLQMessage scans one DLQ row into a DLQMessage. queue is the logical
// queue/topic name used to label any RecordMetadataParseError observation;
// ctx is the caller's context, passed through to that observation.
func (pq *Queue) scanDLQMessage(ctx context.Context, queue string, rows rowScanner) (DLQMessage, error) {
	var (
		msg          DLQMessage
		metadataJSON sql.NullString
	)
	if err := rows.Scan(
		&msg.ID, &msg.OriginalMessageID, &msg.Payload,
		&msg.FailureReason, &msg.RetryCount, &msg.MovedAt, &metadataJSON,
	); err != nil {
		return DLQMessage{}, fmt.Errorf("failed to scan DLQ message: %w", err)
	}
	msg.Metadata = pq.parseMetadataJSON(ctx, queue, metadataJSON)
	return msg, nil
}

// rowScanner is the subset of *sql.Rows / *sql.Row used by scanDLQMessage.
type rowScanner interface {
	Scan(dest ...any) error
}
