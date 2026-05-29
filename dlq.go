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

// defaultDLQPageSize is the page size used by ListDLQMessages when DLQPage.Limit
// is not set to a positive value.
const defaultDLQPageSize = 100

// DLQPage is a keyset-pagination cursor for ListDLQMessages. Start with the zero
// value; on each call pass back the DLQPage returned by the previous call to
// fetch the next page. Keyset pagination on the UUIDv7 id column is index-
// friendly and stable under concurrent inserts and deletes (R8).
type DLQPage struct {
	// AfterID returns only rows with a greater id. The zero value starts at the
	// beginning.
	AfterID uuid.UUID
	// Limit caps the rows per page; <= 0 means defaultDLQPageSize.
	Limit int
}

// ListDLQMessages returns one keyset-paginated page of dead-letter messages,
// ordered by id, together with the cursor for the next page. When the returned
// page has fewer rows than the requested limit, the dead-letter queue is
// exhausted.
func (pq *Queue) ListDLQMessages(
	ctx context.Context,
	name string,
	queueType QueueType,
	page DLQPage,
) ([]DLQMessage, DLQPage, error) {
	if err := pq.checkClosed(); err != nil {
		return nil, DLQPage{}, err
	}

	limit := page.Limit
	if limit <= 0 {
		limit = defaultDLQPageSize
	}

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
		msg, scanErr := pq.scanDLQMessage(name, rows)
		if scanErr != nil {
			return nil, DLQPage{}, scanErr
		}
		messages = append(messages, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, DLQPage{}, fmt.Errorf("error iterating DLQ messages: %w", err)
	}

	next := DLQPage{Limit: limit}
	if len(messages) > 0 {
		next.AfterID = messages[len(messages)-1].ID
	}
	return messages, next, nil
}

// scanDLQMessage scans one DLQ row into a DLQMessage. queue is the logical
// queue/topic name used to label any RecordMetadataParseError observation.
func (pq *Queue) scanDLQMessage(queue string, rows rowScanner) (DLQMessage, error) {
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
	msg.Metadata = pq.parseMetadataJSON(queue, metadataJSON)
	return msg, nil
}

// rowScanner is the subset of *sql.Rows / *sql.Row used by scanDLQMessage.
type rowScanner interface {
	Scan(dest ...any) error
}
