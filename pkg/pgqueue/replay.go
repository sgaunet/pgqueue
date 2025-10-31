package pgqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/sgaunet/pgqueue/internal/db"
)

// ReplayFrom resets messages after a specific timestamp to pending status
func (pq *PGQueue) ReplayFrom(ctx context.Context, queueName string, queueType QueueType, since time.Time, opts ReplayOptions) (int, error) {
	if !opts.Confirm && !opts.DryRun {
		return 0, fmt.Errorf("replay operation requires explicit confirmation or dry-run mode")
	}

	// Get queue metadata
	metadata, err := pq.queries.GetQueueMetadata(ctx, db.GetQueueMetadataParams{
		QueueType: string(queueType),
		QueueName: queueName,
	})
	if err == sql.ErrNoRows {
		return 0, fmt.Errorf("queue not found: %s/%s", queueType, queueName)
	}
	if err != nil {
		return 0, fmt.Errorf("failed to get queue metadata: %w", err)
	}

	tableName := metadata.TableName

	// Build query based on queue type
	var query string
	if queueType == QueueTypeChannel {
		query = fmt.Sprintf(`
			UPDATE pgqueue_msg_%s
			SET status = 'pending',
			    retry_count = 0,
			    visibility_timeout = NULL,
			    ack_deadline = NULL,
			    processed_at = NULL,
			    error_message = NULL
			WHERE created_at >= $1
			AND status != 'pending'
		`, tableName)
	} else {
		// For pub/sub, reset subscriptions
		query = fmt.Sprintf(`
			UPDATE pgqueue_sub_%s
			SET status = 'pending',
			    retry_count = 0,
			    visibility_timeout = NULL,
			    acked_at = NULL,
			    error_message = NULL
			WHERE created_at >= $1
			AND status != 'pending'
		`, tableName)
	}

	// Add limit if specified
	if opts.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", opts.Limit)
	}

	// Dry-run: just count
	if opts.DryRun {
		countQuery := fmt.Sprintf(`
			SELECT COUNT(*) FROM pgqueue_msg_%s
			WHERE created_at >= $1
			AND status != 'pending'
		`, tableName)

		if queueType == QueueTypePubSub {
			countQuery = fmt.Sprintf(`
				SELECT COUNT(*) FROM pgqueue_sub_%s
				WHERE created_at >= $1
				AND status != 'pending'
			`, tableName)
		}

		var count int
		if err := pq.db.QueryRowContext(ctx, countQuery, since).Scan(&count); err != nil {
			return 0, fmt.Errorf("failed to count messages: %w", err)
		}
		return count, nil
	}

	// Execute replay
	result, err := pq.db.ExecContext(ctx, query, since)
	if err != nil {
		return 0, fmt.Errorf("failed to replay messages: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get affected rows: %w", err)
	}

	// Log replay operation
	if opts.PerformedBy != "" {
		if err := pq.logReplay(ctx, queueName, queueType, "timestamp", int(rows), opts.PerformedBy, fmt.Sprintf("since: %s", since)); err != nil {
			// Log error but don't fail the operation
			fmt.Printf("failed to log replay operation: %v\n", err)
		}
	}

	return int(rows), nil
}

// ReplayMessage resets a specific message to pending status
func (pq *PGQueue) ReplayMessage(ctx context.Context, queueName string, queueType QueueType, messageID uuid.UUID, opts ReplayOptions) error {
	if !opts.Confirm && !opts.DryRun {
		return fmt.Errorf("replay operation requires explicit confirmation or dry-run mode")
	}

	// Get queue metadata
	metadata, err := pq.queries.GetQueueMetadata(ctx, db.GetQueueMetadataParams{
		QueueType: string(queueType),
		QueueName: queueName,
	})
	if err == sql.ErrNoRows {
		return fmt.Errorf("queue not found: %s/%s", queueType, queueName)
	}
	if err != nil {
		return fmt.Errorf("failed to get queue metadata: %w", err)
	}

	tableName := metadata.TableName

	if opts.DryRun {
		// Just check if message exists
		checkQuery := fmt.Sprintf(`
			SELECT EXISTS(SELECT 1 FROM pgqueue_msg_%s WHERE id = $1)
		`, tableName)

		var exists bool
		if err := pq.db.QueryRowContext(ctx, checkQuery, messageID).Scan(&exists); err != nil {
			return fmt.Errorf("failed to check message: %w", err)
		}
		if !exists {
			return fmt.Errorf("message not found: %s", messageID)
		}
		return nil
	}

	// Reset message
	query := fmt.Sprintf(`
		UPDATE pgqueue_msg_%s
		SET status = 'pending',
		    retry_count = 0,
		    visibility_timeout = NULL,
		    ack_deadline = NULL,
		    processed_at = NULL,
		    error_message = NULL
		WHERE id = $1
	`, tableName)

	result, err := pq.db.ExecContext(ctx, query, messageID)
	if err != nil {
		return fmt.Errorf("failed to replay message: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get affected rows: %w", err)
	}

	if rows == 0 {
		return fmt.Errorf("message not found: %s", messageID)
	}

	// Log replay operation
	if opts.PerformedBy != "" {
		if err := pq.logReplay(ctx, queueName, queueType, "message_id", 1, opts.PerformedBy, fmt.Sprintf("message_id: %s", messageID)); err != nil {
			fmt.Printf("failed to log replay operation: %v\n", err)
		}
	}

	return nil
}

// ReplayDLQ moves messages from DLQ back to the main queue
func (pq *PGQueue) ReplayDLQ(ctx context.Context, queueName string, queueType QueueType, opts ReplayOptions) (int, error) {
	if !opts.Confirm && !opts.DryRun {
		return 0, fmt.Errorf("replay operation requires explicit confirmation or dry-run mode")
	}

	// Get queue metadata
	metadata, err := pq.queries.GetQueueMetadata(ctx, db.GetQueueMetadataParams{
		QueueType: string(queueType),
		QueueName: queueName,
	})
	if err == sql.ErrNoRows {
		return 0, fmt.Errorf("queue not found: %s/%s", queueType, queueName)
	}
	if err != nil {
		return 0, fmt.Errorf("failed to get queue metadata: %w", err)
	}

	tableName := metadata.TableName

	// Dry-run: just count
	if opts.DryRun {
		countQuery := fmt.Sprintf(`
			SELECT COUNT(*) FROM pgqueue_dlq_%s
		`, tableName)

		var count int
		if err := pq.db.QueryRowContext(ctx, countQuery).Scan(&count); err != nil {
			return 0, fmt.Errorf("failed to count DLQ messages: %w", err)
		}
		return count, nil
	}

	// Begin transaction
	tx, err := pq.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Get DLQ messages
	selectQuery := fmt.Sprintf(`
		SELECT id, original_message_id, payload, metadata
		FROM pgqueue_dlq_%s
	`, tableName)

	if opts.Limit > 0 {
		selectQuery += fmt.Sprintf(" LIMIT %d", opts.Limit)
	}

	rows, err := tx.QueryContext(ctx, selectQuery)
	if err != nil {
		return 0, fmt.Errorf("failed to query DLQ: %w", err)
	}
	defer rows.Close()

	type dlqRow struct {
		id                uuid.UUID
		originalMessageID uuid.UUID
		payload           []byte
		metadata          []byte
	}

	var dlqMessages []dlqRow
	for rows.Next() {
		var msg dlqRow
		if err := rows.Scan(&msg.id, &msg.originalMessageID, &msg.payload, &msg.metadata); err != nil {
			return 0, fmt.Errorf("failed to scan DLQ message: %w", err)
		}
		dlqMessages = append(dlqMessages, msg)
	}

	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("error iterating DLQ messages: %w", err)
	}

	// Insert messages back to main queue
	insertQuery := fmt.Sprintf(`
		INSERT INTO pgqueue_msg_%s (id, payload, created_at, status, retry_count, metadata)
		VALUES ($1, $2, NOW(), 'pending', 0, $3)
		ON CONFLICT (id) DO NOTHING
	`, tableName)

	count := 0
	for _, msg := range dlqMessages {
		result, err := tx.ExecContext(ctx, insertQuery, msg.originalMessageID, msg.payload, msg.metadata)
		if err != nil {
			return 0, fmt.Errorf("failed to insert message: %w", err)
		}

		affected, _ := result.RowsAffected()
		if affected > 0 {
			count++

			// Delete from DLQ
			deleteQuery := fmt.Sprintf(`DELETE FROM pgqueue_dlq_%s WHERE id = $1`, tableName)
			if _, err := tx.ExecContext(ctx, deleteQuery, msg.id); err != nil {
				return 0, fmt.Errorf("failed to delete from DLQ: %w", err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit replay: %w", err)
	}

	// Log replay operation
	if opts.PerformedBy != "" {
		if err := pq.logReplay(ctx, queueName, queueType, "dlq", count, opts.PerformedBy, fmt.Sprintf("replayed %d messages from DLQ", count)); err != nil {
			fmt.Printf("failed to log replay operation: %v\n", err)
		}
	}

	return count, nil
}

// logReplay logs a replay operation to the audit log
func (pq *PGQueue) logReplay(ctx context.Context, queueName string, queueType QueueType, operation string, messageCount int, performedBy, details string) error {
	params, err := json.Marshal(map[string]string{"details": details})
	if err != nil {
		return fmt.Errorf("failed to marshal replay params: %w", err)
	}

	_, err = pq.queries.CreateReplayLog(ctx, db.CreateReplayLogParams{
		QueueType:    string(queueType),
		QueueName:    queueName,
		ReplayType:   operation,
		ReplayParams: params,
		MessageCount: int32(messageCount),
		CreatedBy:    sql.NullString{String: performedBy, Valid: performedBy != ""},
	})
	return err
}

// GetReplayHistory returns the replay history for a queue
func (pq *PGQueue) GetReplayHistory(ctx context.Context, queueName string, queueType QueueType, limit int) ([]db.PgqueueReplayLog, error) {
	if limit <= 0 {
		limit = 100
	}

	return pq.queries.GetReplayHistory(ctx, db.GetReplayHistoryParams{
		QueueType: string(queueType),
		QueueName: queueName,
		Limit:     int32(limit),
	})
}
