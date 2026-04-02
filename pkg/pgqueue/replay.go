package pgqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ReplayFrom resets messages after a specific timestamp to pending status.
func (pq *PGQueue) ReplayFrom(
	ctx context.Context,
	queueName string,
	queueType QueueType,
	since time.Time,
	opts ReplayOptions,
) (int, error) {
	if err := validateReplayOpts(opts); err != nil {
		return 0, err
	}

	metadata, err := pq.getReplayQueueMetadata(ctx, queueType, queueName)
	if err != nil {
		return 0, err
	}

	tableName := metadata.TableName

	if opts.DryRun {
		return pq.countReplayableMessages(
			ctx, tableName, queueType, since,
		)
	}

	count, err := pq.executeReplayFrom(
		ctx, tableName, queueType, since, opts.Limit,
	)
	if err != nil {
		return 0, err
	}

	pq.logReplayIfNeeded(
		ctx, queueName, queueType, "timestamp",
		count, opts.PerformedBy,
		fmt.Sprintf("since: %s", since),
	)

	return count, nil
}

// ReplayMessage resets a specific message to pending status.
func (pq *PGQueue) ReplayMessage(
	ctx context.Context,
	queueName string,
	queueType QueueType,
	messageID uuid.UUID,
	opts ReplayOptions,
) error {
	if err := validateReplayOpts(opts); err != nil {
		return err
	}

	metadata, err := pq.getReplayQueueMetadata(ctx, queueType, queueName)
	if err != nil {
		return err
	}

	tableName := metadata.TableName

	if opts.DryRun {
		return pq.checkMessageExists(ctx, tableName, messageID)
	}

	if err := pq.executeReplayMessage(
		ctx, tableName, messageID,
	); err != nil {
		return err
	}

	pq.logReplayIfNeeded(
		ctx, queueName, queueType, "message_id",
		1, opts.PerformedBy,
		fmt.Sprintf("message_id: %s", messageID),
	)

	return nil
}

// ReplayDLQ moves messages from DLQ back to the main queue.
func (pq *PGQueue) ReplayDLQ(
	ctx context.Context,
	queueName string,
	queueType QueueType,
	opts ReplayOptions,
) (int, error) {
	if err := validateReplayOpts(opts); err != nil {
		return 0, err
	}

	metadata, err := pq.getReplayQueueMetadata(ctx, queueType, queueName)
	if err != nil {
		return 0, err
	}

	tableName := metadata.TableName

	if opts.DryRun {
		return pq.countDLQMessages(ctx, tableName)
	}

	count, err := pq.executeReplayDLQ(ctx, tableName, opts.Limit)
	if err != nil {
		return 0, err
	}

	pq.logReplayIfNeeded(
		ctx, queueName, queueType, "dlq",
		count, opts.PerformedBy,
		fmt.Sprintf("replayed %d messages from DLQ", count),
	)

	return count, nil
}

// GetReplayHistory returns the replay history for a queue.
func (pq *PGQueue) GetReplayHistory(
	ctx context.Context,
	queueName string,
	queueType QueueType,
	limit int,
) ([]ReplayLog, error) {
	if limit <= 0 {
		limit = 100
	}

	return pq.getReplayHistory(
		ctx, string(queueType), queueName, limit,
	)
}

func validateReplayOpts(opts ReplayOptions) error {
	if !opts.Confirm && !opts.DryRun {
		return ErrConfirmationRequired
	}

	return nil
}

func (pq *PGQueue) getReplayQueueMetadata(
	ctx context.Context,
	queueType QueueType,
	queueName string,
) (*QueueMetadata, error) {
	metadata, err := pq.getQueueMetadata(
		ctx, string(queueType), queueName,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf(
			"%s/%s: %w", queueType, queueName, ErrQueueNotFound,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get queue metadata: %w", err)
	}

	return metadata, nil
}

func (pq *PGQueue) countReplayableMessages(
	ctx context.Context,
	tableName string,
	queueType QueueType,
	since time.Time,
) (int, error) {
	var countQuery string
	if queueType == QueueTypePubSub {
		countQuery = fmt.Sprintf(`
			SELECT COUNT(*) FROM pgqueue_sub_%s
			WHERE created_at >= $1
			AND status != 'pending'
		`, tableName)
	} else {
		countQuery = fmt.Sprintf(`
			SELECT COUNT(*) FROM pgqueue_msg_%s
			WHERE created_at >= $1
			AND status != 'pending'
		`, tableName)
	}

	var count int
	if err := pq.db.QueryRowContext(
		ctx, countQuery, since,
	).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count messages: %w", err)
	}

	return count, nil
}

func (pq *PGQueue) executeReplayFrom(
	ctx context.Context,
	tableName string,
	queueType QueueType,
	since time.Time,
	limit int,
) (int, error) {
	query := pq.buildReplayFromQuery(tableName, queueType, limit)

	result, err := pq.db.ExecContext(ctx, query, since)
	if err != nil {
		return 0, fmt.Errorf("failed to replay messages: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get affected rows: %w", err)
	}

	return int(rows), nil
}

func (pq *PGQueue) buildReplayFromQuery(
	tableName string,
	queueType QueueType,
	limit int,
) string {
	if queueType == QueueTypeChannel {
		return fmt.Sprintf(`
			UPDATE pgqueue_msg_%s
			SET status = 'pending',
			    retry_count = 0,
			    visibility_timeout = NULL,
			    ack_deadline = NULL,
			    processed_at = NULL,
			    error_message = NULL
			WHERE created_at >= $1
			AND status != 'pending'
		`, tableName) + buildLimitSuffix(limit)
	}

	return fmt.Sprintf(`
		UPDATE pgqueue_sub_%s
		SET status = 'pending',
		    retry_count = 0,
		    visibility_timeout = NULL,
		    acked_at = NULL,
		    error_message = NULL
		WHERE created_at >= $1
		AND status != 'pending'
	`, tableName) + buildLimitSuffix(limit)
}

func buildLimitSuffix(limit int) string {
	if limit > 0 {
		return fmt.Sprintf(" LIMIT %d", limit)
	}

	return ""
}

func (pq *PGQueue) checkMessageExists(
	ctx context.Context,
	tableName string,
	messageID uuid.UUID,
) error {
	//nolint:gosec // G201: table name validated by queueNameRegex
	checkQuery := fmt.Sprintf(`
		SELECT EXISTS(SELECT 1 FROM pgqueue_msg_%s WHERE id = $1)
	`, tableName)

	var exists bool
	if err := pq.db.QueryRowContext(
		ctx, checkQuery, messageID,
	).Scan(&exists); err != nil {
		return fmt.Errorf("failed to check message: %w", err)
	}
	if !exists {
		return fmt.Errorf("%s: %w", messageID, ErrReplayMessageNotFound)
	}

	return nil
}

func (pq *PGQueue) executeReplayMessage(
	ctx context.Context,
	tableName string,
	messageID uuid.UUID,
) error {
	//nolint:gosec // G201: table name validated by queueNameRegex
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
		return fmt.Errorf("%s: %w", messageID, ErrReplayMessageNotFound)
	}

	return nil
}

func (pq *PGQueue) countDLQMessages(
	ctx context.Context,
	tableName string,
) (int, error) {
	countQuery := "SELECT COUNT(*) FROM pgqueue_dlq_" + tableName

	var count int
	if err := pq.db.QueryRowContext(ctx, countQuery).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count DLQ messages: %w", err)
	}

	return count, nil
}

func (pq *PGQueue) executeReplayDLQ(
	ctx context.Context,
	tableName string,
	limit int,
) (int, error) {
	tx, err := pq.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	dlqMessages, err := pq.fetchDLQMessages(ctx, tx, tableName, limit)
	if err != nil {
		return 0, err
	}

	count, err := pq.reinsertDLQMessages(ctx, tx, tableName, dlqMessages)
	if err != nil {
		return 0, err
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit replay: %w", err)
	}

	return count, nil
}

type dlqRow struct {
	id                uuid.UUID
	originalMessageID uuid.UUID
	payload           []byte
	metadata          []byte
}

func (pq *PGQueue) fetchDLQMessages(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	limit int,
) ([]dlqRow, error) {
	//nolint:gosec // G201: table name validated by queueNameRegex
	selectQuery := fmt.Sprintf(`
		SELECT id, original_message_id, payload, metadata
		FROM pgqueue_dlq_%s
	`, tableName)

	if limit > 0 {
		selectQuery += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := tx.QueryContext(ctx, selectQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query DLQ: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var dlqMessages []dlqRow
	for rows.Next() {
		var msg dlqRow
		if err := rows.Scan(
			&msg.id, &msg.originalMessageID,
			&msg.payload, &msg.metadata,
		); err != nil {
			return nil, fmt.Errorf("failed to scan DLQ message: %w", err)
		}
		dlqMessages = append(dlqMessages, msg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating DLQ messages: %w", err)
	}

	return dlqMessages, nil
}

func (pq *PGQueue) reinsertDLQMessages(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	dlqMessages []dlqRow,
) (int, error) {
	//nolint:gosec // G201: table name validated by queueNameRegex
	insertQuery := fmt.Sprintf(`
		INSERT INTO pgqueue_msg_%s
			(id, payload, created_at, status, retry_count, metadata)
		VALUES ($1, $2, NOW(), 'pending', 0, $3)
		ON CONFLICT (id) DO NOTHING
	`, tableName)

	count := 0
	for _, msg := range dlqMessages {
		result, err := tx.ExecContext(
			ctx, insertQuery,
			msg.originalMessageID, msg.payload, msg.metadata,
		)
		if err != nil {
			return 0, fmt.Errorf("failed to insert message: %w", err)
		}

		affected, _ := result.RowsAffected()
		if affected > 0 {
			count++

			//nolint:gosec // G201: table name validated by queueNameRegex
			deleteQuery := fmt.Sprintf(
				`DELETE FROM pgqueue_dlq_%s WHERE id = $1`, tableName,
			)
			if _, err := tx.ExecContext(ctx, deleteQuery, msg.id); err != nil {
				return 0, fmt.Errorf("failed to delete from DLQ: %w", err)
			}
		}
	}

	return count, nil
}

// logReplay logs a replay operation to the audit log.
func (pq *PGQueue) logReplay(
	ctx context.Context,
	queueName string,
	queueType QueueType,
	operation string,
	messageCount int,
	performedBy, details string,
) error {
	params, err := json.Marshal(map[string]string{"details": details})
	if err != nil {
		return fmt.Errorf("failed to marshal replay params: %w", err)
	}

	var createdBy *string
	if performedBy != "" {
		createdBy = &performedBy
	}

	return pq.createReplayLog(
		ctx, string(queueType), queueName,
		operation, params, messageCount, createdBy,
	)
}

func (pq *PGQueue) logReplayIfNeeded(
	ctx context.Context,
	queueName string,
	queueType QueueType,
	operation string,
	count int,
	performedBy, details string,
) {
	if performedBy == "" {
		return
	}

	if err := pq.logReplay(
		ctx, queueName, queueType,
		operation, count, performedBy, details,
	); err != nil {
		fmt.Printf("failed to log replay operation: %v\n", err)
	}
}
