package pgqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

// parseMetadataJSON parses a nullable JSON string into a metadata map.
func parseMetadataJSON(s sql.NullString) map[string]any {
	if !s.Valid || s.String == "" {
		return nil
	}

	var m map[string]any
	if err := json.Unmarshal([]byte(s.String), &m); err != nil {
		return nil
	}

	return m
}

// Metadata query methods

func (pq *PGQueue) getQueueMetadata(
	ctx context.Context,
	queueType, queueName string,
) (*QueueMetadata, error) {
	query := `
		SELECT id, queue_type, queue_name, table_name, config, created_at, updated_at
		FROM pgqueue_metadata
		WHERE queue_type = $1 AND queue_name = $2
		LIMIT 1
	`

	var meta QueueMetadata
	err := pq.db.QueryRowContext(ctx, query, queueType, queueName).Scan(
		&meta.ID,
		&meta.QueueType,
		&meta.QueueName,
		&meta.TableName,
		&meta.Config,
		&meta.CreatedAt,
		&meta.UpdatedAt,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to scan queue metadata: %w", err)
	}

	return &meta, nil
}

func (pq *PGQueue) createQueueMetadata(
	ctx context.Context,
	tx *sql.Tx,
	queueType, queueName, tableName string,
	config []byte,
) (*QueueMetadata, error) {
	query := `
		INSERT INTO pgqueue_metadata (queue_type, queue_name, table_name, config)
		VALUES ($1, $2, $3, $4)
		RETURNING id, queue_type, queue_name, table_name, config, created_at, updated_at
	`

	var meta QueueMetadata
	err := tx.QueryRowContext(ctx, query, queueType, queueName, tableName, config).Scan(
		&meta.ID,
		&meta.QueueType,
		&meta.QueueName,
		&meta.TableName,
		&meta.Config,
		&meta.CreatedAt,
		&meta.UpdatedAt,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to insert queue metadata: %w", err)
	}

	return &meta, nil
}

func (pq *PGQueue) listQueuesRaw(
	ctx context.Context,
	queueType string,
) ([]QueueMetadata, error) {
	query := `
		SELECT id, queue_type, queue_name, table_name, config, created_at, updated_at
		FROM pgqueue_metadata
		WHERE queue_type = $1
		ORDER BY created_at DESC
	`

	rows, err := pq.db.QueryContext(ctx, query, queueType)
	if err != nil {
		return nil, fmt.Errorf("failed to query queues: %w", err)
	}
	defer func() { _ = rows.Close() }()

	items := []QueueMetadata{}
	for rows.Next() {
		var meta QueueMetadata
		if err := rows.Scan(
			&meta.ID,
			&meta.QueueType,
			&meta.QueueName,
			&meta.TableName,
			&meta.Config,
			&meta.CreatedAt,
			&meta.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan queue row: %w", err)
		}
		items = append(items, meta)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate queue rows: %w", err)
	}

	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("failed to close rows: %w", err)
	}

	return items, nil
}

// Subscriber query methods

func (pq *PGQueue) registerSubscriber(
	ctx context.Context,
	topicName, subscriberID string,
) (*Subscriber, error) {
	query := `
		INSERT INTO pgqueue_subscribers (topic_name, subscriber_id)
		VALUES ($1, $2)
		ON CONFLICT (topic_name, subscriber_id)
		DO UPDATE SET active = TRUE, created_at = NOW()
		RETURNING id, topic_name, subscriber_id, created_at, active
	`

	var sub Subscriber
	err := pq.db.QueryRowContext(ctx, query, topicName, subscriberID).Scan(
		&sub.ID,
		&sub.TopicName,
		&sub.SubscriberID,
		&sub.CreatedAt,
		&sub.Active,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to scan subscriber: %w", err)
	}

	return &sub, nil
}

func (pq *PGQueue) unregisterSubscriber(
	ctx context.Context,
	topicName, subscriberID string,
) error {
	query := `
		UPDATE pgqueue_subscribers
		SET active = FALSE
		WHERE topic_name = $1 AND subscriber_id = $2
	`
	_, err := pq.db.ExecContext(ctx, query, topicName, subscriberID)
	if err != nil {
		return fmt.Errorf("failed to unregister subscriber: %w", err)
	}

	return nil
}

func (pq *PGQueue) getActiveSubscribers(
	ctx context.Context,
	tx *sql.Tx,
	topicName string,
) ([]Subscriber, error) {
	query := `
		SELECT id, topic_name, subscriber_id, created_at, active
		FROM pgqueue_subscribers
		WHERE topic_name = $1 AND active = TRUE
		ORDER BY created_at
	`

	var rows *sql.Rows
	var err error
	if tx != nil {
		rows, err = tx.QueryContext(ctx, query, topicName)
	} else {
		rows, err = pq.db.QueryContext(ctx, query, topicName)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to query subscribers: %w", err)
	}
	defer func() { _ = rows.Close() }()

	items := []Subscriber{}
	for rows.Next() {
		var sub Subscriber
		if err := rows.Scan(
			&sub.ID,
			&sub.TopicName,
			&sub.SubscriberID,
			&sub.CreatedAt,
			&sub.Active,
		); err != nil {
			return nil, fmt.Errorf("failed to scan subscriber row: %w", err)
		}
		items = append(items, sub)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate subscriber rows: %w", err)
	}

	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("failed to close rows: %w", err)
	}

	return items, nil
}

// Delete query methods

func (pq *PGQueue) deleteQueueMetadata(
	ctx context.Context,
	tx *sql.Tx,
	queueType, queueName string,
) error {
	// Delete replay log entries for this queue
	replayQuery := `DELETE FROM pgqueue_replay_log WHERE queue_type = $1 AND queue_name = $2`
	if _, err := tx.ExecContext(ctx, replayQuery, queueType, queueName); err != nil {
		return fmt.Errorf("failed to delete replay log entries: %w", err)
	}

	// For pub/sub, delete subscriber registrations
	if queueType == string(QueueTypePubSub) {
		subQuery := `DELETE FROM pgqueue_subscribers WHERE topic_name = $1`
		if _, err := tx.ExecContext(ctx, subQuery, queueName); err != nil {
			return fmt.Errorf("failed to delete subscriber registrations: %w", err)
		}
	}

	// Delete metadata entry
	metaQuery := `DELETE FROM pgqueue_metadata WHERE queue_type = $1 AND queue_name = $2`
	if _, err := tx.ExecContext(ctx, metaQuery, queueType, queueName); err != nil {
		return fmt.Errorf("failed to delete queue metadata: %w", err)
	}

	return nil
}

// Replay log query methods

func (pq *PGQueue) createReplayLog(
	ctx context.Context,
	queueType, queueName, replayType string,
	replayParams []byte,
	messageCount int,
	createdBy *string,
) error {
	query := `
		INSERT INTO pgqueue_replay_log (
			queue_type, queue_name, replay_type,
			replay_params, message_count, created_by
		)
		VALUES ($1, $2, $3, $4, $5, $6)
	`

	_, err := pq.db.ExecContext(
		ctx, query,
		queueType, queueName, replayType,
		replayParams, messageCount, createdBy,
	)
	if err != nil {
		return fmt.Errorf("failed to create replay log: %w", err)
	}

	return nil
}

func (pq *PGQueue) getReplayHistory(
	ctx context.Context,
	queueType, queueName string,
	limit int,
) ([]ReplayLog, error) {
	query := `
		SELECT id, queue_type, queue_name, replay_type,
		       replay_params, message_count, created_at, created_by
		FROM pgqueue_replay_log
		WHERE queue_type = $1 AND queue_name = $2
		ORDER BY created_at DESC
		LIMIT $3
	`

	rows, err := pq.db.QueryContext(ctx, query, queueType, queueName, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query replay history: %w", err)
	}
	defer func() { _ = rows.Close() }()

	items := []ReplayLog{}
	for rows.Next() {
		var rl ReplayLog
		if err := rows.Scan(
			&rl.ID,
			&rl.QueueType,
			&rl.QueueName,
			&rl.ReplayType,
			&rl.ReplayParams,
			&rl.MessageCount,
			&rl.CreatedAt,
			&rl.CreatedBy,
		); err != nil {
			return nil, fmt.Errorf("failed to scan replay log row: %w", err)
		}
		items = append(items, rl)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate replay log rows: %w", err)
	}

	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("failed to close rows: %w", err)
	}

	return items, nil
}
