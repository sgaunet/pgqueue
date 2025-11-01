package pgqueue

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// GetStats returns statistics for a queue
func (pq *PGQueue) GetStats(ctx context.Context, queueName string, queueType QueueType) (*QueueStats, error) {
	// Get queue metadata
	metadata, err := pq.getQueueMetadata(ctx, string(queueType), queueName)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("queue not found: %s/%s", queueType, queueName)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get queue metadata: %w", err)
	}

	tableName := metadata.TableName
	stats := &QueueStats{
		QueueName: queueName,
	}

	// Get message counts by status
	if queueType == QueueTypeChannel {
		if err := pq.getChannelStats(ctx, tableName, stats); err != nil {
			return nil, err
		}
	} else {
		if err := pq.getPubSubStats(ctx, tableName, stats); err != nil {
			return nil, err
		}
	}

	// Get DLQ count
	dlqQuery := fmt.Sprintf("SELECT COUNT(*) FROM pgqueue_dlq_%s", tableName)
	if err := pq.db.QueryRowContext(ctx, dlqQuery).Scan(&stats.DLQCount); err != nil {
		return nil, fmt.Errorf("failed to get DLQ count: %w", err)
	}

	return stats, nil
}

// getChannelStats gets statistics for a channel queue
func (pq *PGQueue) getChannelStats(ctx context.Context, tableName string, stats *QueueStats) error {
	query := fmt.Sprintf(`
		SELECT
			COUNT(*) FILTER (WHERE status = 'pending') AS pending,
			COUNT(*) FILTER (WHERE status = 'processing') AS processing,
			COUNT(*) FILTER (WHERE status = 'completed') AS completed,
			COUNT(*) FILTER (WHERE status = 'failed') AS failed,
			AVG(EXTRACT(EPOCH FROM (processed_at - created_at))) FILTER (WHERE processed_at IS NOT NULL) AS avg_processing_time,
			MIN(created_at) FILTER (WHERE status = 'pending') AS oldest_pending
		FROM pgqueue_msg_%s
	`, tableName)

	var avgSeconds sql.NullFloat64
	var oldestPending sql.NullTime

	err := pq.db.QueryRowContext(ctx, query).Scan(
		&stats.PendingCount,
		&stats.ProcessingCount,
		&stats.CompletedCount,
		&stats.FailedCount,
		&avgSeconds,
		&oldestPending,
	)
	if err != nil {
		return fmt.Errorf("failed to get channel stats: %w", err)
	}

	if avgSeconds.Valid {
		duration := time.Duration(avgSeconds.Float64 * float64(time.Second))
		stats.AvgProcessingTime = &duration
	}

	if oldestPending.Valid {
		age := time.Since(oldestPending.Time)
		stats.OldestPendingAge = &age
	}

	return nil
}

// getPubSubStats gets statistics for a pub/sub topic
func (pq *PGQueue) getPubSubStats(ctx context.Context, tableName string, stats *QueueStats) error {
	query := fmt.Sprintf(`
		SELECT
			COUNT(*) FILTER (WHERE status = 'pending') AS pending,
			COUNT(*) FILTER (WHERE status = 'processing') AS processing,
			COUNT(*) FILTER (WHERE status = 'acked') AS completed,
			COUNT(*) FILTER (WHERE status = 'nacked') AS failed,
			AVG(EXTRACT(EPOCH FROM (acked_at - created_at))) FILTER (WHERE acked_at IS NOT NULL) AS avg_processing_time,
			MIN(created_at) FILTER (WHERE status = 'pending') AS oldest_pending
		FROM pgqueue_sub_%s
	`, tableName)

	var avgSeconds sql.NullFloat64
	var oldestPending sql.NullTime

	err := pq.db.QueryRowContext(ctx, query).Scan(
		&stats.PendingCount,
		&stats.ProcessingCount,
		&stats.CompletedCount,
		&stats.FailedCount,
		&avgSeconds,
		&oldestPending,
	)
	if err != nil {
		return fmt.Errorf("failed to get pub/sub stats: %w", err)
	}

	if avgSeconds.Valid {
		duration := time.Duration(avgSeconds.Float64 * float64(time.Second))
		stats.AvgProcessingTime = &duration
	}

	if oldestPending.Valid {
		age := time.Since(oldestPending.Time)
		stats.OldestPendingAge = &age
	}

	return nil
}

// GetQueueDepth returns the number of pending messages in a queue
func (pq *PGQueue) GetQueueDepth(ctx context.Context, queueName string, queueType QueueType) (int64, error) {
	// Get queue metadata
	metadata, err := pq.getQueueMetadata(ctx, string(queueType), queueName)
	if err == sql.ErrNoRows {
		return 0, fmt.Errorf("queue not found: %s/%s", queueType, queueName)
	}
	if err != nil {
		return 0, fmt.Errorf("failed to get queue metadata: %w", err)
	}

	tableName := metadata.TableName
	var count int64

	if queueType == QueueTypeChannel {
		query := fmt.Sprintf("SELECT COUNT(*) FROM pgqueue_msg_%s WHERE status = 'pending'", tableName)
		if err := pq.db.QueryRowContext(ctx, query).Scan(&count); err != nil {
			return 0, fmt.Errorf("failed to get queue depth: %w", err)
		}
	} else {
		query := fmt.Sprintf("SELECT COUNT(*) FROM pgqueue_sub_%s WHERE status = 'pending'", tableName)
		if err := pq.db.QueryRowContext(ctx, query).Scan(&count); err != nil {
			return 0, fmt.Errorf("failed to get queue depth: %w", err)
		}
	}

	return count, nil
}

// GetSubscriberLag returns lag statistics for a specific subscriber on a topic
func (pq *PGQueue) GetSubscriberLag(ctx context.Context, topicName string, subscriberID string) (*SubscriberLag, error) {
	// Get topic metadata
	metadata, err := pq.getQueueMetadata(ctx, string(QueueTypePubSub), topicName)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("topic not found: %s", topicName)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get topic metadata: %w", err)
	}

	tableName := metadata.TableName

	query := fmt.Sprintf(`
		SELECT
			COUNT(*) FILTER (WHERE status = 'pending') AS pending_count,
			COUNT(*) FILTER (WHERE status = 'processing') AS processing_count,
			COUNT(*) FILTER (WHERE status = 'acked') AS acked_count,
			MIN(created_at) FILTER (WHERE status = 'pending') AS oldest_pending
		FROM pgqueue_sub_%s
		WHERE subscriber_id = $1
	`, tableName)

	lag := &SubscriberLag{
		SubscriberID: subscriberID,
		TopicName:    topicName,
	}

	var oldestPending sql.NullTime

	err = pq.db.QueryRowContext(ctx, query, subscriberID).Scan(
		&lag.PendingCount,
		&lag.ProcessingCount,
		&lag.AckedCount,
		&oldestPending,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get subscriber lag: %w", err)
	}

	if oldestPending.Valid {
		age := time.Since(oldestPending.Time)
		lag.OldestPendingAge = &age
	}

	return lag, nil
}

// GetDLQStats returns statistics about messages in the dead letter queue
func (pq *PGQueue) GetDLQStats(ctx context.Context, queueName string, queueType QueueType) (*DLQStats, error) {
	// Get queue metadata
	metadata, err := pq.getQueueMetadata(ctx, string(queueType), queueName)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("queue not found: %s/%s", queueType, queueName)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get queue metadata: %w", err)
	}

	tableName := metadata.TableName

	query := fmt.Sprintf(`
		SELECT
			COUNT(*) AS total_count,
			MIN(moved_at) AS oldest_moved_at,
			MAX(moved_at) AS newest_moved_at,
			AVG(retry_count) AS avg_retry_count
		FROM pgqueue_dlq_%s
	`, tableName)

	stats := &DLQStats{
		QueueName: queueName,
	}

	var oldestMovedAt, newestMovedAt sql.NullTime
	var avgRetryCount sql.NullFloat64

	err = pq.db.QueryRowContext(ctx, query).Scan(
		&stats.TotalCount,
		&oldestMovedAt,
		&newestMovedAt,
		&avgRetryCount,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get DLQ stats: %w", err)
	}

	if oldestMovedAt.Valid {
		stats.OldestMovedAt = &oldestMovedAt.Time
	}

	if newestMovedAt.Valid {
		stats.NewestMovedAt = &newestMovedAt.Time
	}

	if avgRetryCount.Valid {
		stats.AvgRetryCount = avgRetryCount.Float64
	}

	return stats, nil
}

// SubscriberLag holds lag information for a subscriber
type SubscriberLag struct {
	SubscriberID     string
	TopicName        string
	PendingCount     int64
	ProcessingCount  int64
	AckedCount       int64
	OldestPendingAge *time.Duration
}

// DLQStats holds statistics about the dead letter queue
type DLQStats struct {
	QueueName      string
	TotalCount     int64
	OldestMovedAt  *time.Time
	NewestMovedAt  *time.Time
	AvgRetryCount  float64
}
