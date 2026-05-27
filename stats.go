package pgqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// GetStats returns statistics for a queue.
func (pq *Queue) GetStats(
	ctx context.Context,
	queueName string,
	queueType QueueType,
) (*QueueStats, error) {
	if err := pq.checkClosed(); err != nil {
		return nil, err
	}
	// Get queue metadata
	metadata, err := pq.getQueueMetadata(ctx, string(queueType), queueName)
	if errors.Is(err, ErrQueueNotFound) {
		return nil, fmt.Errorf(
			"%s/%s: %w", queueType, queueName, ErrQueueNotFound,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get queue metadata: %w", err)
	}

	tableName := metadata.TableName
	stats := &QueueStats{
		QueueName: queueName,
	}

	// Message counts and the DLQ count are gathered in a single statement so
	// they share one snapshot under READ COMMITTED — otherwise messages can
	// transition (pending → processing → completed, or nack → DLQ) between two
	// separate queries and the returned counts would not sum to a real point
	// in time (issue #112).
	if queueType == QueueTypeChannel {
		if err := pq.getChannelStats(ctx, tableName, stats); err != nil {
			return nil, fmt.Errorf("failed to get channel stats: %w", err)
		}
	} else {
		if err := pq.getPubSubStats(ctx, tableName, stats); err != nil {
			return nil, fmt.Errorf("failed to get pub/sub stats: %w", err)
		}
	}

	// Feed the observed depth to a registered MetricsRecorder (FR-018); a no-op
	// when none is registered.
	pq.observeQueueDepth(queueName, stats.PendingCount)
	pq.observeDLQSize(queueName, stats.DLQCount)

	return stats, nil
}

// GetQueueDepth returns the number of pending messages currently consumable
// from a queue. Messages whose TTL has elapsed are excluded, matching what the
// consume queries actually deliver, so the depth is not inflated by expired
// rows that no consumer can ever receive.
func (pq *Queue) GetQueueDepth(
	ctx context.Context,
	queueName string,
	queueType QueueType,
) (int64, error) {
	if err := pq.checkClosed(); err != nil {
		return 0, err
	}
	// Get queue metadata
	metadata, err := pq.getQueueMetadata(ctx, string(queueType), queueName)
	if errors.Is(err, ErrQueueNotFound) {
		return 0, fmt.Errorf(
			"%s/%s: %w", queueType, queueName, ErrQueueNotFound,
		)
	}
	if err != nil {
		return 0, fmt.Errorf("failed to get queue metadata: %w", err)
	}

	tableName := metadata.TableName
	ttl := pq.getQueueTTL(metadata.Config)
	query, args := queueDepthQuery(pq, tableName, queueType, ttl)

	var count int64
	if err := pq.db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to get queue depth: %w", err)
	}

	return count, nil
}

// queueDepthQuery builds the consumable-depth COUNT for a queue. When a TTL is
// configured the count excludes messages past their TTL — for channels via the
// message table's created_at, for topics via the joined message row — so it
// agrees with what the consume queries deliver.
func queueDepthQuery(pq *Queue, tableName string, queueType QueueType, ttl time.Duration) (string, []any) {
	if queueType == QueueTypeChannel {
		base := fmt.Sprintf(
			"SELECT COUNT(*) FROM %s WHERE status = '%s'",
			pq.msgTable(tableName), MessageStatusPending,
		)
		if ttl > 0 {
			return base + " AND created_at > NOW() - make_interval(secs => $1)",
				[]any{ttl.Seconds()}
		}
		return base, nil
	}

	if ttl > 0 {
		return fmt.Sprintf(
			`SELECT COUNT(*) FROM %s s JOIN %s m ON s.message_id = m.id
			 WHERE s.status = '%s' AND m.created_at > NOW() - make_interval(secs => $1)`,
			pq.subTable(tableName), pq.msgTable(tableName), MessageStatusPending,
		), []any{ttl.Seconds()}
	}
	return fmt.Sprintf(
		"SELECT COUNT(*) FROM %s WHERE status = '%s'",
		pq.subTable(tableName), MessageStatusPending,
	), nil
}

// GetSubscriberLag returns lag statistics for a specific subscriber on a topic.
func (pq *Queue) GetSubscriberLag(
	ctx context.Context,
	topicName string,
	subscriberID string,
) (*SubscriberLag, error) {
	if err := pq.checkClosed(); err != nil {
		return nil, err
	}
	if err := validateSubscriberID(subscriberID); err != nil {
		return nil, err
	}

	// Get topic metadata
	metadata, err := pq.getQueueMetadata(
		ctx, string(QueueTypePubSub), topicName,
	)
	if errors.Is(err, ErrQueueNotFound) {
		return nil, fmt.Errorf("%s: %w", topicName, ErrTopicNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get topic metadata: %w", err)
	}

	tableName := metadata.TableName

	// Age computed in SQL for clock consistency — see getChannelStats (R-19).
	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(`
		SELECT
			COUNT(*) FILTER (WHERE status = '%s') AS pending_count,
			COUNT(*) FILTER (WHERE status = '%s') AS processing_count,
			COUNT(*) FILTER (WHERE status = '%s') AS acked_count,
			EXTRACT(EPOCH FROM (NOW() - MIN(created_at)
				FILTER (WHERE status = '%s'))) AS oldest_pending_age
		FROM %s
		WHERE subscriber_id = $1
	`, MessageStatusPending, MessageStatusProcessing, MessageStatusAcked, MessageStatusPending, pq.subTable(tableName))

	lag := &SubscriberLag{
		SubscriberID: subscriberID,
		TopicName:    topicName,
	}

	var oldestPendingAge sql.NullFloat64

	err = pq.db.QueryRowContext(ctx, query, subscriberID).Scan(
		&lag.PendingCount,
		&lag.ProcessingCount,
		&lag.AckedCount,
		&oldestPendingAge,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get subscriber lag: %w", err)
	}

	lag.OldestPendingAge = secondsToAge(oldestPendingAge)

	return lag, nil
}

// GetDLQStats returns statistics about messages in the dead letter queue.
func (pq *Queue) GetDLQStats(
	ctx context.Context,
	queueName string,
	queueType QueueType,
) (*DLQStats, error) {
	if err := pq.checkClosed(); err != nil {
		return nil, err
	}
	// Get queue metadata
	metadata, err := pq.getQueueMetadata(ctx, string(queueType), queueName)
	if errors.Is(err, ErrQueueNotFound) {
		return nil, fmt.Errorf(
			"%s/%s: %w", queueType, queueName, ErrQueueNotFound,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get queue metadata: %w", err)
	}

	tableName := metadata.TableName

	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(`
		SELECT
			COUNT(*) AS total_count,
			MIN(moved_at) AS oldest_moved_at,
			MAX(moved_at) AS newest_moved_at,
			AVG(retry_count) AS avg_retry_count
		FROM %s
	`, pq.dlqTable(tableName))

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

// getChannelStats gets statistics for a channel queue.
func (pq *Queue) getChannelStats(
	ctx context.Context,
	tableName string,
	stats *QueueStats,
) error {
	// The oldest-pending age is computed in SQL (NOW() - created_at) rather
	// than with time.Since on the application clock, so it is consistent with
	// the database clock and never negative under NTP skew (R-19).
	// The DLQ count rides on the same statement so all four counters share a
	// single snapshot (issue #112).
	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(`
		SELECT
			COUNT(*) FILTER (WHERE status = '%s') AS pending,
			COUNT(*) FILTER (WHERE status = '%s') AS processing,
			COUNT(*) FILTER (WHERE status = '%s') AS completed,
			AVG(EXTRACT(EPOCH FROM (processed_at - created_at)))
				FILTER (WHERE processed_at IS NOT NULL) AS avg_processing_time,
			EXTRACT(EPOCH FROM (NOW() - MIN(created_at)
				FILTER (WHERE status = '%s'))) AS oldest_pending_age,
			(SELECT COUNT(*) FROM %s) AS dlq_count
		FROM %s
	`, MessageStatusPending, MessageStatusProcessing, MessageStatusCompleted, MessageStatusPending,
		pq.dlqTable(tableName), pq.msgTable(tableName))

	var avgSeconds, oldestPendingAge sql.NullFloat64

	err := pq.db.QueryRowContext(ctx, query).Scan(
		&stats.PendingCount,
		&stats.ProcessingCount,
		&stats.CompletedCount,
		&avgSeconds,
		&oldestPendingAge,
		&stats.DLQCount,
	)
	if err != nil {
		return fmt.Errorf("failed to get channel stats: %w", err)
	}

	if avgSeconds.Valid {
		duration := time.Duration(avgSeconds.Float64 * float64(time.Second))
		stats.AvgProcessingTime = &duration
	}

	stats.OldestPendingAge = secondsToAge(oldestPendingAge)

	return nil
}

// secondsToAge converts an age in seconds from a SQL EXTRACT(EPOCH …) result
// into a *time.Duration, clamping any negative value to zero so a reported age
// is never negative (R-19).
func secondsToAge(secs sql.NullFloat64) *time.Duration {
	if !secs.Valid {
		return nil
	}
	v := secs.Float64
	if v < 0 {
		v = 0
	}
	age := time.Duration(v * float64(time.Second))
	return &age
}

// getPubSubStats gets statistics for a pub/sub topic.
func (pq *Queue) getPubSubStats(
	ctx context.Context,
	tableName string,
	stats *QueueStats,
) error {
	// Age computed in SQL for clock consistency — see getChannelStats (R-19).
	// The DLQ count rides on the same statement so all counters share a
	// single snapshot (issue #112).
	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(`
		SELECT
			COUNT(*) FILTER (WHERE status = '%s') AS pending,
			COUNT(*) FILTER (WHERE status = '%s') AS processing,
			COUNT(*) FILTER (WHERE status = '%s') AS completed,
			AVG(EXTRACT(EPOCH FROM (acked_at - created_at)))
				FILTER (WHERE acked_at IS NOT NULL) AS avg_processing_time,
			EXTRACT(EPOCH FROM (NOW() - MIN(created_at)
				FILTER (WHERE status = '%s'))) AS oldest_pending_age,
			(SELECT COUNT(*) FROM %s) AS dlq_count
		FROM %s
	`, MessageStatusPending, MessageStatusProcessing, MessageStatusAcked, MessageStatusPending,
		pq.dlqTable(tableName), pq.subTable(tableName))

	var avgSeconds, oldestPendingAge sql.NullFloat64

	err := pq.db.QueryRowContext(ctx, query).Scan(
		&stats.PendingCount,
		&stats.ProcessingCount,
		&stats.CompletedCount,
		&avgSeconds,
		&oldestPendingAge,
		&stats.DLQCount,
	)
	if err != nil {
		return fmt.Errorf("failed to get pub/sub stats: %w", err)
	}

	if avgSeconds.Valid {
		duration := time.Duration(avgSeconds.Float64 * float64(time.Second))
		stats.AvgProcessingTime = &duration
	}

	stats.OldestPendingAge = secondsToAge(oldestPendingAge)

	return nil
}

// GetSubscriberHealth returns detailed health information for a specific subscriber on a topic.
func (pq *Queue) GetSubscriberHealth(
	ctx context.Context,
	topicName string,
	subscriberID string,
) (*SubscriberHealth, error) {
	if err := pq.checkClosed(); err != nil {
		return nil, err
	}
	if err := validateSubscriberID(subscriberID); err != nil {
		return nil, err
	}

	metadata, err := pq.getQueueMetadata(
		ctx, string(QueueTypePubSub), topicName,
	)
	if errors.Is(err, ErrQueueNotFound) {
		return nil, fmt.Errorf("%s: %w", topicName, ErrTopicNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get topic metadata: %w", err)
	}

	query := buildSubscriberHealthQuery(pq.subTable(metadata.TableName))

	health := &SubscriberHealth{
		TopicName:    topicName,
		SubscriberID: subscriberID,
	}

	var oldestPending, lastActivity sql.NullTime

	err = pq.db.QueryRowContext(ctx, query, subscriberID).Scan(
		&health.PendingMessages,
		&health.StuckMessages,
		&oldestPending,
		&lastActivity,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get subscriber health: %w", err)
	}

	if oldestPending.Valid {
		health.OldestPending = &oldestPending.Time
	}

	if lastActivity.Valid {
		health.LastActivity = &lastActivity.Time
	}

	return health, nil
}

// GetUnhealthySubscribers returns subscribers with health issues across all topics.
// A subscriber is unhealthy if it has messages stuck in processing (visibility timeout
// expired) or pending messages older than the given threshold.
// Note: this executes one query per topic due to the table-per-queue design.
func (pq *Queue) GetUnhealthySubscribers(
	ctx context.Context,
	threshold time.Duration,
) ([]SubscriberHealth, error) {
	if err := pq.checkClosed(); err != nil {
		return nil, err
	}
	// Get all pub/sub topics
	rows, err := pq.db.QueryContext(ctx,
		fmt.Sprintf(
			"SELECT queue_name, table_name FROM %s WHERE queue_type = $1",
			pq.globalTable("pgqueue_metadata"),
		),
		string(QueueTypePubSub),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to list topics: %w", err)
	}
	defer func() { _ = rows.Close() }()

	type topicInfo struct {
		queueName string
		tableName string
	}

	var topics []topicInfo
	for rows.Next() {
		var t topicInfo
		if err := rows.Scan(&t.queueName, &t.tableName); err != nil {
			return nil, fmt.Errorf("failed to scan topic: %w", err)
		}
		topics = append(topics, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate topics: %w", err)
	}

	var unhealthy []SubscriberHealth

	for _, topic := range topics {
		subs, err := pq.findUnhealthyForTopic(ctx, topic.queueName, topic.tableName, threshold)
		if err != nil {
			return nil, fmt.Errorf("failed to check topic %s: %w", topic.queueName, err)
		}
		unhealthy = append(unhealthy, subs...)
	}

	return unhealthy, nil
}

func (pq *Queue) findUnhealthyForTopic(
	ctx context.Context,
	topicName, tableName string,
	threshold time.Duration,
) ([]SubscriberHealth, error) {
	query := buildUnhealthySubscribersQuery(pq.subTable(tableName))

	rows, err := pq.db.QueryContext(ctx, query, threshold.Seconds())
	if err != nil {
		return nil, fmt.Errorf("failed to query unhealthy subscribers: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var results []SubscriberHealth
	for rows.Next() {
		h := SubscriberHealth{TopicName: topicName}
		var oldestPending, lastActivity sql.NullTime

		if err := rows.Scan(
			&h.SubscriberID, &h.PendingMessages, &h.StuckMessages,
			&oldestPending, &lastActivity,
		); err != nil {
			return nil, fmt.Errorf("failed to scan subscriber health: %w", err)
		}

		if oldestPending.Valid {
			h.OldestPending = &oldestPending.Time
		}
		if lastActivity.Valid {
			h.LastActivity = &lastActivity.Time
		}

		results = append(results, h)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate subscriber health rows: %w", err)
	}

	return results, nil
}

// buildSubscriberHealthQuery builds the per-subscriber health aggregate query.
func buildSubscriberHealthQuery(subTable string) string {
	return fmt.Sprintf(`
		SELECT
			COUNT(*) FILTER (WHERE status = '%s') AS pending_messages,
			COUNT(*) FILTER (
				WHERE status = '%s'
				AND visibility_timeout IS NOT NULL
				AND visibility_timeout < NOW()
			) AS stuck_messages,
			MIN(created_at) FILTER (WHERE status = '%s') AS oldest_pending,
			MAX(acked_at) AS last_activity
		FROM %s
		WHERE subscriber_id = $1
	`, MessageStatusPending, MessageStatusProcessing, MessageStatusPending, subTable)
}

func buildUnhealthySubscribersQuery(subTable string) string {
	return fmt.Sprintf(`
		SELECT
			subscriber_id,
			COUNT(*) FILTER (WHERE status = '%s') AS pending_messages,
			COUNT(*) FILTER (
				WHERE status = '%s'
				AND visibility_timeout IS NOT NULL
				AND visibility_timeout < NOW()
			) AS stuck_messages,
			MIN(created_at) FILTER (WHERE status = '%s') AS oldest_pending,
			MAX(acked_at) AS last_activity
		FROM %s
		GROUP BY subscriber_id
		HAVING
			COUNT(*) FILTER (
				WHERE status = '%s'
				AND visibility_timeout IS NOT NULL
				AND visibility_timeout < NOW()
			) > 0
			OR MIN(created_at) FILTER (WHERE status = '%s')
				< NOW() - make_interval(secs => $1)
	`, MessageStatusPending, MessageStatusProcessing, MessageStatusPending, subTable,
		MessageStatusProcessing, MessageStatusPending)
}
