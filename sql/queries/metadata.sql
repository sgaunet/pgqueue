-- name: CreateQueueMetadata :one
INSERT INTO pgqueue_metadata (queue_type, queue_name, table_name, config)
VALUES ($1, $2, $3, $4)
RETURNING *;

-- name: GetQueueMetadata :one
SELECT * FROM pgqueue_metadata
WHERE queue_type = $1 AND queue_name = $2
LIMIT 1;

-- name: ListQueues :many
SELECT * FROM pgqueue_metadata
WHERE queue_type = $1
ORDER BY created_at DESC;

-- name: ListAllQueues :many
SELECT * FROM pgqueue_metadata
ORDER BY queue_type, queue_name;

-- name: DeleteQueueMetadata :exec
DELETE FROM pgqueue_metadata
WHERE queue_type = $1 AND queue_name = $2;

-- name: UpdateQueueConfig :one
UPDATE pgqueue_metadata
SET config = $3, updated_at = NOW()
WHERE queue_type = $1 AND queue_name = $2
RETURNING *;
