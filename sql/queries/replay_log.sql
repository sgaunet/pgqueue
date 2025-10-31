-- name: CreateReplayLog :one
INSERT INTO pgqueue_replay_log (queue_type, queue_name, replay_type, replay_params, message_count, created_by)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING *;

-- name: GetReplayHistory :many
SELECT * FROM pgqueue_replay_log
WHERE queue_type = $1 AND queue_name = $2
ORDER BY created_at DESC
LIMIT $3;

-- name: GetReplayHistoryAll :many
SELECT * FROM pgqueue_replay_log
ORDER BY created_at DESC
LIMIT $1;
