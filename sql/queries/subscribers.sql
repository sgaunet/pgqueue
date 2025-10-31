-- name: RegisterSubscriber :one
INSERT INTO pgqueue_subscribers (topic_name, subscriber_id)
VALUES ($1, $2)
ON CONFLICT (topic_name, subscriber_id)
DO UPDATE SET active = TRUE, created_at = NOW()
RETURNING *;

-- name: UnregisterSubscriber :exec
UPDATE pgqueue_subscribers
SET active = FALSE
WHERE topic_name = $1 AND subscriber_id = $2;

-- name: GetActiveSubscribers :many
SELECT * FROM pgqueue_subscribers
WHERE topic_name = $1 AND active = TRUE
ORDER BY created_at;

-- name: GetSubscriber :one
SELECT * FROM pgqueue_subscribers
WHERE topic_name = $1 AND subscriber_id = $2
LIMIT 1;

-- name: DeleteSubscriber :exec
DELETE FROM pgqueue_subscribers
WHERE topic_name = $1 AND subscriber_id = $2;
