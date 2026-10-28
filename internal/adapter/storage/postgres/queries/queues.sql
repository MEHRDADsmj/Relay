-- name: UpsertQueue :exec
INSERT INTO queues (name, paused, created_at, updated_at)
VALUES ($1, false, $2, $2)
ON CONFLICT (name) DO NOTHING;

-- name: GetQueue :one
SELECT * FROM queues
WHERE name = $1;

-- name: ListQueues :many
SELECT * FROM queues
ORDER BY name ASC;

-- name: SetQueuePaused :one
UPDATE queues
SET
    paused = sqlc.arg(paused),
    updated_at = sqlc.arg(updated_at)
WHERE name = sqlc.arg(name)
RETURNING *;

-- name: QueueStatusCounts :many
SELECT status, count(*)::bigint AS count
FROM jobs
WHERE queue = sqlc.arg(queue)
GROUP BY status;

-- name: QueueScheduledCount :one
SELECT count(*)::bigint AS count
FROM jobs
WHERE queue = sqlc.arg(queue)
  AND status = 'pending'
  AND run_at > sqlc.arg(now)::timestamptz;

-- name: QueueOldestDuePending :one
SELECT run_at FROM jobs
WHERE queue = sqlc.arg(queue)
  AND status = 'pending'
  AND run_at <= sqlc.arg(now)::timestamptz
ORDER BY run_at ASC
LIMIT 1;
