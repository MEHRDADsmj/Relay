-- name: IncrQueueStats :exec
INSERT INTO queue_stats_minutely (queue, minute, enqueued, succeeded, failed, dead)
VALUES (
    sqlc.arg(queue),
    date_trunc('minute', sqlc.arg(now)::timestamptz),
    sqlc.arg(enqueued),
    sqlc.arg(succeeded),
    sqlc.arg(failed),
    sqlc.arg(dead)
)
ON CONFLICT (queue, minute) DO UPDATE SET
    enqueued = queue_stats_minutely.enqueued + EXCLUDED.enqueued,
    succeeded = queue_stats_minutely.succeeded + EXCLUDED.succeeded,
    failed = queue_stats_minutely.failed + EXCLUDED.failed,
    dead = queue_stats_minutely.dead + EXCLUDED.dead;

-- name: ListQueueStats :many
SELECT queue, minute, enqueued, succeeded, failed, dead
FROM queue_stats_minutely
WHERE queue = sqlc.arg(queue)
  AND minute >= sqlc.arg(from_minute)::timestamptz
  AND minute <= sqlc.arg(to_minute)::timestamptz
ORDER BY minute ASC;

-- name: PurgeQueueStats :execrows
DELETE FROM queue_stats_minutely
WHERE minute < sqlc.arg(before)::timestamptz;
