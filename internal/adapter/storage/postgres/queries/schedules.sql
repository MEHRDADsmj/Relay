-- name: CreateSchedule :one
INSERT INTO schedules (
    id, name, cron, timezone, queue, job_type, payload, priority, max_attempts,
    enabled, next_run_at, last_run_at, last_job_id, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
)
RETURNING *;

-- name: GetSchedule :one
SELECT * FROM schedules
WHERE id = $1;

-- name: GetScheduleByName :one
SELECT * FROM schedules
WHERE name = $1;

-- name: ListSchedules :many
SELECT * FROM schedules
WHERE
    (sqlc.narg(cursor_name)::text IS NULL)
    OR (name > sqlc.narg(cursor_name))
    OR (name = sqlc.narg(cursor_name) AND id > sqlc.narg(cursor_id)::uuid)
ORDER BY name ASC, id ASC
LIMIT sqlc.arg(page_limit);

-- name: UpdateSchedule :one
UPDATE schedules
SET
    name = sqlc.arg(name),
    cron = sqlc.arg(cron),
    timezone = sqlc.arg(timezone),
    queue = sqlc.arg(queue),
    job_type = sqlc.arg(job_type),
    payload = sqlc.arg(payload),
    priority = sqlc.arg(priority),
    max_attempts = sqlc.arg(max_attempts),
    enabled = sqlc.arg(enabled),
    next_run_at = sqlc.arg(next_run_at),
    last_run_at = sqlc.arg(last_run_at),
    last_job_id = sqlc.arg(last_job_id),
    updated_at = sqlc.arg(updated_at)
WHERE id = sqlc.arg(id)
  AND updated_at = sqlc.arg(expected_updated_at)
RETURNING *;

-- name: DeleteSchedule :execrows
DELETE FROM schedules
WHERE id = $1;

-- name: DueSchedules :many
SELECT * FROM schedules
WHERE enabled = true
  AND next_run_at <= sqlc.arg(now)::timestamptz
ORDER BY next_run_at ASC, id ASC
LIMIT sqlc.arg(batch_limit);

-- name: MarkFired :one
UPDATE schedules
SET
    last_run_at = sqlc.arg(fired_at),
    last_job_id = sqlc.arg(job_id),
    next_run_at = sqlc.arg(next_run_at),
    updated_at = sqlc.arg(fired_at)
WHERE id = sqlc.arg(id)
  AND next_run_at = sqlc.arg(expected_next)
RETURNING *;
