-- name: InsertAttempt :exec
INSERT INTO job_attempts (
    job_id,
    number,
    worker_id,
    started_at,
    finished_at,
    outcome,
    error
) VALUES (
    $1, $2, $3, $4, $5, $6, $7
);

-- name: MaxAttemptStartedAt :one
SELECT started_at FROM job_attempts
WHERE job_id = $1
ORDER BY started_at DESC
LIMIT 1;

-- name: CloseRunningAttempt :exec
UPDATE job_attempts
SET
    outcome = sqlc.arg(outcome),
    finished_at = sqlc.arg(finished_at),
    error = sqlc.arg(error)
WHERE job_id = sqlc.arg(job_id)
  AND outcome = 'running';

-- name: ListAttempts :many
SELECT * FROM job_attempts
WHERE job_id = $1
ORDER BY started_at ASC, number ASC;
