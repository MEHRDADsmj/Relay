-- name: CreateJob :one
INSERT INTO jobs (
    id,
    queue,
    type,
    payload,
    status,
    priority,
    run_at,
    attempts,
    max_attempts,
    lease_owner,
    lease_expires_at,
    idempotency_key,
    last_error,
    created_at,
    updated_at,
    finished_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
)
ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL
DO NOTHING
RETURNING *;

-- name: GetJob :one
SELECT * FROM jobs
WHERE id = $1;

-- name: GetJobByIdempotencyKey :one
SELECT * FROM jobs
WHERE queue = $1 AND idempotency_key = $2;

-- name: MarkExpiredLeasesDead :exec
UPDATE jobs
SET
    status = 'dead',
    last_error = 'lease expired',
    finished_at = sqlc.arg(now)::timestamptz,
    lease_owner = NULL,
    lease_expires_at = NULL,
    updated_at = sqlc.arg(now)::timestamptz
WHERE queue = sqlc.arg(queue)
  AND status = 'running'
  AND lease_expires_at <= sqlc.arg(now)::timestamptz
  AND attempts >= max_attempts;

-- name: ClaimJobs :many
UPDATE jobs AS j
SET
    status = 'running',
    lease_owner = sqlc.arg(owner),
    lease_expires_at = sqlc.arg(lease_expires_at),
    attempts = attempts + 1,
    updated_at = sqlc.arg(now)::timestamptz
WHERE j.id IN (
    SELECT c.id FROM jobs AS c
    WHERE c.queue = sqlc.arg(queue)
      AND (
          (c.status = 'pending' AND c.run_at <= sqlc.arg(now)::timestamptz)
          OR (
              c.status = 'running'
              AND c.lease_expires_at <= sqlc.arg(now)::timestamptz
              AND c.attempts < c.max_attempts
          )
      )
    ORDER BY c.priority DESC, c.run_at ASC, c.id ASC
    FOR UPDATE SKIP LOCKED
    LIMIT sqlc.arg(batch_limit)
)
RETURNING *;

-- name: ExtendLease :one
UPDATE jobs
SET
    lease_expires_at = sqlc.arg(lease_expires_at),
    updated_at = sqlc.arg(now)::timestamptz
WHERE id = sqlc.arg(id)
  AND status = 'running'
  AND lease_owner = sqlc.arg(owner)
  AND lease_expires_at > sqlc.arg(now)::timestamptz
RETURNING *;

-- name: CompleteJob :one
UPDATE jobs
SET
    status = 'succeeded',
    finished_at = sqlc.arg(now)::timestamptz,
    lease_owner = NULL,
    lease_expires_at = NULL,
    updated_at = sqlc.arg(now)::timestamptz
WHERE id = sqlc.arg(id)
  AND status = 'running'
  AND lease_owner = sqlc.arg(owner)
  AND lease_expires_at > sqlc.arg(now)::timestamptz
RETURNING *;

-- name: FailJobPending :one
UPDATE jobs
SET
    status = 'pending',
    run_at = sqlc.arg(run_at),
    last_error = sqlc.arg(last_error),
    lease_owner = NULL,
    lease_expires_at = NULL,
    updated_at = sqlc.arg(now)::timestamptz
WHERE id = sqlc.arg(id)
  AND status = 'running'
  AND lease_owner = sqlc.arg(owner)
  AND lease_expires_at > sqlc.arg(now)::timestamptz
RETURNING *;

-- name: FailJobDead :one
UPDATE jobs
SET
    status = 'dead',
    finished_at = sqlc.arg(now)::timestamptz,
    last_error = sqlc.arg(last_error),
    lease_owner = NULL,
    lease_expires_at = NULL,
    updated_at = sqlc.arg(now)::timestamptz
WHERE id = sqlc.arg(id)
  AND status = 'running'
  AND lease_owner = sqlc.arg(owner)
  AND lease_expires_at > sqlc.arg(now)::timestamptz
RETURNING *;
