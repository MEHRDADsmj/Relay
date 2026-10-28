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
    cancel_requested,
    cancelled_at,
    created_at,
    updated_at,
    finished_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
)
ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL
DO NOTHING
RETURNING *;

-- name: GetJob :one
SELECT * FROM jobs
WHERE id = $1;

-- name: GetJobForUpdate :one
SELECT * FROM jobs
WHERE id = $1
FOR UPDATE;

-- name: GetJobByIdempotencyKey :one
SELECT * FROM jobs
WHERE queue = $1 AND idempotency_key = $2;

-- name: MarkExpiredLeasesDead :many
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
  AND attempts >= max_attempts
RETURNING *;

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

-- name: CompleteJobSucceeded :one
UPDATE jobs
SET
    status = 'succeeded',
    finished_at = sqlc.arg(now)::timestamptz,
    lease_owner = NULL,
    lease_expires_at = NULL,
    cancel_requested = false,
    updated_at = sqlc.arg(now)::timestamptz
WHERE id = sqlc.arg(id)
  AND status = 'running'
  AND lease_owner = sqlc.arg(owner)
  AND lease_expires_at > sqlc.arg(now)::timestamptz
  AND cancel_requested = false
RETURNING *;

-- name: CompleteJobCancelled :one
UPDATE jobs
SET
    status = 'cancelled',
    finished_at = sqlc.arg(now)::timestamptz,
    cancelled_at = sqlc.arg(now)::timestamptz,
    lease_owner = NULL,
    lease_expires_at = NULL,
    cancel_requested = false,
    updated_at = sqlc.arg(now)::timestamptz
WHERE id = sqlc.arg(id)
  AND status = 'running'
  AND lease_owner = sqlc.arg(owner)
  AND lease_expires_at > sqlc.arg(now)::timestamptz
  AND cancel_requested = true
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
  AND cancel_requested = false
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
  AND cancel_requested = false
RETURNING *;

-- name: FailJobCancelled :one
UPDATE jobs
SET
    status = 'cancelled',
    finished_at = sqlc.arg(now)::timestamptz,
    cancelled_at = sqlc.arg(now)::timestamptz,
    last_error = sqlc.arg(last_error),
    lease_owner = NULL,
    lease_expires_at = NULL,
    cancel_requested = false,
    updated_at = sqlc.arg(now)::timestamptz
WHERE id = sqlc.arg(id)
  AND status = 'running'
  AND lease_owner = sqlc.arg(owner)
  AND lease_expires_at > sqlc.arg(now)::timestamptz
  AND cancel_requested = true
RETURNING *;

-- name: ReleaseJob :one
UPDATE jobs
SET
    status = 'pending',
    attempts = CASE WHEN attempts > 0 THEN attempts - 1 ELSE 0 END,
    run_at = sqlc.arg(now)::timestamptz,
    lease_owner = NULL,
    lease_expires_at = NULL,
    updated_at = sqlc.arg(now)::timestamptz
WHERE id = sqlc.arg(id)
  AND status = 'running'
  AND lease_owner = sqlc.arg(owner)
  AND lease_expires_at > sqlc.arg(now)::timestamptz
RETURNING *;

-- name: CancelPendingJob :one
UPDATE jobs
SET
    status = 'cancelled',
    finished_at = sqlc.arg(now)::timestamptz,
    cancelled_at = sqlc.arg(now)::timestamptz,
    updated_at = sqlc.arg(now)::timestamptz
WHERE id = sqlc.arg(id)
  AND status = 'pending'
RETURNING *;

-- name: CancelRunningJob :one
UPDATE jobs
SET
    cancel_requested = true,
    updated_at = sqlc.arg(now)::timestamptz
WHERE id = sqlc.arg(id)
  AND status = 'running'
RETURNING *;

-- name: RetryJob :one
UPDATE jobs
SET
    status = 'pending',
    attempts = 0,
    run_at = sqlc.arg(now)::timestamptz,
    finished_at = NULL,
    cancelled_at = NULL,
    cancel_requested = false,
    lease_owner = NULL,
    lease_expires_at = NULL,
    updated_at = sqlc.arg(now)::timestamptz
WHERE id = sqlc.arg(id)
  AND status IN ('dead', 'cancelled')
RETURNING *;

-- name: ListJobsByStatus :many
SELECT * FROM jobs
WHERE queue = sqlc.arg(queue)
  AND status = sqlc.arg(status)
  AND (
    sqlc.arg(has_cursor)::boolean = false
    OR (updated_at, id) < (sqlc.arg(cursor_updated_at)::timestamptz, sqlc.arg(cursor_id)::uuid)
  )
ORDER BY updated_at DESC, id DESC
LIMIT sqlc.arg(page_limit);

-- name: ReapExpiredSelect :many
SELECT id FROM jobs
WHERE status = 'running'
  AND lease_expires_at <= sqlc.arg(now)::timestamptz
ORDER BY id ASC
FOR UPDATE SKIP LOCKED
LIMIT sqlc.arg(batch_limit);

-- name: ReapExpiredDead :one
UPDATE jobs
SET
    status = 'dead',
    last_error = 'lease expired',
    finished_at = sqlc.arg(now)::timestamptz,
    lease_owner = NULL,
    lease_expires_at = NULL,
    updated_at = sqlc.arg(now)::timestamptz
WHERE id = sqlc.arg(id)
  AND status = 'running'
  AND lease_expires_at <= sqlc.arg(now)::timestamptz
  AND attempts >= max_attempts
RETURNING *;

-- name: ReapExpiredRequeue :one
UPDATE jobs
SET
    status = 'pending',
    run_at = sqlc.arg(now)::timestamptz,
    lease_owner = NULL,
    lease_expires_at = NULL,
    updated_at = sqlc.arg(now)::timestamptz
WHERE id = sqlc.arg(id)
  AND status = 'running'
  AND lease_expires_at <= sqlc.arg(now)::timestamptz
  AND attempts < max_attempts
RETURNING *;

-- name: PurgeFinishedSelect :many
SELECT id FROM jobs
WHERE status IN ('succeeded', 'dead', 'cancelled')
  AND finished_at IS NOT NULL
  AND finished_at < sqlc.arg(before)::timestamptz
ORDER BY finished_at ASC
FOR UPDATE SKIP LOCKED
LIMIT sqlc.arg(batch_limit);

-- name: DeleteJobsByIDs :exec
DELETE FROM jobs
WHERE id = ANY(sqlc.arg(ids)::uuid[]);
