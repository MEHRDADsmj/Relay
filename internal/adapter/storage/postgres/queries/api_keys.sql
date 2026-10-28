-- name: CreateAPIKey :one
INSERT INTO api_keys (
    id, name, prefix, hash, scopes, created_at, last_used_at, expires_at, revoked_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9
)
RETURNING *;

-- name: GetAPIKey :one
SELECT * FROM api_keys
WHERE id = $1;

-- name: GetAPIKeyByPrefix :one
SELECT * FROM api_keys
WHERE prefix = $1;

-- name: ListAPIKeys :many
SELECT * FROM api_keys
ORDER BY created_at ASC, id ASC;

-- name: RevokeAPIKey :one
UPDATE api_keys
SET revoked_at = sqlc.arg(revoked_at)
WHERE id = sqlc.arg(id)
RETURNING *;

-- name: UpdateAPIKeyLastUsed :execrows
UPDATE api_keys
SET last_used_at = sqlc.arg(last_used_at)
WHERE id = sqlc.arg(id);
