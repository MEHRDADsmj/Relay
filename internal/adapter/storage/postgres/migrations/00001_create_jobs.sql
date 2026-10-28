-- +goose Up
CREATE TABLE jobs (
    id               uuid PRIMARY KEY,
    queue            text        NOT NULL,
    type             text        NOT NULL,
    payload          jsonb       NOT NULL,
    status           text        NOT NULL CHECK (status IN ('pending','running','succeeded','dead')),
    priority         smallint    NOT NULL DEFAULT 0 CHECK (priority BETWEEN -100 AND 100),
    run_at           timestamptz NOT NULL,
    attempts         int         NOT NULL DEFAULT 0,
    max_attempts     int         NOT NULL,
    lease_owner      text,
    lease_expires_at timestamptz,
    idempotency_key  text,
    last_error       text,
    created_at       timestamptz NOT NULL,
    updated_at       timestamptz NOT NULL,
    finished_at      timestamptz
);
CREATE UNIQUE INDEX jobs_idempotency_uq ON jobs (queue, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
CREATE INDEX jobs_claimable_pending ON jobs (queue, priority DESC, run_at, id)
    WHERE status = 'pending';
CREATE INDEX jobs_running_lease ON jobs (queue, lease_expires_at)
    WHERE status = 'running';
-- +goose Down
DROP TABLE jobs;
