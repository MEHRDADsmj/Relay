-- +goose Up
CREATE TABLE schedules (
    id           uuid PRIMARY KEY,
    name         text UNIQUE NOT NULL,
    cron         text NOT NULL,
    timezone     text NOT NULL,
    queue        text NOT NULL,
    job_type     text NOT NULL,
    payload      jsonb NOT NULL,
    priority     smallint NOT NULL,
    max_attempts int NOT NULL,
    enabled      boolean NOT NULL,
    next_run_at  timestamptz NOT NULL,
    last_run_at  timestamptz,
    last_job_id  uuid,
    created_at   timestamptz NOT NULL,
    updated_at   timestamptz NOT NULL
);
CREATE INDEX schedules_due ON schedules (next_run_at) WHERE enabled;

CREATE TABLE api_keys (
    id           uuid PRIMARY KEY,
    name         text NOT NULL,
    prefix       text UNIQUE NOT NULL,
    hash         bytea NOT NULL,
    scopes       text[] NOT NULL,
    created_at   timestamptz NOT NULL,
    last_used_at timestamptz,
    expires_at   timestamptz,
    revoked_at   timestamptz
);

-- +goose Down
DROP TABLE IF EXISTS api_keys;
DROP INDEX IF EXISTS schedules_due;
DROP TABLE IF EXISTS schedules;
