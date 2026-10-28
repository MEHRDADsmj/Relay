-- +goose Up
ALTER TABLE jobs DROP CONSTRAINT jobs_status_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_status_check
    CHECK (status IN ('pending','running','succeeded','dead','cancelled'));
ALTER TABLE jobs ADD COLUMN cancel_requested boolean NOT NULL DEFAULT false;
ALTER TABLE jobs ADD COLUMN cancelled_at timestamptz;

CREATE TABLE job_attempts (
    job_id      uuid        NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    number      int         NOT NULL,
    worker_id   text        NOT NULL,
    started_at  timestamptz NOT NULL,
    finished_at timestamptz,
    outcome     text        NOT NULL CHECK (outcome IN
                  ('running','succeeded','failed','expired','released','cancelled')),
    error       text,
    PRIMARY KEY (job_id, number, started_at)
);

CREATE TABLE queues (
    name       text PRIMARY KEY,
    paused     boolean     NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);
INSERT INTO queues (name, created_at, updated_at)
    SELECT DISTINCT queue, now(), now() FROM jobs ON CONFLICT DO NOTHING;

CREATE INDEX jobs_status_updated ON jobs (queue, status, updated_at DESC, id DESC);
CREATE INDEX jobs_finished ON jobs (finished_at)
    WHERE status IN ('succeeded','dead','cancelled');
CREATE INDEX jobs_running_lease_all ON jobs (lease_expires_at) WHERE status = 'running';

-- +goose Down
DROP INDEX IF EXISTS jobs_running_lease_all;
DROP INDEX IF EXISTS jobs_finished;
DROP INDEX IF EXISTS jobs_status_updated;
DROP TABLE IF EXISTS queues;
DROP TABLE IF EXISTS job_attempts;
ALTER TABLE jobs DROP COLUMN IF EXISTS cancelled_at;
ALTER TABLE jobs DROP COLUMN IF EXISTS cancel_requested;
ALTER TABLE jobs DROP CONSTRAINT jobs_status_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_status_check
    CHECK (status IN ('pending','running','succeeded','dead'));
