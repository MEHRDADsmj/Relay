-- +goose Up
CREATE TABLE queue_stats_minutely (
    queue     text        NOT NULL,
    minute    timestamptz NOT NULL,
    enqueued  bigint      NOT NULL DEFAULT 0,
    succeeded bigint      NOT NULL DEFAULT 0,
    failed    bigint      NOT NULL DEFAULT 0,
    dead      bigint      NOT NULL DEFAULT 0,
    PRIMARY KEY (queue, minute)
);

-- +goose Down
DROP TABLE IF EXISTS queue_stats_minutely;
