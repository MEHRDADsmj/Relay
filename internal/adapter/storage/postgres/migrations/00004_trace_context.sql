-- +goose Up
ALTER TABLE jobs ADD COLUMN trace_context jsonb;

-- +goose Down
ALTER TABLE jobs DROP COLUMN IF EXISTS trace_context;
