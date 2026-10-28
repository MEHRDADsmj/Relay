package redis

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/MEHRDADsmj/relay/internal/domain"
)

func toUnixMs(t time.Time) int64 {
	return t.UTC().UnixMilli()
}

func msString(t time.Time) string {
	return strconv.FormatInt(toUnixMs(t), 10)
}

func optionalMsString(t *time.Time) string {
	if t == nil {
		return ""
	}
	return msString(*t)
}

func parseMs(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, fmt.Errorf("empty timestamp")
	}
	ms, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(ms).UTC(), nil
}

func parseOptionalMs(s string) (*time.Time, error) {
	if s == "" {
		return nil, nil
	}
	t, err := parseMs(s)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func boolFromRedis(s string) bool {
	return s == "1" || s == "true"
}

func boolToRedis(v bool) string {
	if v {
		return "1"
	}
	return "0"
}

func hashToJob(flat []string) (domain.Job, error) {
	if len(flat)%2 != 0 {
		return domain.Job{}, fmt.Errorf("redis hash: odd field count %d", len(flat))
	}
	m := make(map[string]string, len(flat)/2)
	for i := 0; i < len(flat); i += 2 {
		m[flat[i]] = flat[i+1]
	}
	return mapToJob(m)
}

func mapToJob(m map[string]string) (domain.Job, error) {
	id, err := uuid.Parse(m["id"])
	if err != nil {
		return domain.Job{}, fmt.Errorf("redis job id: %w", err)
	}
	priority, err := strconv.Atoi(m["priority"])
	if err != nil {
		return domain.Job{}, fmt.Errorf("redis job priority: %w", err)
	}
	attempts, err := strconv.Atoi(m["attempts"])
	if err != nil {
		return domain.Job{}, fmt.Errorf("redis job attempts: %w", err)
	}
	maxAttempts, err := strconv.Atoi(m["max_attempts"])
	if err != nil {
		return domain.Job{}, fmt.Errorf("redis job max_attempts: %w", err)
	}
	runAt, err := parseMs(m["run_at"])
	if err != nil {
		return domain.Job{}, fmt.Errorf("redis job run_at: %w", err)
	}
	createdAt, err := parseMs(m["created_at"])
	if err != nil {
		return domain.Job{}, fmt.Errorf("redis job created_at: %w", err)
	}
	updatedAt, err := parseMs(m["updated_at"])
	if err != nil {
		return domain.Job{}, fmt.Errorf("redis job updated_at: %w", err)
	}
	leaseExp, err := parseOptionalMs(m["lease_expires_at"])
	if err != nil {
		return domain.Job{}, fmt.Errorf("redis job lease_expires_at: %w", err)
	}
	finishedAt, err := parseOptionalMs(m["finished_at"])
	if err != nil {
		return domain.Job{}, fmt.Errorf("redis job finished_at: %w", err)
	}
	cancelledAt, err := parseOptionalMs(m["cancelled_at"])
	if err != nil {
		return domain.Job{}, fmt.Errorf("redis job cancelled_at: %w", err)
	}

	payload := []byte(m["payload"])
	if len(payload) == 0 {
		payload = []byte("{}")
	}

	return domain.Job{
		ID:              id,
		Queue:           m["queue"],
		Type:            m["type"],
		Payload:         payload,
		Status:          domain.Status(m["status"]),
		Priority:        priority,
		RunAt:           runAt,
		Attempts:        attempts,
		MaxAttempts:     maxAttempts,
		LeaseOwner:      m["lease_owner"],
		LeaseExpiresAt:  leaseExp,
		IdempotencyKey:  m["idempotency_key"],
		LastError:       m["last_error"],
		CreatedAt:       createdAt,
		UpdatedAt:       updatedAt,
		FinishedAt:      finishedAt,
		CancelRequested: boolFromRedis(m["cancel_requested"]),
		CancelledAt:     cancelledAt,
		TraceContext:    decodeTraceContextJSON(m["trace_context"]),
	}, nil
}

func encodeTraceContextJSON(m map[string]string) string {
	if len(m) == 0 {
		return ""
	}
	b, err := json.Marshal(m)
	if err != nil {
		return ""
	}
	return string(b)
}

func decodeTraceContextJSON(s string) map[string]string {
	if s == "" || s == "null" {
		return nil
	}
	var m map[string]string
	if err := json.Unmarshal([]byte(s), &m); err != nil || len(m) == 0 {
		return nil
	}
	return m
}

type attemptJSON struct {
	JobID      string `json:"job_id"`
	Number     int    `json:"number"`
	WorkerID   string `json:"worker_id"`
	StartedAt  int64  `json:"started_at"`
	FinishedAt *int64 `json:"finished_at"`
	Outcome    string `json:"outcome"`
	Error      string `json:"error"`
}

func decodeAttempt(raw string) (domain.Attempt, error) {
	var a attemptJSON
	if err := json.Unmarshal([]byte(raw), &a); err != nil {
		return domain.Attempt{}, err
	}
	id, err := uuid.Parse(a.JobID)
	if err != nil {
		return domain.Attempt{}, err
	}
	out := domain.Attempt{
		JobID:     id,
		Number:    a.Number,
		WorkerID:  a.WorkerID,
		StartedAt: time.UnixMilli(a.StartedAt).UTC(),
		Outcome:   domain.AttemptOutcome(a.Outcome),
		Error:     a.Error,
	}
	if a.FinishedAt != nil {
		t := time.UnixMilli(*a.FinishedAt).UTC()
		out.FinishedAt = &t
	}
	return out, nil
}

func interfaceSliceToStrings(v any) ([]string, error) {
	arr, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("redis script: expected []interface{}, got %T", v)
	}
	out := make([]string, len(arr))
	for i, x := range arr {
		switch t := x.(type) {
		case string:
			out[i] = t
		case []byte:
			out[i] = string(t)
		default:
			out[i] = fmt.Sprint(t)
		}
	}
	return out, nil
}

func mapScriptJobResult(res any) (domain.Job, error) {
	arr, err := interfaceSliceToStrings(res)
	if err != nil || len(arr) < 1 {
		return domain.Job{}, fmt.Errorf("redis script: bad result %v: %w", res, err)
	}
	switch arr[0] {
	case "NOT_FOUND":
		return domain.Job{}, domain.ErrNotFound
	case "LEASE_LOST":
		return domain.Job{}, domain.ErrLeaseLost
	case "INVALID":
		return domain.Job{}, domain.ErrInvalidTransition
	case "OK":
		if len(arr) < 2 {
			return domain.Job{}, fmt.Errorf("redis script: OK without hash")
		}
		return hashToJob(arr[1:])
	default:
		return domain.Job{}, fmt.Errorf("redis script: unknown code %q", arr[0])
	}
}

func mapToQueue(name string, m map[string]string) (domain.Queue, error) {
	created, err := parseMs(m["created_at"])
	if err != nil {
		return domain.Queue{}, err
	}
	updated, err := parseMs(m["updated_at"])
	if err != nil {
		return domain.Queue{}, err
	}
	return domain.Queue{
		Name:      name,
		Paused:    boolFromRedis(m["paused"]),
		CreatedAt: created,
		UpdatedAt: updated,
	}, nil
}
