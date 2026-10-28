package redis

import (
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

	payload := []byte(m["payload"])
	if len(payload) == 0 {
		payload = []byte("{}")
	}

	return domain.Job{
		ID:             id,
		Queue:          m["queue"],
		Type:           m["type"],
		Payload:        payload,
		Status:         domain.Status(m["status"]),
		Priority:       priority,
		RunAt:          runAt,
		Attempts:       attempts,
		MaxAttempts:    maxAttempts,
		LeaseOwner:     m["lease_owner"],
		LeaseExpiresAt: leaseExp,
		IdempotencyKey: m["idempotency_key"],
		LastError:      m["last_error"],
		CreatedAt:      createdAt,
		UpdatedAt:      updatedAt,
		FinishedAt:     finishedAt,
	}, nil
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
