package http

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/MEHRDADsmj/relay/internal/adapter/http/oapi"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

func BenchmarkJobToAPI(b *testing.B) {
	now := time.Now().UTC()
	exp := now.Add(30 * time.Second)
	job := domain.Job{
		ID:             uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		Queue:          "bench",
		Type:           "bench",
		Payload:        []byte(`{"enqueued_at_unix_ns":1,"pad":"xxxxxxxx"}`),
		Status:         domain.StatusPending,
		Priority:       0,
		RunAt:          now,
		Attempts:       0,
		MaxAttempts:    3,
		LeaseOwner:     "worker-1",
		LeaseExpiresAt: &exp,
		IdempotencyKey: "idem-1",
		CreatedAt:      now,
		UpdatedAt:      now,
		TraceContext:   map[string]string{"traceparent": "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = jobToAPI(job)
	}
}

func BenchmarkEnqueueCommand(b *testing.B) {
	payload := json.RawMessage(`{"n":1}`)
	key := "idem-1"
	prio := 0
	body := oapi.EnqueueRequest{
		Queue:          "bench",
		Type:           "bench",
		Payload:        &payload,
		Priority:       &prio,
		IdempotencyKey: &key,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = enqueueCommand(body)
	}
}
