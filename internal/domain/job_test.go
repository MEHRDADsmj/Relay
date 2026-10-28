package domain

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestNewJob_DefaultsAndFields(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	id := JobID(uuid.MustParse("11111111-1111-1111-1111-111111111111"))

	job, err := NewJob(NewJobParams{
		Queue: "default",
		Type:  "send_email",
	}, id, now)
	if err != nil {
		t.Fatalf("NewJob() err = %v", err)
	}

	if job.ID != id {
		t.Fatalf("ID = %v, want %v", job.ID, id)
	}
	if job.Queue != "default" || job.Type != "send_email" {
		t.Fatalf("queue/type = %q/%q", job.Queue, job.Type)
	}
	if !bytes.Equal(job.Payload, []byte("{}")) {
		t.Fatalf("Payload = %q, want {}", job.Payload)
	}
	if job.Status != StatusPending {
		t.Fatalf("Status = %q, want pending", job.Status)
	}
	if job.Priority != DefaultPriority {
		t.Fatalf("Priority = %d", job.Priority)
	}
	if !job.RunAt.Equal(now) {
		t.Fatalf("RunAt = %v, want %v", job.RunAt, now)
	}
	if job.Attempts != 0 {
		t.Fatalf("Attempts = %d, want 0", job.Attempts)
	}
	if job.MaxAttempts != DefaultMaxAttempts {
		t.Fatalf("MaxAttempts = %d", job.MaxAttempts)
	}
	if job.LeaseOwner != "" || job.LeaseExpiresAt != nil {
		t.Fatalf("lease fields must be empty on create")
	}
	if job.IdempotencyKey != "" || job.LastError != "" {
		t.Fatalf("unexpected idempotency/error")
	}
	if !job.CreatedAt.Equal(now) || !job.UpdatedAt.Equal(now) {
		t.Fatalf("timestamps: created=%v updated=%v", job.CreatedAt, job.UpdatedAt)
	}
	if job.FinishedAt != nil {
		t.Fatalf("FinishedAt must be nil")
	}
}

func TestNewJob_ExplicitValues(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	runAt := now.Add(2 * time.Hour)
	id := JobID(uuid.MustParse("22222222-2222-2222-2222-222222222222"))
	prio := 42
	max := 3

	job, err := NewJob(NewJobParams{
		Queue:          "billing",
		Type:           "charge.card",
		Payload:        []byte(`{"n":1}`),
		Priority:       &prio,
		RunAt:          &runAt,
		MaxAttempts:    &max,
		IdempotencyKey: "idem-1",
	}, id, now)
	if err != nil {
		t.Fatalf("NewJob() err = %v", err)
	}
	if job.Priority != 42 || job.MaxAttempts != 3 {
		t.Fatalf("priority/max = %d/%d", job.Priority, job.MaxAttempts)
	}
	if !job.RunAt.Equal(runAt) {
		t.Fatalf("RunAt = %v", job.RunAt)
	}
	if job.IdempotencyKey != "idem-1" {
		t.Fatalf("IdempotencyKey = %q", job.IdempotencyKey)
	}
	if !bytes.Equal(job.Payload, []byte(`{"n":1}`)) {
		t.Fatalf("Payload = %q", job.Payload)
	}
}

func TestNewJob_PastRunAtBecomesNow(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	past := now.Add(-time.Minute)
	job, err := NewJob(NewJobParams{
		Queue: "q",
		Type:  "t",
		RunAt: &past,
	}, JobID(uuid.New()), now)
	if err != nil {
		t.Fatalf("NewJob() err = %v", err)
	}
	if !job.RunAt.Equal(now) {
		t.Fatalf("past RunAt should become now, got %v", job.RunAt)
	}
}

func TestNewJob_ValidationErrors(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	id := JobID(uuid.New())

	tests := []struct {
		name  string
		p     NewJobParams
		field string
	}{
		{name: "bad queue", p: NewJobParams{Queue: "BAD", Type: "t"}, field: "queue"},
		{name: "bad type", p: NewJobParams{Queue: "q", Type: ""}, field: "type"},
		{name: "bad payload", p: NewJobParams{Queue: "q", Type: "t", Payload: []byte(`{`)}, field: "payload"},
		{name: "bad priority", p: NewJobParams{Queue: "q", Type: "t", Priority: intPtr(101)}, field: "priority"},
		{name: "bad max", p: NewJobParams{Queue: "q", Type: "t", MaxAttempts: intPtr(0)}, field: "max_attempts"},
		{name: "bad idem", p: NewJobParams{Queue: "q", Type: "t", IdempotencyKey: "x\x00"}, field: "idempotency_key"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewJob(tt.p, id, now)
			assertValidationField(t, err, tt.field)
		})
	}
}

func TestDecideFailure(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	delay := 5 * time.Second

	tests := []struct {
		name       string
		attempts   int
		max        int
		wantStatus Status
		wantRunAt  bool
	}{
		{name: "retry when attempts below max", attempts: 1, max: 3, wantStatus: StatusPending, wantRunAt: true},
		{name: "retry at attempts max-1", attempts: 2, max: 3, wantStatus: StatusPending, wantRunAt: true},
		{name: "dead when attempts equals max", attempts: 3, max: 3, wantStatus: StatusDead, wantRunAt: false},
		{name: "dead when attempts above max", attempts: 5, max: 3, wantStatus: StatusDead, wantRunAt: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			j := Job{
				Status:      StatusRunning,
				Attempts:    tt.attempts,
				MaxAttempts: tt.max,
			}
			d := j.DecideFailure(now, delay, false)
			if d.NextStatus != tt.wantStatus {
				t.Fatalf("NextStatus = %q, want %q", d.NextStatus, tt.wantStatus)
			}
			if tt.wantRunAt {
				if d.NextRunAt == nil {
					t.Fatal("NextRunAt is nil, want set")
				}
				want := now.Add(delay)
				if !d.NextRunAt.Equal(want) {
					t.Fatalf("NextRunAt = %v, want %v", *d.NextRunAt, want)
				}
			} else if d.NextRunAt != nil {
				t.Fatalf("NextRunAt = %v, want nil", *d.NextRunAt)
			}
		})
	}
}

func TestEnsureLeasedBy(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	exp := now.Add(30 * time.Second)
	expired := now.Add(-time.Second)

	tests := []struct {
		name    string
		job     Job
		owner   string
		wantErr error
	}{
		{
			name: "ok",
			job: Job{
				Status:         StatusRunning,
				LeaseOwner:     "w1",
				LeaseExpiresAt: &exp,
			},
			owner: "w1",
		},
		{
			name: "not running",
			job: Job{
				Status:         StatusPending,
				LeaseOwner:     "w1",
				LeaseExpiresAt: &exp,
			},
			owner:   "w1",
			wantErr: ErrLeaseLost,
		},
		{
			name: "owner mismatch",
			job: Job{
				Status:         StatusRunning,
				LeaseOwner:     "w1",
				LeaseExpiresAt: &exp,
			},
			owner:   "w2",
			wantErr: ErrLeaseLost,
		},
		{
			name: "expired",
			job: Job{
				Status:         StatusRunning,
				LeaseOwner:     "w1",
				LeaseExpiresAt: &expired,
			},
			owner:   "w1",
			wantErr: ErrLeaseLost,
		},
		{
			name: "nil expiry",
			job: Job{
				Status:     StatusRunning,
				LeaseOwner: "w1",
			},
			owner:   "w1",
			wantErr: ErrLeaseLost,
		},
		{
			name: "expiry equal now is lost",
			job: Job{
				Status:         StatusRunning,
				LeaseOwner:     "w1",
				LeaseExpiresAt: &now,
			},
			owner:   "w1",
			wantErr: ErrLeaseLost,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.job.EnsureLeasedBy(tt.owner, now)
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("EnsureLeasedBy() = %v", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("EnsureLeasedBy() = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestStatusConstants(t *testing.T) {
	t.Parallel()
	if StatusPending != "pending" || StatusRunning != "running" ||
		StatusSucceeded != "succeeded" || StatusDead != "dead" ||
		StatusCancelled != "cancelled" {
		t.Fatalf("unexpected status constants")
	}
}
