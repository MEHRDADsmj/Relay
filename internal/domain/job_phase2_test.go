package domain

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestStatusCancelledConstant(t *testing.T) {
	t.Parallel()
	if StatusCancelled != "cancelled" {
		t.Fatalf("StatusCancelled = %q, want cancelled", StatusCancelled)
	}
}

func TestNewJob_CancelFieldsZero(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	job, err := NewJob(NewJobParams{Queue: "q", Type: "t"}, JobID(uuid.New()), now)
	if err != nil {
		t.Fatalf("NewJob() err = %v", err)
	}
	if job.CancelRequested {
		t.Fatal("CancelRequested must be false")
	}
	if job.CancelledAt != nil {
		t.Fatal("CancelledAt must be nil")
	}
}

func TestCanCancel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		status  Status
		wantErr error
	}{
		{name: "pending ok", status: StatusPending},
		{name: "running ok", status: StatusRunning},
		{name: "succeeded invalid", status: StatusSucceeded, wantErr: ErrInvalidTransition},
		{name: "dead invalid", status: StatusDead, wantErr: ErrInvalidTransition},
		{name: "cancelled invalid", status: StatusCancelled, wantErr: ErrInvalidTransition},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := Job{Status: tt.status}.CanCancel()
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("CanCancel() = %v", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("CanCancel() = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestCanRetry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		status  Status
		wantErr error
	}{
		{name: "dead ok", status: StatusDead},
		{name: "cancelled ok", status: StatusCancelled},
		{name: "pending invalid", status: StatusPending, wantErr: ErrInvalidTransition},
		{name: "running invalid", status: StatusRunning, wantErr: ErrInvalidTransition},
		{name: "succeeded invalid", status: StatusSucceeded, wantErr: ErrInvalidTransition},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := Job{Status: tt.status}.CanRetry()
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("CanRetry() = %v", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("CanRetry() = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestResolveOutcome(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		cancelRequested bool
		requested       Status
		want            Status
	}{
		{name: "no cancel succeed", cancelRequested: false, requested: StatusSucceeded, want: StatusSucceeded},
		{name: "no cancel dead", cancelRequested: false, requested: StatusDead, want: StatusDead},
		{name: "no cancel pending", cancelRequested: false, requested: StatusPending, want: StatusPending},
		{name: "cancel requested succeed → cancelled", cancelRequested: true, requested: StatusSucceeded, want: StatusCancelled},
		{name: "cancel requested dead → cancelled", cancelRequested: true, requested: StatusDead, want: StatusCancelled},
		{name: "cancel requested pending → cancelled", cancelRequested: true, requested: StatusPending, want: StatusCancelled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := Job{CancelRequested: tt.cancelRequested}.ResolveOutcome(tt.requested)
			if got != tt.want {
				t.Fatalf("ResolveOutcome(%q) = %q, want %q", tt.requested, got, tt.want)
			}
		})
	}
}

func TestDecideFailure_Permanent(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	delay := 5 * time.Second

	j := Job{Status: StatusRunning, Attempts: 1, MaxAttempts: 10}
	d := j.DecideFailure(now, delay, true)
	if d.NextStatus != StatusDead {
		t.Fatalf("NextStatus = %q, want dead", d.NextStatus)
	}
	if d.NextRunAt != nil {
		t.Fatalf("NextRunAt = %v, want nil", *d.NextRunAt)
	}

	d = j.DecideFailure(now, delay, false)
	if d.NextStatus != StatusPending {
		t.Fatalf("non-permanent NextStatus = %q, want pending", d.NextStatus)
	}
	if d.NextRunAt == nil || !d.NextRunAt.Equal(now.Add(delay)) {
		t.Fatalf("non-permanent NextRunAt = %v", d.NextRunAt)
	}
}

func TestStateMachineTable(t *testing.T) {
	t.Parallel()

	type actionKind string
	const (
		actCancel           actionKind = "cancel"
		actRetry            actionKind = "retry"
		actResolveSucceeded actionKind = "resolve_succeeded"
		actResolveFailed    actionKind = "resolve_failed"
		actResolvePending   actionKind = "resolve_pending"
	)

	statuses := []Status{
		StatusPending, StatusRunning, StatusSucceeded, StatusDead, StatusCancelled,
	}

	type want struct {
		err            error
		resolvedStatus Status // only for resolve_* ; empty means N/A
	}

	// Expected outcomes for every (status, action). Resolve* uses CancelRequested=false.
	matrix := map[Status]map[actionKind]want{
		StatusPending: {
			actCancel:           {err: nil},
			actRetry:            {err: ErrInvalidTransition},
			actResolveSucceeded: {resolvedStatus: StatusSucceeded},
			actResolveFailed:    {resolvedStatus: StatusDead},
			actResolvePending:   {resolvedStatus: StatusPending},
		},
		StatusRunning: {
			actCancel:           {err: nil},
			actRetry:            {err: ErrInvalidTransition},
			actResolveSucceeded: {resolvedStatus: StatusSucceeded},
			actResolveFailed:    {resolvedStatus: StatusDead},
			actResolvePending:   {resolvedStatus: StatusPending},
		},
		StatusSucceeded: {
			actCancel:           {err: ErrInvalidTransition},
			actRetry:            {err: ErrInvalidTransition},
			actResolveSucceeded: {resolvedStatus: StatusSucceeded},
			actResolveFailed:    {resolvedStatus: StatusDead},
			actResolvePending:   {resolvedStatus: StatusPending},
		},
		StatusDead: {
			actCancel:           {err: ErrInvalidTransition},
			actRetry:            {err: nil},
			actResolveSucceeded: {resolvedStatus: StatusSucceeded},
			actResolveFailed:    {resolvedStatus: StatusDead},
			actResolvePending:   {resolvedStatus: StatusPending},
		},
		StatusCancelled: {
			actCancel:           {err: ErrInvalidTransition},
			actRetry:            {err: nil},
			actResolveSucceeded: {resolvedStatus: StatusSucceeded},
			actResolveFailed:    {resolvedStatus: StatusDead},
			actResolvePending:   {resolvedStatus: StatusPending},
		},
	}

	actions := []actionKind{
		actCancel, actRetry, actResolveSucceeded, actResolveFailed, actResolvePending,
	}

	for _, st := range statuses {
		for _, act := range actions {
			st, act := st, act
			exp := matrix[st][act]
			t.Run(string(st)+"/"+string(act), func(t *testing.T) {
				t.Parallel()
				j := Job{Status: st}
				switch act {
				case actCancel:
					err := j.CanCancel()
					if exp.err == nil {
						if err != nil {
							t.Fatalf("CanCancel() = %v", err)
						}
					} else if !errors.Is(err, exp.err) {
						t.Fatalf("CanCancel() = %v, want %v", err, exp.err)
					}
				case actRetry:
					err := j.CanRetry()
					if exp.err == nil {
						if err != nil {
							t.Fatalf("CanRetry() = %v", err)
						}
					} else if !errors.Is(err, exp.err) {
						t.Fatalf("CanRetry() = %v, want %v", err, exp.err)
					}
				case actResolveSucceeded:
					got := j.ResolveOutcome(StatusSucceeded)
					if got != exp.resolvedStatus {
						t.Fatalf("ResolveOutcome(succeeded) = %q, want %q", got, exp.resolvedStatus)
					}
				case actResolveFailed:
					got := j.ResolveOutcome(StatusDead)
					if got != exp.resolvedStatus {
						t.Fatalf("ResolveOutcome(dead) = %q, want %q", got, exp.resolvedStatus)
					}
				case actResolvePending:
					got := j.ResolveOutcome(StatusPending)
					if got != exp.resolvedStatus {
						t.Fatalf("ResolveOutcome(pending) = %q, want %q", got, exp.resolvedStatus)
					}
				}
			})
		}
	}

	// Running + CancelRequested: any resolve → cancelled
	t.Run("running/cancel_requested/resolve_all_cancelled", func(t *testing.T) {
		t.Parallel()
		j := Job{Status: StatusRunning, CancelRequested: true}
		for _, req := range []Status{StatusSucceeded, StatusDead, StatusPending} {
			if got := j.ResolveOutcome(req); got != StatusCancelled {
				t.Fatalf("ResolveOutcome(%q) = %q, want cancelled", req, got)
			}
		}
	})
}

func TestAttemptOutcomeConstants(t *testing.T) {
	t.Parallel()
	want := map[AttemptOutcome]string{
		AttemptRunning:   "running",
		AttemptSucceeded: "succeeded",
		AttemptFailed:    "failed",
		AttemptExpired:   "expired",
		AttemptReleased:  "released",
		AttemptCancelled: "cancelled",
	}
	for c, s := range want {
		if string(c) != s {
			t.Fatalf("%q != %q", c, s)
		}
	}
}

func TestAttemptQueuePageTypesCompile(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	fin := now.Add(time.Second)
	_ = Attempt{
		JobID:      JobID(uuid.MustParse("33333333-3333-3333-3333-333333333333")),
		Number:     1,
		WorkerID:   "w1",
		StartedAt:  now,
		FinishedAt: &fin,
		Outcome:    AttemptSucceeded,
		Error:      "",
	}
	_ = Queue{Name: "default", Paused: false, CreatedAt: now, UpdatedAt: now}
	_ = QueueStats{
		Queue: "default", Paused: false,
		Pending: 1, Scheduled: 0, Running: 0, Succeeded: 0, Dead: 0, Cancelled: 0,
		OldestPendingAge: time.Second,
	}
	_ = PageRequest{Limit: 50, Cursor: ""}
	_ = Page[Job]{Items: nil, NextCursor: ""}
}
