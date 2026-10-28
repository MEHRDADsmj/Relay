package sdk

import (
	"encoding/json"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	relayv1 "github.com/MEHRDADsmj/relay/gen/relay/v1"
)

func jobFromProto(j *relayv1.Job) apiJob {
	if j == nil {
		return apiJob{}
	}
	out := apiJob{
		ID:              j.GetId(),
		Queue:           j.GetQueue(),
		Type:            j.GetType(),
		Payload:         append(json.RawMessage(nil), j.GetPayload()...),
		Status:          jobStatusString(j.GetStatus()),
		Priority:        int(j.GetPriority()),
		Attempts:        int(j.GetAttempts()),
		MaxAttempts:     int(j.GetMaxAttempts()),
		CancelRequested: j.GetCancelRequested(),
	}
	if ts := j.GetRunAt(); ts != nil {
		out.RunAt = ts.AsTime()
	}
	if ts := j.GetCreatedAt(); ts != nil {
		out.CreatedAt = ts.AsTime()
	}
	if ts := j.GetUpdatedAt(); ts != nil {
		out.UpdatedAt = ts.AsTime()
	}
	if owner := j.GetLeaseOwner(); owner != "" {
		out.LeaseOwner = &owner
	}
	if ts := j.GetLeaseExpiresAt(); ts != nil {
		t := ts.AsTime()
		out.LeaseExpiresAt = &t
	}
	if key := j.GetIdempotencyKey(); key != "" {
		out.IdempotencyKey = &key
	}
	if errMsg := j.GetLastError(); errMsg != "" {
		out.LastError = &errMsg
	}
	if ts := j.GetCancelledAt(); ts != nil {
		t := ts.AsTime()
		out.CancelledAt = &t
	}
	if ts := j.GetFinishedAt(); ts != nil {
		t := ts.AsTime()
		out.FinishedAt = &t
	}
	if tc := j.GetTraceContext(); len(tc) > 0 {
		out.TraceContext = make(map[string]string, len(tc))
		for k, v := range tc {
			out.TraceContext[k] = v
		}
	}
	return out
}

func jobInfoFromProto(j *relayv1.Job) JobInfo {
	return jobFromProto(j).toJobInfo()
}

func jobStatusString(s relayv1.JobStatus) string {
	switch s {
	case relayv1.JobStatus_JOB_STATUS_PENDING:
		return "pending"
	case relayv1.JobStatus_JOB_STATUS_RUNNING:
		return "running"
	case relayv1.JobStatus_JOB_STATUS_SUCCEEDED:
		return "succeeded"
	case relayv1.JobStatus_JOB_STATUS_DEAD:
		return "dead"
	case relayv1.JobStatus_JOB_STATUS_CANCELLED:
		return "cancelled"
	default:
		return ""
	}
}

func jobStatusFromString(s string) (relayv1.JobStatus, error) {
	switch s {
	case "pending":
		return relayv1.JobStatus_JOB_STATUS_PENDING, nil
	case "running":
		return relayv1.JobStatus_JOB_STATUS_RUNNING, nil
	case "succeeded":
		return relayv1.JobStatus_JOB_STATUS_SUCCEEDED, nil
	case "dead":
		return relayv1.JobStatus_JOB_STATUS_DEAD, nil
	case "cancelled":
		return relayv1.JobStatus_JOB_STATUS_CANCELLED, nil
	default:
		return relayv1.JobStatus_JOB_STATUS_UNSPECIFIED, fmt.Errorf("relay: unknown job status %q", s)
	}
}

func attemptFromProto(a *relayv1.Attempt) AttemptInfo {
	if a == nil {
		return AttemptInfo{}
	}
	info := AttemptInfo{
		JobID:    a.GetJobId(),
		Number:   int(a.GetNumber()),
		WorkerID: a.GetWorkerId(),
		Outcome:  attemptOutcomeString(a.GetOutcome()),
		Error:    a.GetError(),
	}
	if ts := a.GetStartedAt(); ts != nil {
		info.StartedAt = ts.AsTime()
	}
	if ts := a.GetFinishedAt(); ts != nil {
		t := ts.AsTime()
		info.FinishedAt = &t
	}
	return info
}

func attemptOutcomeString(o relayv1.AttemptOutcome) string {
	switch o {
	case relayv1.AttemptOutcome_ATTEMPT_OUTCOME_RUNNING:
		return "running"
	case relayv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED:
		return "succeeded"
	case relayv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED:
		return "failed"
	case relayv1.AttemptOutcome_ATTEMPT_OUTCOME_EXPIRED:
		return "expired"
	case relayv1.AttemptOutcome_ATTEMPT_OUTCOME_RELEASED:
		return "released"
	case relayv1.AttemptOutcome_ATTEMPT_OUTCOME_CANCELLED:
		return "cancelled"
	default:
		return ""
	}
}

func queueFromProto(q *relayv1.Queue) QueueInfo {
	if q == nil {
		return QueueInfo{}
	}
	info := QueueInfo{
		Name:   q.GetName(),
		Paused: q.GetPaused(),
	}
	if ts := q.GetCreatedAt(); ts != nil {
		info.CreatedAt = ts.AsTime()
	}
	if ts := q.GetUpdatedAt(); ts != nil {
		info.UpdatedAt = ts.AsTime()
	}
	return info
}

func queueStatsFromProto(s *relayv1.QueueStats) QueueStatsInfo {
	if s == nil {
		return QueueStatsInfo{}
	}
	return QueueStatsInfo{
		Queue:            s.GetQueue(),
		Paused:           s.GetPaused(),
		Pending:          s.GetPending(),
		Scheduled:        s.GetScheduled(),
		Running:          s.GetRunning(),
		Succeeded:        s.GetSucceeded(),
		Dead:             s.GetDead(),
		Cancelled:        s.GetCancelled(),
		OldestPendingAge: time.Duration(s.GetOldestPendingAgeSeconds()) * time.Second,
	}
}

func scheduleFromProto(s *relayv1.Schedule) ScheduleInfo {
	if s == nil {
		return ScheduleInfo{}
	}
	info := ScheduleInfo{
		ID:          s.GetId(),
		Name:        s.GetName(),
		Cron:        s.GetCron(),
		Timezone:    s.GetTimezone(),
		Queue:       s.GetQueue(),
		Type:        s.GetType(),
		Payload:     append([]byte(nil), s.GetPayload()...),
		Priority:    int(s.GetPriority()),
		MaxAttempts: int(s.GetMaxAttempts()),
		Enabled:     s.GetEnabled(),
		LastJobID:   s.GetLastJobId(),
	}
	if ts := s.GetNextRunAt(); ts != nil {
		info.NextRunAt = ts.AsTime()
	}
	if ts := s.GetLastRunAt(); ts != nil {
		t := ts.AsTime()
		info.LastRunAt = &t
	}
	if ts := s.GetCreatedAt(); ts != nil {
		info.CreatedAt = ts.AsTime()
	}
	if ts := s.GetUpdatedAt(); ts != nil {
		info.UpdatedAt = ts.AsTime()
	}
	return info
}

func apiKeyFromProto(k *relayv1.APIKey) APIKeyInfo {
	if k == nil {
		return APIKeyInfo{}
	}
	info := APIKeyInfo{
		ID:     k.GetId(),
		Name:   k.GetName(),
		Prefix: k.GetPrefix(),
		Scopes: scopesFromProto(k.GetScopes()),
	}
	if ts := k.GetCreatedAt(); ts != nil {
		info.CreatedAt = ts.AsTime()
	}
	if ts := k.GetLastUsedAt(); ts != nil {
		t := ts.AsTime()
		info.LastUsedAt = &t
	}
	if ts := k.GetExpiresAt(); ts != nil {
		t := ts.AsTime()
		info.ExpiresAt = &t
	}
	if ts := k.GetRevokedAt(); ts != nil {
		t := ts.AsTime()
		info.RevokedAt = &t
	}
	return info
}

func scopesFromProto(scopes []relayv1.Scope) []string {
	out := make([]string, 0, len(scopes))
	for _, s := range scopes {
		switch s {
		case relayv1.Scope_SCOPE_PRODUCER:
			out = append(out, ScopeProducer)
		case relayv1.Scope_SCOPE_WORKER:
			out = append(out, ScopeWorker)
		case relayv1.Scope_SCOPE_ADMIN:
			out = append(out, ScopeAdmin)
		case relayv1.Scope_SCOPE_DASHBOARD_READ:
			out = append(out, ScopeDashboardRead)
		}
	}
	return out
}

func scopesToProto(scopes []string) ([]relayv1.Scope, error) {
	out := make([]relayv1.Scope, 0, len(scopes))
	for _, s := range scopes {
		switch s {
		case ScopeProducer:
			out = append(out, relayv1.Scope_SCOPE_PRODUCER)
		case ScopeWorker:
			out = append(out, relayv1.Scope_SCOPE_WORKER)
		case ScopeAdmin:
			out = append(out, relayv1.Scope_SCOPE_ADMIN)
		case ScopeDashboardRead:
			out = append(out, relayv1.Scope_SCOPE_DASHBOARD_READ)
		default:
			return nil, fmt.Errorf("relay: unknown scope %q", s)
		}
	}
	return out, nil
}

func marshalPayload(v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	switch p := v.(type) {
	case []byte:
		return append([]byte(nil), p...), nil
	case json.RawMessage:
		return append([]byte(nil), p...), nil
	default:
		return json.Marshal(p)
	}
}

func ptrTimestamp(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t.UTC())
}

func ptrInt32(v int) *int32 {
	x := int32(v) //nolint:gosec // G115: caller bounds checked or API-sized
	return &x
}
