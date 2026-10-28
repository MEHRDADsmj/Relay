package grpc

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	relayv1 "github.com/MEHRDADsmj/relay/gen/relay/v1"
	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

func jobToProto(j domain.Job) *relayv1.Job {
	out := &relayv1.Job{
		Id:              j.ID.String(),
		Queue:           j.Queue,
		Type:            j.Type,
		Payload:         append([]byte(nil), j.Payload...),
		Status:          statusToProto(j.Status),
		Priority:        int32(j.Priority), //nolint:gosec // G115: domain-bounded -100..100
		RunAt:           timestamppb.New(j.RunAt.UTC()),
		Attempts:        int32(j.Attempts),    //nolint:gosec // G115: non-negative domain field
		MaxAttempts:     int32(j.MaxAttempts), //nolint:gosec // G115: domain-bounded 1..100
		LeaseOwner:      j.LeaseOwner,
		IdempotencyKey:  j.IdempotencyKey,
		LastError:       j.LastError,
		CancelRequested: j.CancelRequested,
		CreatedAt:       timestamppb.New(j.CreatedAt.UTC()),
		UpdatedAt:       timestamppb.New(j.UpdatedAt.UTC()),
	}
	if j.LeaseExpiresAt != nil {
		out.LeaseExpiresAt = timestamppb.New(j.LeaseExpiresAt.UTC())
	}
	if j.CancelledAt != nil {
		out.CancelledAt = timestamppb.New(j.CancelledAt.UTC())
	}
	if j.FinishedAt != nil {
		out.FinishedAt = timestamppb.New(j.FinishedAt.UTC())
	}
	if len(j.TraceContext) > 0 {
		out.TraceContext = make(map[string]string, len(j.TraceContext))
		for k, v := range j.TraceContext {
			out.TraceContext[k] = v
		}
	}
	return out
}

func jobsToProto(jobs []domain.Job) []*relayv1.Job {
	out := make([]*relayv1.Job, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, jobToProto(j))
	}
	return out
}

func attemptToProto(a domain.Attempt) *relayv1.Attempt {
	out := &relayv1.Attempt{
		JobId:     a.JobID.String(),
		Number:    int32(a.Number), //nolint:gosec // G115: attempt number is small
		WorkerId:  a.WorkerID,
		StartedAt: timestamppb.New(a.StartedAt.UTC()),
		Outcome:   attemptOutcomeToProto(a.Outcome),
		Error:     a.Error,
	}
	if a.FinishedAt != nil {
		out.FinishedAt = timestamppb.New(a.FinishedAt.UTC())
	}
	return out
}

func attemptsToProto(attempts []domain.Attempt) []*relayv1.Attempt {
	out := make([]*relayv1.Attempt, 0, len(attempts))
	for _, a := range attempts {
		out = append(out, attemptToProto(a))
	}
	return out
}

func queueToProto(q domain.Queue) *relayv1.Queue {
	return &relayv1.Queue{
		Name:      q.Name,
		Paused:    q.Paused,
		CreatedAt: timestamppb.New(q.CreatedAt.UTC()),
		UpdatedAt: timestamppb.New(q.UpdatedAt.UTC()),
	}
}

func queuesToProto(queues []domain.Queue) []*relayv1.Queue {
	out := make([]*relayv1.Queue, 0, len(queues))
	for _, q := range queues {
		out = append(out, queueToProto(q))
	}
	return out
}

func queueStatsToProto(s domain.QueueStats) *relayv1.QueueStats {
	return &relayv1.QueueStats{
		Queue:                   s.Queue,
		Paused:                  s.Paused,
		Pending:                 s.Pending,
		Scheduled:               s.Scheduled,
		Running:                 s.Running,
		Succeeded:               s.Succeeded,
		Dead:                    s.Dead,
		Cancelled:               s.Cancelled,
		OldestPendingAgeSeconds: int64(s.OldestPendingAge / time.Second),
	}
}

func scheduleToProto(s domain.Schedule) *relayv1.Schedule {
	out := &relayv1.Schedule{
		Id:          s.ID.String(),
		Name:        s.Name,
		Cron:        s.Cron,
		Timezone:    s.Timezone,
		Queue:       s.Queue,
		Type:        s.JobType,
		Payload:     append([]byte(nil), s.Payload...),
		Priority:    int32(s.Priority),    //nolint:gosec // G115: domain-bounded -100..100
		MaxAttempts: int32(s.MaxAttempts), //nolint:gosec // G115: domain-bounded 1..100
		Enabled:     s.Enabled,
		NextRunAt:   timestamppb.New(s.NextRunAt.UTC()),
		CreatedAt:   timestamppb.New(s.CreatedAt.UTC()),
		UpdatedAt:   timestamppb.New(s.UpdatedAt.UTC()),
	}
	if s.LastRunAt != nil {
		out.LastRunAt = timestamppb.New(s.LastRunAt.UTC())
	}
	if s.LastJobID != nil {
		out.LastJobId = s.LastJobID.String()
	}
	return out
}

func schedulesToProto(items []domain.Schedule) []*relayv1.Schedule {
	out := make([]*relayv1.Schedule, 0, len(items))
	for _, s := range items {
		out = append(out, scheduleToProto(s))
	}
	return out
}

func apiKeyToProto(k domain.APIKey) *relayv1.APIKey {
	out := &relayv1.APIKey{
		Id:        k.ID,
		Name:      k.Name,
		Prefix:    k.Prefix,
		Scopes:    scopesToProto(k.Scopes),
		CreatedAt: timestamppb.New(k.CreatedAt.UTC()),
	}
	if k.LastUsedAt != nil {
		out.LastUsedAt = timestamppb.New(k.LastUsedAt.UTC())
	}
	if k.ExpiresAt != nil {
		out.ExpiresAt = timestamppb.New(k.ExpiresAt.UTC())
	}
	if k.RevokedAt != nil {
		out.RevokedAt = timestamppb.New(k.RevokedAt.UTC())
	}
	return out
}

func apiKeysToProto(keys []domain.APIKey) []*relayv1.APIKey {
	out := make([]*relayv1.APIKey, 0, len(keys))
	for _, k := range keys {
		out = append(out, apiKeyToProto(k))
	}
	return out
}

func scopesToProto(scopes []domain.Scope) []relayv1.Scope {
	out := make([]relayv1.Scope, 0, len(scopes))
	for _, s := range scopes {
		out = append(out, scopeToProto(s))
	}
	return out
}

func scopesFromProto(scopes []relayv1.Scope) []domain.Scope {
	out := make([]domain.Scope, 0, len(scopes))
	for _, s := range scopes {
		if d, ok := scopeFromProto(s); ok {
			out = append(out, d)
		}
	}
	return out
}

func scopeToProto(s domain.Scope) relayv1.Scope {
	switch s {
	case domain.ScopeProducer:
		return relayv1.Scope_SCOPE_PRODUCER
	case domain.ScopeWorker:
		return relayv1.Scope_SCOPE_WORKER
	case domain.ScopeAdmin:
		return relayv1.Scope_SCOPE_ADMIN
	case domain.ScopeDashboardRead:
		return relayv1.Scope_SCOPE_DASHBOARD_READ
	default:
		return relayv1.Scope_SCOPE_UNSPECIFIED
	}
}

func scopeFromProto(s relayv1.Scope) (domain.Scope, bool) {
	switch s {
	case relayv1.Scope_SCOPE_PRODUCER:
		return domain.ScopeProducer, true
	case relayv1.Scope_SCOPE_WORKER:
		return domain.ScopeWorker, true
	case relayv1.Scope_SCOPE_ADMIN:
		return domain.ScopeAdmin, true
	case relayv1.Scope_SCOPE_DASHBOARD_READ:
		return domain.ScopeDashboardRead, true
	default:
		return "", false
	}
}

func statusToProto(s domain.Status) relayv1.JobStatus {
	switch s {
	case domain.StatusPending:
		return relayv1.JobStatus_JOB_STATUS_PENDING
	case domain.StatusRunning:
		return relayv1.JobStatus_JOB_STATUS_RUNNING
	case domain.StatusSucceeded:
		return relayv1.JobStatus_JOB_STATUS_SUCCEEDED
	case domain.StatusDead:
		return relayv1.JobStatus_JOB_STATUS_DEAD
	case domain.StatusCancelled:
		return relayv1.JobStatus_JOB_STATUS_CANCELLED
	default:
		return relayv1.JobStatus_JOB_STATUS_UNSPECIFIED
	}
}

func statusFromProto(s relayv1.JobStatus) (domain.Status, error) {
	switch s {
	case relayv1.JobStatus_JOB_STATUS_PENDING:
		return domain.StatusPending, nil
	case relayv1.JobStatus_JOB_STATUS_RUNNING:
		return domain.StatusRunning, nil
	case relayv1.JobStatus_JOB_STATUS_SUCCEEDED:
		return domain.StatusSucceeded, nil
	case relayv1.JobStatus_JOB_STATUS_DEAD:
		return domain.StatusDead, nil
	case relayv1.JobStatus_JOB_STATUS_CANCELLED:
		return domain.StatusCancelled, nil
	default:
		return "", &domain.ValidationError{Field: "status", Message: "required"}
	}
}

func attemptOutcomeToProto(o domain.AttemptOutcome) relayv1.AttemptOutcome {
	switch o {
	case domain.AttemptRunning:
		return relayv1.AttemptOutcome_ATTEMPT_OUTCOME_RUNNING
	case domain.AttemptSucceeded:
		return relayv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED
	case domain.AttemptFailed:
		return relayv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED
	case domain.AttemptExpired:
		return relayv1.AttemptOutcome_ATTEMPT_OUTCOME_EXPIRED
	case domain.AttemptReleased:
		return relayv1.AttemptOutcome_ATTEMPT_OUTCOME_RELEASED
	case domain.AttemptCancelled:
		return relayv1.AttemptOutcome_ATTEMPT_OUTCOME_CANCELLED
	default:
		return relayv1.AttemptOutcome_ATTEMPT_OUTCOME_UNSPECIFIED
	}
}

func enqueueCommand(req *relayv1.EnqueueRequest) app.EnqueueCommand {
	cmd := app.EnqueueCommand{
		Queue:          req.GetQueue(),
		Type:           req.GetType(),
		Payload:        req.GetPayload(),
		IdempotencyKey: req.GetIdempotencyKey(),
	}
	if req.Priority != nil {
		v := int(req.GetPriority())
		cmd.Priority = &v
	}
	if req.GetRunAt() != nil {
		t := req.GetRunAt().AsTime()
		cmd.RunAt = &t
	}
	if req.MaxAttempts != nil {
		v := int(req.GetMaxAttempts())
		cmd.MaxAttempts = &v
	}
	return cmd
}

func leaseCommand(req *relayv1.LeaseRequest) app.LeaseCommand {
	cmd := app.LeaseCommand{
		Queue:    req.GetQueue(),
		WorkerID: req.GetWorkerId(),
	}
	if req.MaxJobs != nil {
		cmd.MaxJobs = int(req.GetMaxJobs())
	}
	if req.LeaseSeconds != nil {
		cmd.LeaseDuration = time.Duration(req.GetLeaseSeconds()) * time.Second
	}
	if req.WaitSeconds != nil {
		cmd.Wait = time.Duration(req.GetWaitSeconds()) * time.Second
	}
	return cmd
}

func createScheduleCommand(req *relayv1.CreateScheduleRequest) app.CreateScheduleCommand {
	cmd := app.CreateScheduleCommand{
		Name:     req.GetName(),
		Cron:     req.GetCron(),
		Timezone: req.GetTimezone(),
		Queue:    req.GetQueue(),
		JobType:  req.GetType(),
		Payload:  req.GetPayload(),
	}
	if req.Priority != nil {
		v := int(req.GetPriority())
		cmd.Priority = &v
	}
	if req.MaxAttempts != nil {
		v := int(req.GetMaxAttempts())
		cmd.MaxAttempts = &v
	}
	if req.Enabled != nil {
		v := req.GetEnabled()
		cmd.Enabled = &v
	}
	return cmd
}

func updateScheduleCommand(req *relayv1.UpdateScheduleRequest, id domain.ScheduleID, expected time.Time) app.UpdateScheduleCommand {
	cmd := app.UpdateScheduleCommand{
		ID:                id,
		ExpectedUpdatedAt: expected,
		SetPayload:        req.GetSetPayload(),
	}
	if req.GetSetPayload() {
		cmd.Payload = req.GetPayload()
	}
	if req.Cron != nil {
		v := req.GetCron()
		cmd.Cron = &v
	}
	if req.Timezone != nil {
		v := req.GetTimezone()
		cmd.Timezone = &v
	}
	if req.Queue != nil {
		v := req.GetQueue()
		cmd.Queue = &v
	}
	if req.Type != nil {
		v := req.GetType()
		cmd.JobType = &v
	}
	if req.Priority != nil {
		v := int(req.GetPriority())
		cmd.Priority = &v
	}
	if req.MaxAttempts != nil {
		v := int(req.GetMaxAttempts())
		cmd.MaxAttempts = &v
	}
	if req.Enabled != nil {
		v := req.GetEnabled()
		cmd.Enabled = &v
	}
	return cmd
}

func createAPIKeyCommand(req *relayv1.CreateAPIKeyRequest) app.CreateAPIKeyCommand {
	cmd := app.CreateAPIKeyCommand{
		Name:   req.GetName(),
		Scopes: scopesFromProto(req.GetScopes()),
	}
	if req.GetExpiresAt() != nil {
		t := req.GetExpiresAt().AsTime()
		cmd.ExpiresAt = &t
	}
	return cmd
}
