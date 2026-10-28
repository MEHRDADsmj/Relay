package http

import (
	"encoding/json"
	"time"

	openapi_types "github.com/oapi-codegen/runtime/types"

	"github.com/MEHRDADsmj/relay/internal/adapter/http/oapi"
	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

func jobToAPI(j domain.Job) oapi.Job {
	out := oapi.Job{
		Id:              openapi_types.UUID(j.ID),
		Queue:           j.Queue,
		Type:            j.Type,
		Payload:         json.RawMessage(j.Payload),
		Status:          oapi.JobStatus(j.Status),
		Priority:        j.Priority,
		RunAt:           j.RunAt.UTC(),
		Attempts:        j.Attempts,
		MaxAttempts:     j.MaxAttempts,
		CancelRequested: j.CancelRequested,
		CreatedAt:       j.CreatedAt.UTC(),
		UpdatedAt:       j.UpdatedAt.UTC(),
	}
	if j.LeaseOwner != "" {
		out.LeaseOwner = &j.LeaseOwner
	}
	if j.LeaseExpiresAt != nil {
		t := j.LeaseExpiresAt.UTC()
		out.LeaseExpiresAt = &t
	}
	if j.IdempotencyKey != "" {
		out.IdempotencyKey = &j.IdempotencyKey
	}
	if j.LastError != "" {
		out.LastError = &j.LastError
	}
	if j.FinishedAt != nil {
		t := j.FinishedAt.UTC()
		out.FinishedAt = &t
	}
	if j.CancelledAt != nil {
		t := j.CancelledAt.UTC()
		out.CancelledAt = &t
	}
	if len(j.TraceContext) > 0 {
		tc := make(map[string]string, len(j.TraceContext))
		for k, v := range j.TraceContext {
			tc[k] = v
		}
		out.TraceContext = &tc
	}
	return out
}

func jobsToAPI(jobs []domain.Job) []oapi.Job {
	out := make([]oapi.Job, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, jobToAPI(j))
	}
	return out
}

func attemptToAPI(a domain.Attempt) oapi.Attempt {
	out := oapi.Attempt{
		JobId:     openapi_types.UUID(a.JobID),
		Number:    a.Number,
		WorkerId:  a.WorkerID,
		StartedAt: a.StartedAt.UTC(),
		Outcome:   oapi.AttemptOutcome(a.Outcome),
	}
	if a.FinishedAt != nil {
		t := a.FinishedAt.UTC()
		out.FinishedAt = &t
	}
	if a.Error != "" {
		out.Error = &a.Error
	}
	return out
}

func attemptsToAPI(attempts []domain.Attempt) []oapi.Attempt {
	out := make([]oapi.Attempt, 0, len(attempts))
	for _, a := range attempts {
		out = append(out, attemptToAPI(a))
	}
	return out
}

func queueToAPI(q domain.Queue) oapi.Queue {
	return oapi.Queue{
		Name:      q.Name,
		Paused:    q.Paused,
		CreatedAt: q.CreatedAt.UTC(),
		UpdatedAt: q.UpdatedAt.UTC(),
	}
}

func queuesToAPI(queues []domain.Queue) []oapi.Queue {
	out := make([]oapi.Queue, 0, len(queues))
	for _, q := range queues {
		out = append(out, queueToAPI(q))
	}
	return out
}

func queueStatsToAPI(s domain.QueueStats) oapi.QueueStats {
	return oapi.QueueStats{
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

func enqueueCommand(body oapi.EnqueueRequest) app.EnqueueCommand {
	cmd := app.EnqueueCommand{
		Queue:       body.Queue,
		Type:        body.Type,
		Priority:    body.Priority,
		RunAt:       body.RunAt,
		MaxAttempts: body.MaxAttempts,
	}
	if body.Payload != nil {
		cmd.Payload = []byte(*body.Payload)
	}
	if body.IdempotencyKey != nil {
		cmd.IdempotencyKey = *body.IdempotencyKey
	}
	return cmd
}

func leaseCommand(queue string, body oapi.LeaseRequest) app.LeaseCommand {
	cmd := app.LeaseCommand{
		Queue:    queue,
		WorkerID: body.WorkerId,
	}
	if body.MaxJobs != nil {
		cmd.MaxJobs = *body.MaxJobs
	}
	if body.LeaseSeconds != nil {
		cmd.LeaseDuration = time.Duration(*body.LeaseSeconds) * time.Second
	}
	if body.WaitSeconds != nil {
		cmd.Wait = time.Duration(*body.WaitSeconds) * time.Second
	}
	return cmd
}

func heartbeatCommand(id domain.JobID, body oapi.HeartbeatRequest) app.HeartbeatCommand {
	cmd := app.HeartbeatCommand{
		JobID:    id,
		WorkerID: body.WorkerId,
	}
	if body.LeaseSeconds != nil {
		cmd.LeaseDuration = time.Duration(*body.LeaseSeconds) * time.Second
	}
	return cmd
}

func completeCommand(id domain.JobID, body oapi.CompleteRequest) app.CompleteCommand {
	return app.CompleteCommand{JobID: id, WorkerID: body.WorkerId}
}

func failCommand(id domain.JobID, body oapi.FailRequest) app.FailCommand {
	cmd := app.FailCommand{JobID: id, WorkerID: body.WorkerId, Error: body.Error}
	if body.Permanent != nil {
		cmd.Permanent = *body.Permanent
	}
	return cmd
}

func releaseCommand(id domain.JobID, body oapi.ReleaseRequest) app.ReleaseCommand {
	return app.ReleaseCommand{JobID: id, WorkerID: body.WorkerId}
}

func scheduleToAPI(s domain.Schedule) oapi.Schedule {
	out := oapi.Schedule{
		Id:          openapi_types.UUID(s.ID),
		Name:        s.Name,
		Cron:        s.Cron,
		Timezone:    s.Timezone,
		Queue:       s.Queue,
		Type:        s.JobType,
		Payload:     json.RawMessage(s.Payload),
		Priority:    s.Priority,
		MaxAttempts: s.MaxAttempts,
		Enabled:     s.Enabled,
		NextRunAt:   s.NextRunAt.UTC(),
		CreatedAt:   s.CreatedAt.UTC(),
		UpdatedAt:   s.UpdatedAt.UTC(),
	}
	if s.LastRunAt != nil {
		t := s.LastRunAt.UTC()
		out.LastRunAt = &t
	}
	if s.LastJobID != nil {
		id := openapi_types.UUID(*s.LastJobID)
		out.LastJobId = &id
	}
	return out
}

func schedulesToAPI(items []domain.Schedule) []oapi.Schedule {
	out := make([]oapi.Schedule, 0, len(items))
	for _, s := range items {
		out = append(out, scheduleToAPI(s))
	}
	return out
}

func createScheduleCommand(body oapi.CreateScheduleRequest) app.CreateScheduleCommand {
	cmd := app.CreateScheduleCommand{
		Name:        body.Name,
		Cron:        body.Cron,
		Queue:       body.Queue,
		JobType:     body.Type,
		Priority:    body.Priority,
		MaxAttempts: body.MaxAttempts,
		Enabled:     body.Enabled,
	}
	if body.Timezone != nil {
		cmd.Timezone = *body.Timezone
	}
	if body.Payload != nil {
		cmd.Payload = []byte(*body.Payload)
	}
	return cmd
}

func patchScheduleCommand(id domain.ScheduleID, expected time.Time, body oapi.PatchScheduleRequest) app.UpdateScheduleCommand {
	cmd := app.UpdateScheduleCommand{
		ID:                id,
		ExpectedUpdatedAt: expected,
		Cron:              body.Cron,
		Timezone:          body.Timezone,
		Queue:             body.Queue,
		JobType:           body.Type,
		Priority:          body.Priority,
		MaxAttempts:       body.MaxAttempts,
		Enabled:           body.Enabled,
	}
	if body.Payload != nil {
		cmd.SetPayload = true
		cmd.Payload = []byte(*body.Payload)
	}
	return cmd
}

func apiKeyToAPI(k domain.APIKey) oapi.APIKey {
	out := oapi.APIKey{
		Id:        k.ID,
		Name:      k.Name,
		Prefix:    k.Prefix,
		Scopes:    scopesToAPI(k.Scopes),
		CreatedAt: k.CreatedAt.UTC(),
	}
	if k.LastUsedAt != nil {
		t := k.LastUsedAt.UTC()
		out.LastUsedAt = &t
	}
	if k.ExpiresAt != nil {
		t := k.ExpiresAt.UTC()
		out.ExpiresAt = &t
	}
	if k.RevokedAt != nil {
		t := k.RevokedAt.UTC()
		out.RevokedAt = &t
	}
	return out
}

func apiKeysToAPI(keys []domain.APIKey) []oapi.APIKey {
	out := make([]oapi.APIKey, 0, len(keys))
	for _, k := range keys {
		out = append(out, apiKeyToAPI(k))
	}
	return out
}

func createdAPIKeyToAPI(c app.CreatedAPIKey) oapi.CreatedAPIKey {
	return oapi.CreatedAPIKey{
		Key:    apiKeyToAPI(c.Key),
		Secret: c.Secret,
	}
}

func scopesToAPI(scopes []domain.Scope) []oapi.Scope {
	out := make([]oapi.Scope, 0, len(scopes))
	for _, s := range scopes {
		out = append(out, oapi.Scope(s))
	}
	return out
}

func scopesFromAPI(scopes []oapi.Scope) []domain.Scope {
	out := make([]domain.Scope, 0, len(scopes))
	for _, s := range scopes {
		out = append(out, domain.Scope(s))
	}
	return out
}

func createAPIKeyCommand(body oapi.CreateAPIKeyRequest) app.CreateAPIKeyCommand {
	return app.CreateAPIKeyCommand{
		Name:      body.Name,
		Scopes:    scopesFromAPI(body.Scopes),
		ExpiresAt: body.ExpiresAt,
	}
}
