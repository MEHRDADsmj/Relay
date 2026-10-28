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
