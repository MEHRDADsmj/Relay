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
		Id:          openapi_types.UUID(j.ID),
		Queue:       j.Queue,
		Type:        j.Type,
		Payload:     json.RawMessage(j.Payload),
		Status:      oapi.JobStatus(j.Status),
		Priority:    j.Priority,
		RunAt:       j.RunAt.UTC(),
		Attempts:    j.Attempts,
		MaxAttempts: j.MaxAttempts,
		CreatedAt:   j.CreatedAt.UTC(),
		UpdatedAt:   j.UpdatedAt.UTC(),
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
	return out
}

func jobsToAPI(jobs []domain.Job) []oapi.Job {
	out := make([]oapi.Job, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, jobToAPI(j))
	}
	return out
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
	return app.FailCommand{JobID: id, WorkerID: body.WorkerId, Error: body.Error}
}
