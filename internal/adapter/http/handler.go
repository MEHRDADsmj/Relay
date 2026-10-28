package http

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/MEHRDADsmj/relay/internal/adapter/http/oapi"
	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

// Handler implements the generated strict OpenAPI server against app services.
type Handler struct {
	jobs      *app.JobService
	queues    *app.QueueService
	schedules *app.ScheduleService
	apiKeys   *app.APIKeyService
	log       *slog.Logger
}

// NewHandler returns a strict-server implementation.
func NewHandler(
	jobs *app.JobService,
	queues *app.QueueService,
	schedules *app.ScheduleService,
	apiKeys *app.APIKeyService,
	log *slog.Logger,
) *Handler {
	if log == nil {
		log = slog.Default()
	}
	return &Handler{jobs: jobs, queues: queues, schedules: schedules, apiKeys: apiKeys, log: log}
}

var _ oapi.StrictServerInterface = (*Handler)(nil)

// GetHealthz implements liveness.
func (h *Handler) GetHealthz(context.Context, oapi.GetHealthzRequestObject) (oapi.GetHealthzResponseObject, error) {
	return oapi.GetHealthz200JSONResponse{Status: "ok"}, nil
}

// GetReadyz implements readiness via JobService.Ready.
func (h *Handler) GetReadyz(ctx context.Context, _ oapi.GetReadyzRequestObject) (oapi.GetReadyzResponseObject, error) {
	if err := h.jobs.Ready(ctx); err != nil {
		return oapi.GetReadyz503ApplicationProblemPlusJSONResponse(readinessProblem()), nil
	}
	return oapi.GetReadyz200JSONResponse{Status: "ok"}, nil
}

// EnqueueJob creates a job or returns an idempotent replay.
func (h *Handler) EnqueueJob(ctx context.Context, request oapi.EnqueueJobRequestObject) (oapi.EnqueueJobResponseObject, error) {
	if request.Body == nil {
		return nil, &domain.ValidationError{Field: "body", Message: "required"}
	}
	job, created, err := h.jobs.Enqueue(ctx, enqueueCommand(*request.Body))
	if err != nil {
		return nil, err
	}
	apiJob := jobToAPI(job)
	if !created {
		return oapi.EnqueueJob200JSONResponse{
			Body: apiJob,
			Headers: oapi.EnqueueJob200ResponseHeaders{
				IdempotentReplayed: "true",
			},
		}, nil
	}
	return oapi.EnqueueJob201JSONResponse(apiJob), nil
}

// GetJob returns a job by ID.
func (h *Handler) GetJob(ctx context.Context, request oapi.GetJobRequestObject) (oapi.GetJobResponseObject, error) {
	job, err := h.jobs.Get(ctx, domain.JobID(request.Id))
	if err != nil {
		return nil, err
	}
	return oapi.GetJob200JSONResponse(jobToAPI(job)), nil
}

// LeaseJobs claims jobs from a queue, optionally long-polling via wait_seconds.
func (h *Handler) LeaseJobs(ctx context.Context, request oapi.LeaseJobsRequestObject) (oapi.LeaseJobsResponseObject, error) {
	if request.Body == nil {
		return nil, &domain.ValidationError{Field: "body", Message: "required"}
	}
	jobs, err := h.jobs.LeaseWait(ctx, leaseCommand(request.Queue, *request.Body))
	if err != nil {
		// Client disconnect during long-poll: return empty leases, not 500.
		if errors.Is(err, context.Canceled) {
			return oapi.LeaseJobs200JSONResponse{Jobs: []oapi.Job{}}, nil
		}
		return nil, err
	}
	if jobs == nil {
		jobs = []domain.Job{}
	}
	return oapi.LeaseJobs200JSONResponse{Jobs: jobsToAPI(jobs)}, nil
}

// HeartbeatJob extends a lease.
func (h *Handler) HeartbeatJob(ctx context.Context, request oapi.HeartbeatJobRequestObject) (oapi.HeartbeatJobResponseObject, error) {
	if request.Body == nil {
		return nil, &domain.ValidationError{Field: "body", Message: "required"}
	}
	job, err := h.jobs.Heartbeat(ctx, heartbeatCommand(domain.JobID(request.Id), *request.Body))
	if err != nil {
		return nil, err
	}
	return oapi.HeartbeatJob200JSONResponse(jobToAPI(job)), nil
}

// CompleteJob marks a job succeeded.
func (h *Handler) CompleteJob(ctx context.Context, request oapi.CompleteJobRequestObject) (oapi.CompleteJobResponseObject, error) {
	if request.Body == nil {
		return nil, &domain.ValidationError{Field: "body", Message: "required"}
	}
	job, err := h.jobs.Complete(ctx, completeCommand(domain.JobID(request.Id), *request.Body))
	if err != nil {
		return nil, err
	}
	return oapi.CompleteJob200JSONResponse(jobToAPI(job)), nil
}

// FailJob reports a failure (optional permanent → dead).
func (h *Handler) FailJob(ctx context.Context, request oapi.FailJobRequestObject) (oapi.FailJobResponseObject, error) {
	if request.Body == nil {
		return nil, &domain.ValidationError{Field: "body", Message: "required"}
	}
	job, err := h.jobs.Fail(ctx, failCommand(domain.JobID(request.Id), *request.Body))
	if err != nil {
		return nil, err
	}
	return oapi.FailJob200JSONResponse(jobToAPI(job)), nil
}

// ReleaseJob voluntarily returns a leased job to pending.
func (h *Handler) ReleaseJob(ctx context.Context, request oapi.ReleaseJobRequestObject) (oapi.ReleaseJobResponseObject, error) {
	if request.Body == nil {
		return nil, &domain.ValidationError{Field: "body", Message: "required"}
	}
	job, err := h.jobs.Release(ctx, releaseCommand(domain.JobID(request.Id), *request.Body))
	if err != nil {
		return nil, err
	}
	return oapi.ReleaseJob200JSONResponse(jobToAPI(job)), nil
}

// CancelJob cancels a pending job or requests cancel on a running job.
func (h *Handler) CancelJob(ctx context.Context, request oapi.CancelJobRequestObject) (oapi.CancelJobResponseObject, error) {
	job, err := h.jobs.Cancel(ctx, domain.JobID(request.Id))
	if err != nil {
		return nil, err
	}
	return oapi.CancelJob200JSONResponse(jobToAPI(job)), nil
}

// RetryJob requeues a dead or cancelled job.
func (h *Handler) RetryJob(ctx context.Context, request oapi.RetryJobRequestObject) (oapi.RetryJobResponseObject, error) {
	job, err := h.jobs.Retry(ctx, domain.JobID(request.Id))
	if err != nil {
		return nil, err
	}
	return oapi.RetryJob200JSONResponse(jobToAPI(job)), nil
}

// ListJobAttempts returns attempt history for a job.
func (h *Handler) ListJobAttempts(ctx context.Context, request oapi.ListJobAttemptsRequestObject) (oapi.ListJobAttemptsResponseObject, error) {
	attempts, err := h.jobs.ListAttempts(ctx, domain.JobID(request.Id))
	if err != nil {
		return nil, err
	}
	if attempts == nil {
		attempts = []domain.Attempt{}
	}
	return oapi.ListJobAttempts200JSONResponse{Attempts: attemptsToAPI(attempts)}, nil
}

// ListQueues returns all registered queues.
func (h *Handler) ListQueues(ctx context.Context, _ oapi.ListQueuesRequestObject) (oapi.ListQueuesResponseObject, error) {
	queues, err := h.queues.ListQueues(ctx)
	if err != nil {
		return nil, err
	}
	if queues == nil {
		queues = []domain.Queue{}
	}
	return oapi.ListQueues200JSONResponse{Queues: queuesToAPI(queues)}, nil
}

// GetQueueStats returns aggregate counts for a queue.
func (h *Handler) GetQueueStats(ctx context.Context, request oapi.GetQueueStatsRequestObject) (oapi.GetQueueStatsResponseObject, error) {
	stats, err := h.queues.Stats(ctx, request.Queue)
	if err != nil {
		return nil, err
	}
	return oapi.GetQueueStats200JSONResponse(queueStatsToAPI(stats)), nil
}

// PauseQueue pauses a queue.
func (h *Handler) PauseQueue(ctx context.Context, request oapi.PauseQueueRequestObject) (oapi.PauseQueueResponseObject, error) {
	q, err := h.queues.Pause(ctx, request.Queue)
	if err != nil {
		return nil, err
	}
	return oapi.PauseQueue200JSONResponse(queueToAPI(q)), nil
}

// ResumeQueue resumes a paused queue.
func (h *Handler) ResumeQueue(ctx context.Context, request oapi.ResumeQueueRequestObject) (oapi.ResumeQueueResponseObject, error) {
	q, err := h.queues.Resume(ctx, request.Queue)
	if err != nil {
		return nil, err
	}
	return oapi.ResumeQueue200JSONResponse(queueToAPI(q)), nil
}

// ListQueueJobs returns a keyset page of jobs filtered by status.
func (h *Handler) ListQueueJobs(ctx context.Context, request oapi.ListQueueJobsRequestObject) (oapi.ListQueueJobsResponseObject, error) {
	cmd := app.ListByStatusCommand{
		Queue:  request.Queue,
		Status: domain.Status(request.Params.Status),
	}
	if request.Params.Limit != nil {
		cmd.Page.Limit = *request.Params.Limit
	}
	if request.Params.Cursor != nil {
		cmd.Page.Cursor = *request.Params.Cursor
	}
	page, err := h.jobs.ListByStatus(ctx, cmd)
	if err != nil {
		return nil, err
	}
	jobs := page.Items
	if jobs == nil {
		jobs = []domain.Job{}
	}
	return oapi.ListQueueJobs200JSONResponse{
		Jobs:       jobsToAPI(jobs),
		NextCursor: page.NextCursor,
	}, nil
}

// RetryDeadJobs bulk-retries dead jobs in a queue.
func (h *Handler) RetryDeadJobs(ctx context.Context, request oapi.RetryDeadJobsRequestObject) (oapi.RetryDeadJobsResponseObject, error) {
	// Ensure the queue exists (RetryAllDead alone returns 0 for unknown queues).
	if _, err := h.queues.Stats(ctx, request.Queue); err != nil {
		return nil, err
	}
	cmd := app.RetryAllDeadCommand{Queue: request.Queue}
	if request.Body != nil && request.Body.Limit != nil {
		cmd.Limit = *request.Body.Limit
	}
	retried, err := h.queues.RetryAllDead(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return oapi.RetryDeadJobs200JSONResponse{Retried: retried}, nil
}

// CreateSchedule creates a cron schedule.
func (h *Handler) CreateSchedule(ctx context.Context, request oapi.CreateScheduleRequestObject) (oapi.CreateScheduleResponseObject, error) {
	if request.Body == nil {
		return nil, &domain.ValidationError{Field: "body", Message: "required"}
	}
	sched, err := h.schedules.Create(ctx, createScheduleCommand(*request.Body))
	if err != nil {
		return nil, err
	}
	return oapi.CreateSchedule201JSONResponse(scheduleToAPI(sched)), nil
}

// ListSchedules returns a keyset page of schedules.
func (h *Handler) ListSchedules(ctx context.Context, request oapi.ListSchedulesRequestObject) (oapi.ListSchedulesResponseObject, error) {
	pageReq := domain.PageRequest{}
	if request.Params.Limit != nil {
		pageReq.Limit = *request.Params.Limit
	}
	if request.Params.Cursor != nil {
		pageReq.Cursor = *request.Params.Cursor
	}
	page, err := h.schedules.List(ctx, pageReq)
	if err != nil {
		return nil, err
	}
	items := page.Items
	if items == nil {
		items = []domain.Schedule{}
	}
	return oapi.ListSchedules200JSONResponse{
		Schedules:  schedulesToAPI(items),
		NextCursor: page.NextCursor,
	}, nil
}

// GetSchedule returns a schedule by UUID or unique name.
func (h *Handler) GetSchedule(ctx context.Context, request oapi.GetScheduleRequestObject) (oapi.GetScheduleResponseObject, error) {
	key := strings.TrimSpace(request.IdOrName)
	var sched domain.Schedule
	var err error
	if id, parseErr := uuid.Parse(key); parseErr == nil {
		sched, err = h.schedules.Get(ctx, id)
	} else {
		sched, err = h.schedules.GetByName(ctx, key)
	}
	if err != nil {
		return nil, err
	}
	return oapi.GetSchedule200JSONResponse(scheduleToAPI(sched)), nil
}

// PatchSchedule applies a partial update with If-Match optimistic concurrency.
func (h *Handler) PatchSchedule(ctx context.Context, request oapi.PatchScheduleRequestObject) (oapi.PatchScheduleResponseObject, error) {
	if request.Params.IfMatch == nil || strings.TrimSpace(*request.Params.IfMatch) == "" {
		return oapi.PatchSchedule428ApplicationProblemPlusJSONResponse{
			PreconditionRequiredApplicationProblemPlusJSONResponse: oapi.PreconditionRequiredApplicationProblemPlusJSONResponse(preconditionRequiredProblem()),
		}, nil
	}
	expected, err := parseIfMatch(*request.Params.IfMatch)
	if err != nil {
		return nil, &domain.ValidationError{Field: "If-Match", Message: "must be RFC3339Nano timestamp"}
	}
	if request.Body == nil {
		return nil, &domain.ValidationError{Field: "body", Message: "required"}
	}
	sched, err := h.schedules.Update(ctx, patchScheduleCommand(domain.ScheduleID(request.Id), expected, *request.Body))
	if err != nil {
		if errors.Is(err, domain.ErrConflict) {
			return oapi.PatchSchedule412ApplicationProblemPlusJSONResponse{
				PreconditionFailedApplicationProblemPlusJSONResponse: oapi.PreconditionFailedApplicationProblemPlusJSONResponse(preconditionFailedProblem()),
			}, nil
		}
		return nil, err
	}
	return oapi.PatchSchedule200JSONResponse(scheduleToAPI(sched)), nil
}

// DeleteSchedule removes a schedule.
func (h *Handler) DeleteSchedule(ctx context.Context, request oapi.DeleteScheduleRequestObject) (oapi.DeleteScheduleResponseObject, error) {
	if err := h.schedules.Delete(ctx, domain.ScheduleID(request.Id)); err != nil {
		return nil, err
	}
	return oapi.DeleteSchedule204Response{}, nil
}

// EnableSchedule enables a schedule.
func (h *Handler) EnableSchedule(ctx context.Context, request oapi.EnableScheduleRequestObject) (oapi.EnableScheduleResponseObject, error) {
	sched, err := h.schedules.Enable(ctx, domain.ScheduleID(request.Id))
	if err != nil {
		return nil, err
	}
	return oapi.EnableSchedule200JSONResponse(scheduleToAPI(sched)), nil
}

// DisableSchedule disables a schedule.
func (h *Handler) DisableSchedule(ctx context.Context, request oapi.DisableScheduleRequestObject) (oapi.DisableScheduleResponseObject, error) {
	sched, err := h.schedules.Disable(ctx, domain.ScheduleID(request.Id))
	if err != nil {
		return nil, err
	}
	return oapi.DisableSchedule200JSONResponse(scheduleToAPI(sched)), nil
}

// TriggerSchedule enqueues a job for the schedule immediately.
func (h *Handler) TriggerSchedule(ctx context.Context, request oapi.TriggerScheduleRequestObject) (oapi.TriggerScheduleResponseObject, error) {
	job, err := h.schedules.TriggerNow(ctx, domain.ScheduleID(request.Id))
	if err != nil {
		return nil, err
	}
	return oapi.TriggerSchedule200JSONResponse(jobToAPI(job)), nil
}

// CreateAPIKey creates a key and returns the secret once.
func (h *Handler) CreateAPIKey(ctx context.Context, request oapi.CreateAPIKeyRequestObject) (oapi.CreateAPIKeyResponseObject, error) {
	if request.Body == nil {
		return nil, &domain.ValidationError{Field: "body", Message: "required"}
	}
	created, err := h.apiKeys.Create(ctx, createAPIKeyCommand(*request.Body))
	if err != nil {
		return nil, err
	}
	return oapi.CreateAPIKey201JSONResponse(createdAPIKeyToAPI(created)), nil
}

// ListAPIKeys lists keys without secrets.
func (h *Handler) ListAPIKeys(ctx context.Context, _ oapi.ListAPIKeysRequestObject) (oapi.ListAPIKeysResponseObject, error) {
	keys, err := h.apiKeys.List(ctx)
	if err != nil {
		return nil, err
	}
	if keys == nil {
		keys = []domain.APIKey{}
	}
	return oapi.ListAPIKeys200JSONResponse{Keys: apiKeysToAPI(keys)}, nil
}

// RevokeAPIKey revokes a key by ID.
func (h *Handler) RevokeAPIKey(ctx context.Context, request oapi.RevokeAPIKeyRequestObject) (oapi.RevokeAPIKeyResponseObject, error) {
	key, err := h.apiKeys.Revoke(ctx, request.Id)
	if err != nil {
		return nil, err
	}
	return oapi.RevokeAPIKey200JSONResponse(apiKeyToAPI(key)), nil
}

func parseIfMatch(raw string) (time.Time, error) {
	s := strings.TrimSpace(raw)
	s = strings.Trim(s, `"`)
	return time.Parse(time.RFC3339Nano, s)
}
