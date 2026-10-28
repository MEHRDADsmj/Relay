package http

import (
	"context"
	"log/slog"

	"github.com/MEHRDADsmj/relay/internal/adapter/http/oapi"
	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

// Handler implements the generated strict OpenAPI server against JobService.
type Handler struct {
	svc *app.JobService
	log *slog.Logger
}

// NewHandler returns a strict-server implementation.
func NewHandler(svc *app.JobService, log *slog.Logger) *Handler {
	if log == nil {
		log = slog.Default()
	}
	return &Handler{svc: svc, log: log}
}

var _ oapi.StrictServerInterface = (*Handler)(nil)

// GetHealthz implements liveness.
func (h *Handler) GetHealthz(context.Context, oapi.GetHealthzRequestObject) (oapi.GetHealthzResponseObject, error) {
	return oapi.GetHealthz200JSONResponse{Status: "ok"}, nil
}

// GetReadyz implements readiness via JobService.Ready.
func (h *Handler) GetReadyz(ctx context.Context, _ oapi.GetReadyzRequestObject) (oapi.GetReadyzResponseObject, error) {
	if err := h.svc.Ready(ctx); err != nil {
		return oapi.GetReadyz503ApplicationProblemPlusJSONResponse(readinessProblem()), nil
	}
	return oapi.GetReadyz200JSONResponse{Status: "ok"}, nil
}

// EnqueueJob creates a job or returns an idempotent replay.
func (h *Handler) EnqueueJob(ctx context.Context, request oapi.EnqueueJobRequestObject) (oapi.EnqueueJobResponseObject, error) {
	if request.Body == nil {
		return nil, &domain.ValidationError{Field: "body", Message: "required"}
	}
	job, created, err := h.svc.Enqueue(ctx, enqueueCommand(*request.Body))
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
	job, err := h.svc.Get(ctx, domain.JobID(request.Id))
	if err != nil {
		return nil, err
	}
	return oapi.GetJob200JSONResponse(jobToAPI(job)), nil
}

// LeaseJobs claims jobs from a queue.
func (h *Handler) LeaseJobs(ctx context.Context, request oapi.LeaseJobsRequestObject) (oapi.LeaseJobsResponseObject, error) {
	if request.Body == nil {
		return nil, &domain.ValidationError{Field: "body", Message: "required"}
	}
	jobs, err := h.svc.Lease(ctx, leaseCommand(request.Queue, *request.Body))
	if err != nil {
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
	job, err := h.svc.Heartbeat(ctx, heartbeatCommand(domain.JobID(request.Id), *request.Body))
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
	job, err := h.svc.Complete(ctx, completeCommand(domain.JobID(request.Id), *request.Body))
	if err != nil {
		return nil, err
	}
	return oapi.CompleteJob200JSONResponse(jobToAPI(job)), nil
}

// FailJob reports a failure.
func (h *Handler) FailJob(ctx context.Context, request oapi.FailJobRequestObject) (oapi.FailJobResponseObject, error) {
	if request.Body == nil {
		return nil, &domain.ValidationError{Field: "body", Message: "required"}
	}
	job, err := h.svc.Fail(ctx, failCommand(domain.JobID(request.Id), *request.Body))
	if err != nil {
		return nil, err
	}
	return oapi.FailJob200JSONResponse(jobToAPI(job)), nil
}
