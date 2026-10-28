package grpc

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	relayv1 "github.com/MEHRDADsmj/relay/gen/relay/v1"
	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

type jobServer struct {
	relayv1.UnimplementedJobServiceServer
	jobs *app.JobService
}

func (s *jobServer) Enqueue(ctx context.Context, req *relayv1.EnqueueRequest) (*relayv1.EnqueueResponse, error) {
	job, created, err := s.jobs.Enqueue(ctx, enqueueCommand(req))
	if err != nil {
		return nil, mapError(err)
	}
	return &relayv1.EnqueueResponse{Job: jobToProto(job), Created: created}, nil
}

func (s *jobServer) GetJob(ctx context.Context, req *relayv1.GetJobRequest) (*relayv1.GetJobResponse, error) {
	id, err := parseJobID(req.GetId())
	if err != nil {
		return nil, mapError(err)
	}
	job, err := s.jobs.Get(ctx, id)
	if err != nil {
		return nil, mapError(err)
	}
	return &relayv1.GetJobResponse{Job: jobToProto(job)}, nil
}

func (s *jobServer) Lease(ctx context.Context, req *relayv1.LeaseRequest) (*relayv1.LeaseResponse, error) {
	jobs, err := s.jobs.LeaseWait(ctx, leaseCommand(req))
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return &relayv1.LeaseResponse{Jobs: []*relayv1.Job{}}, nil
		}
		return nil, mapError(err)
	}
	if jobs == nil {
		jobs = []domain.Job{}
	}
	return &relayv1.LeaseResponse{Jobs: jobsToProto(jobs)}, nil
}

func (s *jobServer) Heartbeat(ctx context.Context, req *relayv1.HeartbeatRequest) (*relayv1.HeartbeatResponse, error) {
	id, err := parseJobID(req.GetId())
	if err != nil {
		return nil, mapError(err)
	}
	cmd := app.HeartbeatCommand{JobID: id, WorkerID: req.GetWorkerId()}
	if req.LeaseSeconds != nil {
		cmd.LeaseDuration = durationSeconds(req.GetLeaseSeconds())
	}
	job, err := s.jobs.Heartbeat(ctx, cmd)
	if err != nil {
		return nil, mapError(err)
	}
	return &relayv1.HeartbeatResponse{Job: jobToProto(job)}, nil
}

func (s *jobServer) Complete(ctx context.Context, req *relayv1.CompleteRequest) (*relayv1.CompleteResponse, error) {
	id, err := parseJobID(req.GetId())
	if err != nil {
		return nil, mapError(err)
	}
	job, err := s.jobs.Complete(ctx, app.CompleteCommand{JobID: id, WorkerID: req.GetWorkerId()})
	if err != nil {
		return nil, mapError(err)
	}
	return &relayv1.CompleteResponse{Job: jobToProto(job)}, nil
}

func (s *jobServer) Fail(ctx context.Context, req *relayv1.FailRequest) (*relayv1.FailResponse, error) {
	id, err := parseJobID(req.GetId())
	if err != nil {
		return nil, mapError(err)
	}
	job, err := s.jobs.Fail(ctx, app.FailCommand{
		JobID:     id,
		WorkerID:  req.GetWorkerId(),
		Error:     req.GetError(),
		Permanent: req.GetPermanent(),
	})
	if err != nil {
		return nil, mapError(err)
	}
	return &relayv1.FailResponse{Job: jobToProto(job)}, nil
}

func (s *jobServer) Release(ctx context.Context, req *relayv1.ReleaseRequest) (*relayv1.ReleaseResponse, error) {
	id, err := parseJobID(req.GetId())
	if err != nil {
		return nil, mapError(err)
	}
	job, err := s.jobs.Release(ctx, app.ReleaseCommand{JobID: id, WorkerID: req.GetWorkerId()})
	if err != nil {
		return nil, mapError(err)
	}
	return &relayv1.ReleaseResponse{Job: jobToProto(job)}, nil
}

func (s *jobServer) Cancel(ctx context.Context, req *relayv1.CancelRequest) (*relayv1.CancelResponse, error) {
	id, err := parseJobID(req.GetId())
	if err != nil {
		return nil, mapError(err)
	}
	job, err := s.jobs.Cancel(ctx, id)
	if err != nil {
		return nil, mapError(err)
	}
	return &relayv1.CancelResponse{Job: jobToProto(job)}, nil
}

func (s *jobServer) Retry(ctx context.Context, req *relayv1.RetryRequest) (*relayv1.RetryResponse, error) {
	id, err := parseJobID(req.GetId())
	if err != nil {
		return nil, mapError(err)
	}
	job, err := s.jobs.Retry(ctx, id)
	if err != nil {
		return nil, mapError(err)
	}
	return &relayv1.RetryResponse{Job: jobToProto(job)}, nil
}

func (s *jobServer) ListAttempts(ctx context.Context, req *relayv1.ListAttemptsRequest) (*relayv1.ListAttemptsResponse, error) {
	id, err := parseJobID(req.GetId())
	if err != nil {
		return nil, mapError(err)
	}
	attempts, err := s.jobs.ListAttempts(ctx, id)
	if err != nil {
		return nil, mapError(err)
	}
	if attempts == nil {
		attempts = []domain.Attempt{}
	}
	return &relayv1.ListAttemptsResponse{Attempts: attemptsToProto(attempts)}, nil
}

func parseJobID(raw string) (domain.JobID, error) {
	id, err := uuid.Parse(raw)
	if err != nil {
		return domain.JobID{}, &domain.ValidationError{Field: "id", Message: "must be a UUID"}
	}
	return id, nil
}

func parseScheduleID(raw string) (domain.ScheduleID, error) {
	id, err := uuid.Parse(raw)
	if err != nil {
		return domain.ScheduleID{}, &domain.ValidationError{Field: "id", Message: "must be a UUID"}
	}
	return id, nil
}

func durationSeconds(sec int32) time.Duration {
	return time.Duration(sec) * time.Second
}
