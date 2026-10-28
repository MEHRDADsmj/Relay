package grpc

import (
	"context"

	relayv1 "github.com/MEHRDADsmj/relay/gen/relay/v1"
	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

type queueServer struct {
	relayv1.UnimplementedQueueServiceServer
	queues *app.QueueService
	jobs   *app.JobService
}

func (s *queueServer) ListQueues(ctx context.Context, _ *relayv1.ListQueuesRequest) (*relayv1.ListQueuesResponse, error) {
	queues, err := s.queues.ListQueues(ctx)
	if err != nil {
		return nil, mapError(err)
	}
	if queues == nil {
		queues = []domain.Queue{}
	}
	return &relayv1.ListQueuesResponse{Queues: queuesToProto(queues)}, nil
}

func (s *queueServer) GetQueueStats(ctx context.Context, req *relayv1.GetQueueStatsRequest) (*relayv1.GetQueueStatsResponse, error) {
	stats, err := s.queues.Stats(ctx, req.GetQueue())
	if err != nil {
		return nil, mapError(err)
	}
	return &relayv1.GetQueueStatsResponse{Stats: queueStatsToProto(stats)}, nil
}

func (s *queueServer) PauseQueue(ctx context.Context, req *relayv1.PauseQueueRequest) (*relayv1.PauseQueueResponse, error) {
	q, err := s.queues.Pause(ctx, req.GetQueue())
	if err != nil {
		return nil, mapError(err)
	}
	return &relayv1.PauseQueueResponse{Queue: queueToProto(q)}, nil
}

func (s *queueServer) ResumeQueue(ctx context.Context, req *relayv1.ResumeQueueRequest) (*relayv1.ResumeQueueResponse, error) {
	q, err := s.queues.Resume(ctx, req.GetQueue())
	if err != nil {
		return nil, mapError(err)
	}
	return &relayv1.ResumeQueueResponse{Queue: queueToProto(q)}, nil
}

func (s *queueServer) ListQueueJobs(ctx context.Context, req *relayv1.ListQueueJobsRequest) (*relayv1.ListQueueJobsResponse, error) {
	st, err := statusFromProto(req.GetStatus())
	if err != nil {
		return nil, mapError(err)
	}
	cmd := app.ListByStatusCommand{
		Queue:  req.GetQueue(),
		Status: st,
		Page:   domain.PageRequest{Cursor: req.GetCursor()},
	}
	if req.Limit != nil {
		cmd.Page.Limit = int(req.GetLimit())
	}
	page, err := s.jobs.ListByStatus(ctx, cmd)
	if err != nil {
		return nil, mapError(err)
	}
	jobs := page.Items
	if jobs == nil {
		jobs = []domain.Job{}
	}
	return &relayv1.ListQueueJobsResponse{
		Jobs:       jobsToProto(jobs),
		NextCursor: page.NextCursor,
	}, nil
}

func (s *queueServer) RetryDeadJobs(ctx context.Context, req *relayv1.RetryDeadJobsRequest) (*relayv1.RetryDeadJobsResponse, error) {
	if _, err := s.queues.Stats(ctx, req.GetQueue()); err != nil {
		return nil, mapError(err)
	}
	cmd := app.RetryAllDeadCommand{Queue: req.GetQueue()}
	if req.Limit != nil {
		cmd.Limit = int(req.GetLimit())
	}
	retried, err := s.queues.RetryAllDead(ctx, cmd)
	if err != nil {
		return nil, mapError(err)
	}
	return &relayv1.RetryDeadJobsResponse{Retried: int32(retried)}, nil //nolint:gosec // G115: retried bounded by limit
}
