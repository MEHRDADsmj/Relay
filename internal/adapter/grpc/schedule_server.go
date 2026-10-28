package grpc

import (
	"context"
	"strings"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	relayv1 "github.com/MEHRDADsmj/relay/gen/relay/v1"
	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

type scheduleServer struct {
	relayv1.UnimplementedScheduleServiceServer
	schedules *app.ScheduleService
}

func (s *scheduleServer) CreateSchedule(ctx context.Context, req *relayv1.CreateScheduleRequest) (*relayv1.CreateScheduleResponse, error) {
	sched, err := s.schedules.Create(ctx, createScheduleCommand(req))
	if err != nil {
		return nil, mapError(err)
	}
	return &relayv1.CreateScheduleResponse{Schedule: scheduleToProto(sched)}, nil
}

func (s *scheduleServer) ListSchedules(ctx context.Context, req *relayv1.ListSchedulesRequest) (*relayv1.ListSchedulesResponse, error) {
	pageReq := domain.PageRequest{Cursor: req.GetCursor()}
	if req.Limit != nil {
		pageReq.Limit = int(req.GetLimit())
	}
	page, err := s.schedules.List(ctx, pageReq)
	if err != nil {
		return nil, mapError(err)
	}
	items := page.Items
	if items == nil {
		items = []domain.Schedule{}
	}
	return &relayv1.ListSchedulesResponse{
		Schedules:  schedulesToProto(items),
		NextCursor: page.NextCursor,
	}, nil
}

func (s *scheduleServer) GetSchedule(ctx context.Context, req *relayv1.GetScheduleRequest) (*relayv1.GetScheduleResponse, error) {
	key := strings.TrimSpace(req.GetIdOrName())
	var sched domain.Schedule
	var err error
	if id, parseErr := uuid.Parse(key); parseErr == nil {
		sched, err = s.schedules.Get(ctx, id)
	} else {
		sched, err = s.schedules.GetByName(ctx, key)
	}
	if err != nil {
		return nil, mapError(err)
	}
	return &relayv1.GetScheduleResponse{Schedule: scheduleToProto(sched)}, nil
}

func (s *scheduleServer) UpdateSchedule(ctx context.Context, req *relayv1.UpdateScheduleRequest) (*relayv1.UpdateScheduleResponse, error) {
	if req.GetExpectedUpdatedAt() == nil {
		return nil, status.Error(codes.InvalidArgument, "expected_updated_at is required")
	}
	id, err := parseScheduleID(req.GetId())
	if err != nil {
		return nil, mapError(err)
	}
	expected := req.GetExpectedUpdatedAt().AsTime()
	sched, err := s.schedules.Update(ctx, updateScheduleCommand(req, id, expected))
	if err != nil {
		return nil, mapError(err)
	}
	return &relayv1.UpdateScheduleResponse{Schedule: scheduleToProto(sched)}, nil
}

func (s *scheduleServer) DeleteSchedule(ctx context.Context, req *relayv1.DeleteScheduleRequest) (*relayv1.DeleteScheduleResponse, error) {
	id, err := parseScheduleID(req.GetId())
	if err != nil {
		return nil, mapError(err)
	}
	if err := s.schedules.Delete(ctx, id); err != nil {
		return nil, mapError(err)
	}
	return &relayv1.DeleteScheduleResponse{}, nil
}

func (s *scheduleServer) EnableSchedule(ctx context.Context, req *relayv1.EnableScheduleRequest) (*relayv1.EnableScheduleResponse, error) {
	id, err := parseScheduleID(req.GetId())
	if err != nil {
		return nil, mapError(err)
	}
	sched, err := s.schedules.Enable(ctx, id)
	if err != nil {
		return nil, mapError(err)
	}
	return &relayv1.EnableScheduleResponse{Schedule: scheduleToProto(sched)}, nil
}

func (s *scheduleServer) DisableSchedule(ctx context.Context, req *relayv1.DisableScheduleRequest) (*relayv1.DisableScheduleResponse, error) {
	id, err := parseScheduleID(req.GetId())
	if err != nil {
		return nil, mapError(err)
	}
	sched, err := s.schedules.Disable(ctx, id)
	if err != nil {
		return nil, mapError(err)
	}
	return &relayv1.DisableScheduleResponse{Schedule: scheduleToProto(sched)}, nil
}

func (s *scheduleServer) TriggerSchedule(ctx context.Context, req *relayv1.TriggerScheduleRequest) (*relayv1.TriggerScheduleResponse, error) {
	id, err := parseScheduleID(req.GetId())
	if err != nil {
		return nil, mapError(err)
	}
	job, err := s.schedules.TriggerNow(ctx, id)
	if err != nil {
		return nil, mapError(err)
	}
	return &relayv1.TriggerScheduleResponse{Job: jobToProto(job)}, nil
}
