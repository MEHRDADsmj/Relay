package sdk

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	relayv1 "github.com/MEHRDADsmj/relay/gen/relay/v1"
)

type grpcTransport struct {
	conn    *grpc.ClientConn
	apiKey  string
	jobs    relayv1.JobServiceClient
	queues  relayv1.QueueServiceClient
	scheds  relayv1.ScheduleServiceClient
	apiKeys relayv1.APIKeyServiceClient
}

func newGRPCTransport(target, apiKey string, dialOpts ...grpc.DialOption) (*grpcTransport, error) {
	addr, useTLS, err := parseGRPCTarget(target)
	if err != nil {
		return nil, err
	}
	creds := grpc.WithTransportCredentials(insecure.NewCredentials())
	if useTLS {
		creds = grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12}))
	}
	// Default credentials first; callers may add a dialer (bufconn). Do not pass a second
	// TransportCredentials or NewClient will fail.
	opts := append([]grpc.DialOption{creds}, dialOpts...)

	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("relay: grpc dial: %w", err)
	}
	return &grpcTransport{
		conn:    conn,
		apiKey:  apiKey,
		jobs:    relayv1.NewJobServiceClient(conn),
		queues:  relayv1.NewQueueServiceClient(conn),
		scheds:  relayv1.NewScheduleServiceClient(conn),
		apiKeys: relayv1.NewAPIKeyServiceClient(conn),
	}, nil
}

func parseGRPCTarget(target string) (addr string, useTLS bool, err error) {
	// Custom dialers (bufconn) use the passthrough resolver so gRPC does not DNS-resolve.
	if strings.HasPrefix(target, "passthrough:///") || strings.HasPrefix(target, "dns:///") {
		return target, false, nil
	}
	u, err := url.Parse(target)
	if err != nil {
		return "", false, fmt.Errorf("relay: parse grpc target: %w", err)
	}
	switch strings.ToLower(u.Scheme) {
	case "grpc", "":
		useTLS = false
	case "grpcs":
		useTLS = true
	case "http":
		// WithTransport(GRPC) override of an http URL.
		useTLS = false
	case "https":
		useTLS = true
	default:
		return "", false, fmt.Errorf("relay: unsupported grpc target scheme %q", u.Scheme)
	}
	addr = u.Host
	if addr == "" {
		addr = strings.TrimPrefix(target, u.Scheme+"://")
	}
	if addr == "" {
		return "", false, fmt.Errorf("relay: grpc target missing host")
	}
	return addr, useTLS, nil
}

func (t *grpcTransport) close() error {
	if t == nil || t.conn == nil {
		return nil
	}
	return t.conn.Close()
}

func (t *grpcTransport) ctx(ctx context.Context) context.Context {
	if t.apiKey == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+t.apiKey)
}

func (t *grpcTransport) enqueue(ctx context.Context, r EnqueueRequest) (JobInfo, error) {
	payload, err := marshalPayload(r.Payload)
	if err != nil {
		return JobInfo{}, fmt.Errorf("relay: marshal payload: %w", err)
	}
	req := &relayv1.EnqueueRequest{
		Queue:          r.Queue,
		Type:           r.Type,
		Payload:        payload,
		IdempotencyKey: r.IdempotencyKey,
	}
	if r.Priority != 0 {
		req.Priority = ptrInt32(r.Priority)
	}
	if !r.RunAt.IsZero() {
		req.RunAt = timestamppb.New(r.RunAt.UTC())
	}
	if r.MaxAttempts != 0 {
		req.MaxAttempts = ptrInt32(r.MaxAttempts)
	}
	res, err := t.jobs.Enqueue(t.ctx(ctx), req)
	if err != nil {
		return JobInfo{}, mapGRPCError(err)
	}
	return jobInfoFromProto(res.GetJob()), nil
}

func (t *grpcTransport) getJob(ctx context.Context, id string) (JobInfo, error) {
	res, err := t.jobs.GetJob(t.ctx(ctx), &relayv1.GetJobRequest{Id: id})
	if err != nil {
		return JobInfo{}, mapGRPCError(err)
	}
	return jobInfoFromProto(res.GetJob()), nil
}

func (t *grpcTransport) cancelJob(ctx context.Context, id string) (JobInfo, error) {
	res, err := t.jobs.Cancel(t.ctx(ctx), &relayv1.CancelRequest{Id: id})
	if err != nil {
		return JobInfo{}, mapGRPCError(err)
	}
	return jobInfoFromProto(res.GetJob()), nil
}

func (t *grpcTransport) retryJob(ctx context.Context, id string) (JobInfo, error) {
	res, err := t.jobs.Retry(t.ctx(ctx), &relayv1.RetryRequest{Id: id})
	if err != nil {
		return JobInfo{}, mapGRPCError(err)
	}
	return jobInfoFromProto(res.GetJob()), nil
}

func (t *grpcTransport) listAttempts(ctx context.Context, id string) ([]AttemptInfo, error) {
	res, err := t.jobs.ListAttempts(t.ctx(ctx), &relayv1.ListAttemptsRequest{Id: id})
	if err != nil {
		return nil, mapGRPCError(err)
	}
	items := make([]AttemptInfo, 0, len(res.GetAttempts()))
	for _, a := range res.GetAttempts() {
		items = append(items, attemptFromProto(a))
	}
	return items, nil
}

func (t *grpcTransport) listQueues(ctx context.Context) ([]QueueInfo, error) {
	res, err := t.queues.ListQueues(t.ctx(ctx), &relayv1.ListQueuesRequest{})
	if err != nil {
		return nil, mapGRPCError(err)
	}
	items := make([]QueueInfo, 0, len(res.GetQueues()))
	for _, q := range res.GetQueues() {
		items = append(items, queueFromProto(q))
	}
	return items, nil
}

func (t *grpcTransport) queueStats(ctx context.Context, queue string) (QueueStatsInfo, error) {
	res, err := t.queues.GetQueueStats(t.ctx(ctx), &relayv1.GetQueueStatsRequest{Queue: queue})
	if err != nil {
		return QueueStatsInfo{}, mapGRPCError(err)
	}
	return queueStatsFromProto(res.GetStats()), nil
}

func (t *grpcTransport) pauseQueue(ctx context.Context, queue string) (QueueInfo, error) {
	res, err := t.queues.PauseQueue(t.ctx(ctx), &relayv1.PauseQueueRequest{Queue: queue})
	if err != nil {
		return QueueInfo{}, mapGRPCError(err)
	}
	return queueFromProto(res.GetQueue()), nil
}

func (t *grpcTransport) resumeQueue(ctx context.Context, queue string) (QueueInfo, error) {
	res, err := t.queues.ResumeQueue(t.ctx(ctx), &relayv1.ResumeQueueRequest{Queue: queue})
	if err != nil {
		return QueueInfo{}, mapGRPCError(err)
	}
	return queueFromProto(res.GetQueue()), nil
}

func (t *grpcTransport) retryDead(ctx context.Context, queue string, limit int) (int, error) {
	req := &relayv1.RetryDeadJobsRequest{Queue: queue}
	if limit > 0 {
		req.Limit = ptrInt32(limit)
	}
	res, err := t.queues.RetryDeadJobs(t.ctx(ctx), req)
	if err != nil {
		return 0, mapGRPCError(err)
	}
	return int(res.GetRetried()), nil
}

func (t *grpcTransport) listJobsPage(ctx context.Context, queue, status, cursor string, limit int) ([]JobInfo, string, error) {
	st, err := jobStatusFromString(status)
	if err != nil {
		return nil, "", err
	}
	req := &relayv1.ListQueueJobsRequest{
		Queue:  queue,
		Status: st,
		Cursor: cursor,
	}
	if limit > 0 {
		req.Limit = ptrInt32(limit)
	}
	res, err := t.queues.ListQueueJobs(t.ctx(ctx), req)
	if err != nil {
		return nil, "", mapGRPCError(err)
	}
	items := make([]JobInfo, 0, len(res.GetJobs()))
	for _, j := range res.GetJobs() {
		items = append(items, jobInfoFromProto(j))
	}
	return items, res.GetNextCursor(), nil
}

func (t *grpcTransport) lease(ctx context.Context, queue, workerID string, maxJobs, leaseSeconds, waitSeconds int) ([]apiJob, error) {
	req := &relayv1.LeaseRequest{
		Queue:    queue,
		WorkerId: workerID,
	}
	if maxJobs > 0 {
		req.MaxJobs = ptrInt32(maxJobs)
	}
	if leaseSeconds > 0 {
		req.LeaseSeconds = ptrInt32(leaseSeconds)
	}
	if waitSeconds > 0 {
		req.WaitSeconds = ptrInt32(waitSeconds)
	}
	res, err := t.jobs.Lease(t.ctx(ctx), req)
	if err != nil {
		return nil, mapGRPCError(err)
	}
	jobs := make([]apiJob, 0, len(res.GetJobs()))
	for _, j := range res.GetJobs() {
		jobs = append(jobs, jobFromProto(j))
	}
	return jobs, nil
}

func (t *grpcTransport) heartbeat(ctx context.Context, jobID, workerID string, leaseSeconds int) (apiJob, error) {
	req := &relayv1.HeartbeatRequest{Id: jobID, WorkerId: workerID}
	if leaseSeconds > 0 {
		req.LeaseSeconds = ptrInt32(leaseSeconds)
	}
	res, err := t.jobs.Heartbeat(t.ctx(ctx), req)
	if err != nil {
		return apiJob{}, mapGRPCError(err)
	}
	return jobFromProto(res.GetJob()), nil
}

func (t *grpcTransport) complete(ctx context.Context, jobID, workerID string) error {
	_, err := t.jobs.Complete(t.ctx(ctx), &relayv1.CompleteRequest{Id: jobID, WorkerId: workerID})
	return mapGRPCError(err)
}

func (t *grpcTransport) fail(ctx context.Context, jobID, workerID, errMsg string, permanent bool) error {
	_, err := t.jobs.Fail(t.ctx(ctx), &relayv1.FailRequest{
		Id: jobID, WorkerId: workerID, Error: errMsg, Permanent: permanent,
	})
	return mapGRPCError(err)
}

func (t *grpcTransport) release(ctx context.Context, jobID, workerID string) error {
	_, err := t.jobs.Release(t.ctx(ctx), &relayv1.ReleaseRequest{Id: jobID, WorkerId: workerID})
	return mapGRPCError(err)
}

func (t *grpcTransport) createSchedule(ctx context.Context, r CreateScheduleRequest) (ScheduleInfo, error) {
	payload, err := marshalPayload(r.Payload)
	if err != nil {
		return ScheduleInfo{}, fmt.Errorf("relay: marshal payload: %w", err)
	}
	req := &relayv1.CreateScheduleRequest{
		Name:     r.Name,
		Cron:     r.Cron,
		Timezone: r.Timezone,
		Queue:    r.Queue,
		Type:     r.Type,
		Payload:  payload,
	}
	if r.Priority != 0 {
		req.Priority = ptrInt32(r.Priority)
	}
	if r.MaxAttempts != 0 {
		req.MaxAttempts = ptrInt32(r.MaxAttempts)
	}
	if r.Enabled != nil {
		req.Enabled = r.Enabled
	}
	res, err := t.scheds.CreateSchedule(t.ctx(ctx), req)
	if err != nil {
		return ScheduleInfo{}, mapGRPCError(err)
	}
	return scheduleFromProto(res.GetSchedule()), nil
}

func (t *grpcTransport) getSchedule(ctx context.Context, idOrName string) (ScheduleInfo, error) {
	res, err := t.scheds.GetSchedule(t.ctx(ctx), &relayv1.GetScheduleRequest{IdOrName: idOrName})
	if err != nil {
		return ScheduleInfo{}, mapGRPCError(err)
	}
	return scheduleFromProto(res.GetSchedule()), nil
}

func (t *grpcTransport) listSchedules(ctx context.Context, opts ListSchedulesOptions) ([]ScheduleInfo, string, error) {
	req := &relayv1.ListSchedulesRequest{Cursor: opts.Cursor}
	if opts.Limit > 0 {
		req.Limit = ptrInt32(opts.Limit)
	}
	res, err := t.scheds.ListSchedules(t.ctx(ctx), req)
	if err != nil {
		return nil, "", mapGRPCError(err)
	}
	items := make([]ScheduleInfo, 0, len(res.GetSchedules()))
	for _, s := range res.GetSchedules() {
		items = append(items, scheduleFromProto(s))
	}
	return items, res.GetNextCursor(), nil
}

func (t *grpcTransport) updateSchedule(ctx context.Context, id string, r UpdateScheduleRequest) (ScheduleInfo, error) {
	req := &relayv1.UpdateScheduleRequest{
		Id:                id,
		ExpectedUpdatedAt: ptrTimestamp(r.ExpectedUpdatedAt),
		SetPayload:        r.SetPayload,
		Payload:           append([]byte(nil), r.Payload...),
	}
	if r.Cron != nil {
		req.Cron = r.Cron
	}
	if r.Timezone != nil {
		req.Timezone = r.Timezone
	}
	if r.Queue != nil {
		req.Queue = r.Queue
	}
	if r.Type != nil {
		req.Type = r.Type
	}
	if r.Priority != nil {
		req.Priority = ptrInt32(*r.Priority)
	}
	if r.MaxAttempts != nil {
		req.MaxAttempts = ptrInt32(*r.MaxAttempts)
	}
	if r.Enabled != nil {
		req.Enabled = r.Enabled
	}
	res, err := t.scheds.UpdateSchedule(t.ctx(ctx), req)
	if err != nil {
		return ScheduleInfo{}, mapGRPCError(err)
	}
	return scheduleFromProto(res.GetSchedule()), nil
}

func (t *grpcTransport) deleteSchedule(ctx context.Context, id string) error {
	_, err := t.scheds.DeleteSchedule(t.ctx(ctx), &relayv1.DeleteScheduleRequest{Id: id})
	return mapGRPCError(err)
}

func (t *grpcTransport) enableSchedule(ctx context.Context, id string) (ScheduleInfo, error) {
	res, err := t.scheds.EnableSchedule(t.ctx(ctx), &relayv1.EnableScheduleRequest{Id: id})
	if err != nil {
		return ScheduleInfo{}, mapGRPCError(err)
	}
	return scheduleFromProto(res.GetSchedule()), nil
}

func (t *grpcTransport) disableSchedule(ctx context.Context, id string) (ScheduleInfo, error) {
	res, err := t.scheds.DisableSchedule(t.ctx(ctx), &relayv1.DisableScheduleRequest{Id: id})
	if err != nil {
		return ScheduleInfo{}, mapGRPCError(err)
	}
	return scheduleFromProto(res.GetSchedule()), nil
}

func (t *grpcTransport) triggerSchedule(ctx context.Context, id string) (JobInfo, error) {
	res, err := t.scheds.TriggerSchedule(t.ctx(ctx), &relayv1.TriggerScheduleRequest{Id: id})
	if err != nil {
		return JobInfo{}, mapGRPCError(err)
	}
	return jobInfoFromProto(res.GetJob()), nil
}

func (t *grpcTransport) createAPIKey(ctx context.Context, r CreateAPIKeyRequest) (CreatedAPIKey, error) {
	scopes, err := scopesToProto(r.Scopes)
	if err != nil {
		return CreatedAPIKey{}, err
	}
	req := &relayv1.CreateAPIKeyRequest{
		Name:   r.Name,
		Scopes: scopes,
	}
	if r.ExpiresAt != nil {
		req.ExpiresAt = timestamppb.New(r.ExpiresAt.UTC())
	}
	res, err := t.apiKeys.CreateAPIKey(t.ctx(ctx), req)
	if err != nil {
		return CreatedAPIKey{}, mapGRPCError(err)
	}
	return CreatedAPIKey{
		Key:    apiKeyFromProto(res.GetKey()),
		Secret: res.GetSecret(),
	}, nil
}

func (t *grpcTransport) listAPIKeys(ctx context.Context) ([]APIKeyInfo, error) {
	res, err := t.apiKeys.ListAPIKeys(t.ctx(ctx), &relayv1.ListAPIKeysRequest{})
	if err != nil {
		return nil, mapGRPCError(err)
	}
	items := make([]APIKeyInfo, 0, len(res.GetKeys()))
	for _, k := range res.GetKeys() {
		items = append(items, apiKeyFromProto(k))
	}
	return items, nil
}

func (t *grpcTransport) revokeAPIKey(ctx context.Context, id string) (APIKeyInfo, error) {
	res, err := t.apiKeys.RevokeAPIKey(t.ctx(ctx), &relayv1.RevokeAPIKeyRequest{Id: id})
	if err != nil {
		return APIKeyInfo{}, mapGRPCError(err)
	}
	return apiKeyFromProto(res.GetKey()), nil
}

func mapGRPCError(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	ae := &APIError{
		StatusCode: grpcCodeToHTTP(st.Code()),
		Title:      st.Code().String(),
		Detail:     st.Message(),
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			switch info.GetReason() {
			case "LEASE_LOST":
				ae.Type = problemTypeLeaseLost
			case "INVALID_TRANSITION":
				ae.Type = problemTypeInvalidTransition
			}
		}
	}
	// Default lease-lost typing for FailedPrecondition without details.
	if st.Code() == codes.FailedPrecondition && ae.Type == "" {
		ae.Type = problemTypeLeaseLost
	}
	return ae
}

func grpcCodeToHTTP(c codes.Code) int {
	switch c {
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.NotFound:
		return http.StatusNotFound
	case codes.Aborted:
		return http.StatusConflict
	case codes.FailedPrecondition:
		return http.StatusConflict
	case codes.AlreadyExists:
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}
