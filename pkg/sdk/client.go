package sdk

import (
	"context"
	"fmt"
	"net/http"
)

// Client talks to a Relay server over HTTP or gRPC.
type Client struct {
	tr   transport
	opts clientOptions
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithAPIKey sets the Bearer token used for Authorization.
func WithAPIKey(key string) ClientOption {
	return func(c *Client) {
		c.opts.apiKey = key
	}
}

// WithHTTPClient replaces the default HTTP client (HTTP transport only).
func WithHTTPClient(hc *http.Client) ClientOption {
	return func(c *Client) {
		c.opts.httpClient = hc
	}
}

// NewClient builds a Client for target.
// Schemes: http:// / https:// → HTTP; grpc:// / grpcs:// → gRPC.
// WithTransport overrides scheme detection.
func NewClient(target string, opts ...ClientOption) *Client {
	c := &Client{}
	for _, opt := range opts {
		opt(c)
	}
	tr, err := buildTransport(target, c.opts)
	if err != nil {
		c.tr = &errTransport{err: err}
		return c
	}
	c.tr = tr
	return c
}

// Close releases transport resources (gRPC connections). Safe for HTTP.
func (c *Client) Close() error {
	if c == nil || c.tr == nil {
		return nil
	}
	return c.tr.close()
}

// Enqueue creates a job (or returns an existing one on idempotent replay).
func (c *Client) Enqueue(ctx context.Context, r EnqueueRequest) (JobInfo, error) {
	return c.tr.enqueue(ctx, r)
}

// GetJob fetches a job by ID.
func (c *Client) GetJob(ctx context.Context, id string) (JobInfo, error) {
	return c.tr.getJob(ctx, id)
}

// CancelJob cancels a pending job or requests cancel on a running job.
func (c *Client) CancelJob(ctx context.Context, id string) (JobInfo, error) {
	return c.tr.cancelJob(ctx, id)
}

// RetryJob requeues a dead or cancelled job as pending.
func (c *Client) RetryJob(ctx context.Context, id string) (JobInfo, error) {
	return c.tr.retryJob(ctx, id)
}

// ListAttempts returns attempt history for a job.
func (c *Client) ListAttempts(ctx context.Context, id string) ([]AttemptInfo, error) {
	return c.tr.listAttempts(ctx, id)
}

// ListQueues returns all registered queues.
func (c *Client) ListQueues(ctx context.Context) ([]QueueInfo, error) {
	return c.tr.listQueues(ctx)
}

// QueueStats returns counters for a queue.
func (c *Client) QueueStats(ctx context.Context, queue string) (QueueStatsInfo, error) {
	return c.tr.queueStats(ctx, queue)
}

// PauseQueue pauses claims on a queue.
func (c *Client) PauseQueue(ctx context.Context, queue string) (QueueInfo, error) {
	return c.tr.pauseQueue(ctx, queue)
}

// ResumeQueue resumes a paused queue.
func (c *Client) ResumeQueue(ctx context.Context, queue string) (QueueInfo, error) {
	return c.tr.resumeQueue(ctx, queue)
}

// RetryDead bulk-retries dead jobs in a queue. limit 0 uses the server default.
func (c *Client) RetryDead(ctx context.Context, queue string, limit int) (int, error) {
	return c.tr.retryDead(ctx, queue, limit)
}

// ListJobs returns a range-over-func iterator that walks pages of jobs by status.
func (c *Client) ListJobs(ctx context.Context, queue, status string, opts ListJobsOptions) func(func(JobInfo, error) bool) {
	return func(yield func(JobInfo, error) bool) {
		cursor := ""
		for {
			jobs, next, err := c.tr.listJobsPage(ctx, queue, status, cursor, opts.Limit)
			if err != nil {
				yield(JobInfo{}, err)
				return
			}
			for _, j := range jobs {
				if !yield(j, nil) {
					return
				}
			}
			if next == "" {
				return
			}
			cursor = next
		}
	}
}

// CreateSchedule creates a cron schedule.
func (c *Client) CreateSchedule(ctx context.Context, r CreateScheduleRequest) (ScheduleInfo, error) {
	return c.tr.createSchedule(ctx, r)
}

// GetSchedule fetches a schedule by UUID or unique name.
func (c *Client) GetSchedule(ctx context.Context, idOrName string) (ScheduleInfo, error) {
	return c.tr.getSchedule(ctx, idOrName)
}

// ListSchedules returns a page of schedules.
func (c *Client) ListSchedules(ctx context.Context, opts ListSchedulesOptions) ([]ScheduleInfo, string, error) {
	return c.tr.listSchedules(ctx, opts)
}

// UpdateSchedule applies a partial update. ExpectedUpdatedAt is required (If-Match / expected_updated_at).
func (c *Client) UpdateSchedule(ctx context.Context, id string, r UpdateScheduleRequest) (ScheduleInfo, error) {
	return c.tr.updateSchedule(ctx, id, r)
}

// DeleteSchedule removes a schedule.
func (c *Client) DeleteSchedule(ctx context.Context, id string) error {
	return c.tr.deleteSchedule(ctx, id)
}

// EnableSchedule enables a schedule.
func (c *Client) EnableSchedule(ctx context.Context, id string) (ScheduleInfo, error) {
	return c.tr.enableSchedule(ctx, id)
}

// DisableSchedule disables a schedule.
func (c *Client) DisableSchedule(ctx context.Context, id string) (ScheduleInfo, error) {
	return c.tr.disableSchedule(ctx, id)
}

// TriggerSchedule enqueues a job for the schedule immediately.
func (c *Client) TriggerSchedule(ctx context.Context, id string) (JobInfo, error) {
	return c.tr.triggerSchedule(ctx, id)
}

// CreateAPIKey creates an API key; Secret is returned only once.
func (c *Client) CreateAPIKey(ctx context.Context, r CreateAPIKeyRequest) (CreatedAPIKey, error) {
	return c.tr.createAPIKey(ctx, r)
}

// ListAPIKeys lists keys without secrets.
func (c *Client) ListAPIKeys(ctx context.Context) ([]APIKeyInfo, error) {
	return c.tr.listAPIKeys(ctx)
}

// RevokeAPIKey revokes a key by ID.
func (c *Client) RevokeAPIKey(ctx context.Context, id string) (APIKeyInfo, error) {
	return c.tr.revokeAPIKey(ctx, id)
}

func (c *Client) lease(ctx context.Context, queue, workerID string, maxJobs, leaseSeconds, waitSeconds int) ([]apiJob, error) {
	return c.tr.lease(ctx, queue, workerID, maxJobs, leaseSeconds, waitSeconds)
}

func (c *Client) heartbeat(ctx context.Context, jobID, workerID string, leaseSeconds int) (apiJob, error) {
	return c.tr.heartbeat(ctx, jobID, workerID, leaseSeconds)
}

func (c *Client) complete(ctx context.Context, jobID, workerID string) error {
	return c.tr.complete(ctx, jobID, workerID)
}

func (c *Client) fail(ctx context.Context, jobID, workerID, errMsg string, permanent bool) error {
	return c.tr.fail(ctx, jobID, workerID, errMsg, permanent)
}

func (c *Client) release(ctx context.Context, jobID, workerID string) error {
	return c.tr.release(ctx, jobID, workerID)
}

// errTransport returns a fixed error from every method (invalid NewClient target).
type errTransport struct{ err error }

func (e *errTransport) close() error { return nil }

func (e *errTransport) failErr() error {
	if e == nil || e.err == nil {
		return fmt.Errorf("relay: transport not configured")
	}
	return e.err
}

func (e *errTransport) enqueue(context.Context, EnqueueRequest) (JobInfo, error) {
	return JobInfo{}, e.failErr()
}

func (e *errTransport) getJob(context.Context, string) (JobInfo, error) {
	return JobInfo{}, e.failErr()
}

func (e *errTransport) cancelJob(context.Context, string) (JobInfo, error) {
	return JobInfo{}, e.failErr()
}

func (e *errTransport) retryJob(context.Context, string) (JobInfo, error) {
	return JobInfo{}, e.failErr()
}

func (e *errTransport) listAttempts(context.Context, string) ([]AttemptInfo, error) {
	return nil, e.failErr()
}

func (e *errTransport) listQueues(context.Context) ([]QueueInfo, error) {
	return nil, e.failErr()
}

func (e *errTransport) queueStats(context.Context, string) (QueueStatsInfo, error) {
	return QueueStatsInfo{}, e.failErr()
}

func (e *errTransport) pauseQueue(context.Context, string) (QueueInfo, error) {
	return QueueInfo{}, e.failErr()
}

func (e *errTransport) resumeQueue(context.Context, string) (QueueInfo, error) {
	return QueueInfo{}, e.failErr()
}

func (e *errTransport) retryDead(context.Context, string, int) (int, error) {
	return 0, e.failErr()
}

func (e *errTransport) listJobsPage(context.Context, string, string, string, int) ([]JobInfo, string, error) {
	return nil, "", e.failErr()
}

func (e *errTransport) lease(context.Context, string, string, int, int, int) ([]apiJob, error) {
	return nil, e.failErr()
}

func (e *errTransport) heartbeat(context.Context, string, string, int) (apiJob, error) {
	return apiJob{}, e.failErr()
}

func (e *errTransport) complete(context.Context, string, string) error { return e.failErr() }

func (e *errTransport) fail(context.Context, string, string, string, bool) error {
	return e.failErr()
}

func (e *errTransport) release(context.Context, string, string) error { return e.failErr() }

func (e *errTransport) createSchedule(context.Context, CreateScheduleRequest) (ScheduleInfo, error) {
	return ScheduleInfo{}, e.failErr()
}

func (e *errTransport) getSchedule(context.Context, string) (ScheduleInfo, error) {
	return ScheduleInfo{}, e.failErr()
}

func (e *errTransport) listSchedules(context.Context, ListSchedulesOptions) ([]ScheduleInfo, string, error) {
	return nil, "", e.failErr()
}

func (e *errTransport) updateSchedule(context.Context, string, UpdateScheduleRequest) (ScheduleInfo, error) {
	return ScheduleInfo{}, e.failErr()
}

func (e *errTransport) deleteSchedule(context.Context, string) error { return e.failErr() }

func (e *errTransport) enableSchedule(context.Context, string) (ScheduleInfo, error) {
	return ScheduleInfo{}, e.failErr()
}

func (e *errTransport) disableSchedule(context.Context, string) (ScheduleInfo, error) {
	return ScheduleInfo{}, e.failErr()
}

func (e *errTransport) triggerSchedule(context.Context, string) (JobInfo, error) {
	return JobInfo{}, e.failErr()
}

func (e *errTransport) createAPIKey(context.Context, CreateAPIKeyRequest) (CreatedAPIKey, error) {
	return CreatedAPIKey{}, e.failErr()
}

func (e *errTransport) listAPIKeys(context.Context) ([]APIKeyInfo, error) {
	return nil, e.failErr()
}

func (e *errTransport) revokeAPIKey(context.Context, string) (APIKeyInfo, error) {
	return APIKeyInfo{}, e.failErr()
}
