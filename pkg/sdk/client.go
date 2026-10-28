package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client talks to a Relay HTTP API.
type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithAPIKey sets the Bearer token used for Authorization.
func WithAPIKey(key string) ClientOption {
	return func(c *Client) {
		c.apiKey = key
	}
}

// WithHTTPClient replaces the default HTTP client.
func WithHTTPClient(hc *http.Client) ClientOption {
	return func(c *Client) {
		c.httpClient = hc
	}
}

// NewClient builds a Client for baseURL (e.g. "http://localhost:7420").
func NewClient(baseURL string, opts ...ClientOption) *Client {
	c := &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		httpClient: http.DefaultClient,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Enqueue creates a job (or returns an existing one on idempotent replay).
func (c *Client) Enqueue(ctx context.Context, r EnqueueRequest) (JobInfo, error) {
	body := map[string]any{
		"queue": r.Queue,
		"type":  r.Type,
	}
	if r.Payload != nil {
		body["payload"] = r.Payload
	}
	if r.Priority != 0 {
		body["priority"] = r.Priority
	}
	if !r.RunAt.IsZero() {
		body["run_at"] = r.RunAt.UTC()
	}
	if r.MaxAttempts != 0 {
		body["max_attempts"] = r.MaxAttempts
	}
	if r.IdempotencyKey != "" {
		body["idempotency_key"] = r.IdempotencyKey
	}
	var out apiJob
	if err := c.doJSON(ctx, http.MethodPost, "/v1/jobs", body, &out); err != nil {
		return JobInfo{}, err
	}
	return out.toJobInfo(), nil
}

// GetJob fetches a job by ID.
func (c *Client) GetJob(ctx context.Context, id string) (JobInfo, error) {
	var out apiJob
	if err := c.doJSON(ctx, http.MethodGet, "/v1/jobs/"+url.PathEscape(id), nil, &out); err != nil {
		return JobInfo{}, err
	}
	return out.toJobInfo(), nil
}

// CancelJob cancels a pending job or requests cancel on a running job.
func (c *Client) CancelJob(ctx context.Context, id string) (JobInfo, error) {
	var out apiJob
	if err := c.doJSON(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(id)+"/cancel", nil, &out); err != nil {
		return JobInfo{}, err
	}
	return out.toJobInfo(), nil
}

// RetryJob requeues a dead or cancelled job as pending.
func (c *Client) RetryJob(ctx context.Context, id string) (JobInfo, error) {
	var out apiJob
	if err := c.doJSON(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(id)+"/retry", nil, &out); err != nil {
		return JobInfo{}, err
	}
	return out.toJobInfo(), nil
}

// ListAttempts returns attempt history for a job.
func (c *Client) ListAttempts(ctx context.Context, id string) ([]AttemptInfo, error) {
	var out struct {
		Attempts []apiAttempt `json:"attempts"`
	}
	if err := c.doJSON(ctx, http.MethodGet, "/v1/jobs/"+url.PathEscape(id)+"/attempts", nil, &out); err != nil {
		return nil, err
	}
	items := make([]AttemptInfo, 0, len(out.Attempts))
	for _, a := range out.Attempts {
		items = append(items, a.toAttemptInfo())
	}
	return items, nil
}

// ListQueues returns all registered queues.
func (c *Client) ListQueues(ctx context.Context) ([]QueueInfo, error) {
	var out struct {
		Queues []apiQueue `json:"queues"`
	}
	if err := c.doJSON(ctx, http.MethodGet, "/v1/queues", nil, &out); err != nil {
		return nil, err
	}
	items := make([]QueueInfo, 0, len(out.Queues))
	for _, q := range out.Queues {
		items = append(items, q.toQueueInfo())
	}
	return items, nil
}

// QueueStats returns counters for a queue.
func (c *Client) QueueStats(ctx context.Context, queue string) (QueueStatsInfo, error) {
	var out apiQueueStats
	if err := c.doJSON(ctx, http.MethodGet, "/v1/queues/"+url.PathEscape(queue), nil, &out); err != nil {
		return QueueStatsInfo{}, err
	}
	return out.toQueueStatsInfo(), nil
}

// PauseQueue pauses claims on a queue.
func (c *Client) PauseQueue(ctx context.Context, queue string) (QueueInfo, error) {
	var out apiQueue
	if err := c.doJSON(ctx, http.MethodPost, "/v1/queues/"+url.PathEscape(queue)+"/pause", nil, &out); err != nil {
		return QueueInfo{}, err
	}
	return out.toQueueInfo(), nil
}

// ResumeQueue resumes a paused queue.
func (c *Client) ResumeQueue(ctx context.Context, queue string) (QueueInfo, error) {
	var out apiQueue
	if err := c.doJSON(ctx, http.MethodPost, "/v1/queues/"+url.PathEscape(queue)+"/resume", nil, &out); err != nil {
		return QueueInfo{}, err
	}
	return out.toQueueInfo(), nil
}

// RetryDead bulk-retries dead jobs in a queue. limit 0 uses the server default.
func (c *Client) RetryDead(ctx context.Context, queue string, limit int) (int, error) {
	body := map[string]any{}
	if limit > 0 {
		body["limit"] = limit
	}
	var out struct {
		Retried int `json:"retried"`
	}
	path := "/v1/queues/" + url.PathEscape(queue) + "/dead/retry"
	if err := c.doJSON(ctx, http.MethodPost, path, body, &out); err != nil {
		return 0, err
	}
	return out.Retried, nil
}

// ListJobs returns a range-over-func iterator that walks pages of jobs by status.
func (c *Client) ListJobs(ctx context.Context, queue, status string, opts ListJobsOptions) func(func(JobInfo, error) bool) {
	return func(yield func(JobInfo, error) bool) {
		cursor := ""
		for {
			jobs, next, err := c.listJobsPage(ctx, queue, status, cursor, opts.Limit)
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

func (c *Client) listJobsPage(ctx context.Context, queue, status, cursor string, limit int) ([]JobInfo, string, error) {
	q := url.Values{}
	q.Set("status", status)
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	path := "/v1/queues/" + url.PathEscape(queue) + "/jobs?" + q.Encode()
	var out struct {
		Jobs       []apiJob `json:"jobs"`
		NextCursor string   `json:"next_cursor"`
	}
	if err := c.doJSON(ctx, http.MethodGet, path, nil, &out); err != nil {
		return nil, "", err
	}
	items := make([]JobInfo, 0, len(out.Jobs))
	for _, j := range out.Jobs {
		items = append(items, j.toJobInfo())
	}
	return items, out.NextCursor, nil
}

func (c *Client) lease(ctx context.Context, queue, workerID string, maxJobs int, leaseSeconds, waitSeconds int) ([]apiJob, error) {
	body := map[string]any{
		"worker_id": workerID,
		"max_jobs":  maxJobs,
	}
	if leaseSeconds > 0 {
		body["lease_seconds"] = leaseSeconds
	}
	if waitSeconds > 0 {
		body["wait_seconds"] = waitSeconds
	}
	path := "/v1/queues/" + url.PathEscape(queue) + "/leases"
	var out struct {
		Jobs []apiJob `json:"jobs"`
	}
	if err := c.doJSON(ctx, http.MethodPost, path, body, &out); err != nil {
		return nil, err
	}
	return out.Jobs, nil
}

func (c *Client) heartbeat(ctx context.Context, jobID, workerID string, leaseSeconds int) (apiJob, error) {
	body := map[string]any{"worker_id": workerID}
	if leaseSeconds > 0 {
		body["lease_seconds"] = leaseSeconds
	}
	var out apiJob
	if err := c.doJSON(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(jobID)+"/heartbeat", body, &out); err != nil {
		return apiJob{}, err
	}
	return out, nil
}

func (c *Client) complete(ctx context.Context, jobID, workerID string) error {
	body := map[string]any{"worker_id": workerID}
	return c.doJSON(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(jobID)+"/complete", body, nil)
}

func (c *Client) fail(ctx context.Context, jobID, workerID, errMsg string, permanent bool) error {
	body := map[string]any{
		"worker_id": workerID,
		"error":     errMsg,
	}
	if permanent {
		body["permanent"] = true
	}
	return c.doJSON(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(jobID)+"/fail", body, nil)
}

func (c *Client) release(ctx context.Context, jobID, workerID string) error {
	body := map[string]any{"worker_id": workerID}
	return c.doJSON(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(jobID)+"/release", body, nil)
}

func (c *Client) doJSON(ctx context.Context, method, path string, body any, out any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("relay: marshal request: %w", err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, rdr)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	res, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return err
	}
	if res.StatusCode >= 400 {
		return parseAPIError(res.StatusCode, data)
	}
	if out == nil || res.StatusCode == http.StatusNoContent || len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("relay: decode response: %w", err)
	}
	return nil
}

func parseAPIError(status int, data []byte) error {
	var p struct {
		Type   string `json:"type"`
		Title  string `json:"title"`
		Detail string `json:"detail"`
		Status int    `json:"status"`
	}
	_ = json.Unmarshal(data, &p)
	ae := &APIError{
		StatusCode: status,
		Type:       p.Type,
		Title:      p.Title,
		Detail:     p.Detail,
	}
	if ae.Title == "" {
		ae.Title = http.StatusText(status)
	}
	return ae
}

// apiJob is the wire shape of a Job from the HTTP API.
type apiJob struct {
	ID              string          `json:"id"`
	Queue           string          `json:"queue"`
	Type            string          `json:"type"`
	Payload         json.RawMessage `json:"payload"`
	Status          string          `json:"status"`
	Priority        int             `json:"priority"`
	RunAt           time.Time       `json:"run_at"`
	Attempts        int             `json:"attempts"`
	MaxAttempts     int             `json:"max_attempts"`
	LeaseOwner      *string         `json:"lease_owner"`
	LeaseExpiresAt  *time.Time      `json:"lease_expires_at"`
	IdempotencyKey  *string         `json:"idempotency_key"`
	LastError       *string         `json:"last_error"`
	CancelRequested bool            `json:"cancel_requested"`
	CancelledAt     *time.Time      `json:"cancelled_at"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
	FinishedAt      *time.Time      `json:"finished_at"`
}

func (j apiJob) toJobInfo() JobInfo {
	info := JobInfo{
		ID:              j.ID,
		Queue:           j.Queue,
		Type:            j.Type,
		Payload:         append([]byte(nil), j.Payload...),
		Status:          j.Status,
		Priority:        j.Priority,
		RunAt:           j.RunAt,
		Attempts:        j.Attempts,
		MaxAttempts:     j.MaxAttempts,
		LeaseExpiresAt:  j.LeaseExpiresAt,
		CancelRequested: j.CancelRequested,
		CancelledAt:     j.CancelledAt,
		CreatedAt:       j.CreatedAt,
		UpdatedAt:       j.UpdatedAt,
		FinishedAt:      j.FinishedAt,
	}
	if j.LeaseOwner != nil {
		info.LeaseOwner = *j.LeaseOwner
	}
	if j.IdempotencyKey != nil {
		info.IdempotencyKey = *j.IdempotencyKey
	}
	if j.LastError != nil {
		info.LastError = *j.LastError
	}
	return info
}

type apiAttempt struct {
	JobID      string     `json:"job_id"`
	Number     int        `json:"number"`
	WorkerID   string     `json:"worker_id"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at"`
	Outcome    string     `json:"outcome"`
	Error      string     `json:"error"`
}

func (a apiAttempt) toAttemptInfo() AttemptInfo {
	return AttemptInfo(a)
}

type apiQueue struct {
	Name      string    `json:"name"`
	Paused    bool      `json:"paused"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (q apiQueue) toQueueInfo() QueueInfo {
	return QueueInfo(q)
}

type apiQueueStats struct {
	Queue                   string `json:"queue"`
	Paused                  bool   `json:"paused"`
	Pending                 int64  `json:"pending"`
	Scheduled               int64  `json:"scheduled"`
	Running                 int64  `json:"running"`
	Succeeded               int64  `json:"succeeded"`
	Dead                    int64  `json:"dead"`
	Cancelled               int64  `json:"cancelled"`
	OldestPendingAgeSeconds int64  `json:"oldest_pending_age_seconds"`
}

func (s apiQueueStats) toQueueStatsInfo() QueueStatsInfo {
	return QueueStatsInfo{
		Queue:            s.Queue,
		Paused:           s.Paused,
		Pending:          s.Pending,
		Scheduled:        s.Scheduled,
		Running:          s.Running,
		Succeeded:        s.Succeeded,
		Dead:             s.Dead,
		Cancelled:        s.Cancelled,
		OldestPendingAge: time.Duration(s.OldestPendingAgeSeconds) * time.Second,
	}
}
